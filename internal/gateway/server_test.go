package gateway

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/coder/websocket"
	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"github.com/kylin1993/codex-remote/internal/tailnet"
	"google.golang.org/protobuf/encoding/protojson"
)

type failingAudit struct{ calls atomic.Int32 }

func (a *failingAudit) RecordWire(context.Context, bool, string, string, string, []byte, *remotev1.Frame, remotev1.AuditOutcome) (string, error) {
	a.calls.Add(1)
	return "", errors.New("audit unavailable")
}
func (a *failingAudit) Record(context.Context, *remotev1.AuditRecord) error {
	a.calls.Add(1)
	return errors.New("audit unavailable")
}

type testIdentity struct{}

func (testIdentity) WhoIs(context.Context, string) (tailnet.PeerIdentity, error) {
	return tailnet.PeerIdentity{NodeID: "n", UserID: "u"}, nil
}

type testBackend struct{ starts int }

type errorBackend struct {
	testBackend
	err error
}

func (b *errorBackend) GetHost(context.Context, *remotev1.GetHostRequest) (*remotev1.GetHostResponse, error) {
	return nil, b.err
}

func (*testBackend) GetHost(context.Context, *remotev1.GetHostRequest) (*remotev1.GetHostResponse, error) {
	return &remotev1.GetHostResponse{Host: &remotev1.HostInfo{Status: remotev1.HostStatus_HOST_STATUS_READY}}, nil
}
func (*testBackend) ListDirectories(context.Context, *remotev1.ListDirectoriesRequest) (*remotev1.ListDirectoriesResponse, error) {
	return &remotev1.ListDirectoriesResponse{}, nil
}
func (*testBackend) ListSessionCandidates(context.Context, *remotev1.ListSessionCandidatesRequest) (*remotev1.ListSessionCandidatesResponse, error) {
	return &remotev1.ListSessionCandidatesResponse{}, nil
}
func (*testBackend) ListCodexes(context.Context, *remotev1.ListCodexesRequest) (*remotev1.ListCodexesResponse, error) {
	return &remotev1.ListCodexesResponse{}, nil
}
func (*testBackend) CreateCodex(context.Context, *remotev1.CreateCodexRequest) (*remotev1.CreateCodexResponse, error) {
	return &remotev1.CreateCodexResponse{}, nil
}
func (*testBackend) ImportSession(context.Context, *remotev1.ImportSessionRequest) (*remotev1.ImportSessionResponse, error) {
	return &remotev1.ImportSessionResponse{}, nil
}
func (*testBackend) ListHistory(context.Context, *remotev1.ListHistoryRequest) (*remotev1.ListHistoryResponse, error) {
	return &remotev1.ListHistoryResponse{}, nil
}
func (b *testBackend) StartTurn(context.Context, *remotev1.StartTurnRequest) (*remotev1.StartTurnResponse, error) {
	b.starts++
	return &remotev1.StartTurnResponse{TurnId: "turn"}, nil
}
func (*testBackend) InterruptTurn(context.Context, *remotev1.InterruptTurnRequest) (*remotev1.InterruptTurnResponse, error) {
	return &remotev1.InterruptTurnResponse{}, nil
}
func (*testBackend) RespondApproval(context.Context, *remotev1.RespondApprovalRequest) (*remotev1.RespondApprovalResponse, error) {
	return &remotev1.RespondApprovalResponse{}, nil
}
func (*testBackend) RespondUserInput(context.Context, *remotev1.RespondUserInputRequest) (*remotev1.RespondUserInputResponse, error) {
	return &remotev1.RespondUserInputResponse{}, nil
}

func TestDispatcherPersistsSideEffectDedup(t *testing.T) {
	p, err := persistence.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	b := new(testBackend)
	d := &Dispatcher{Backend: b, Dedup: p}
	req := &remotev1.Request{RequestId: "same", Request: &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: "c", Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "hello"}}}}}}}
	r1, err := d.Dispatch(context.Background(), req)
	if err != nil || r1.GetStartTurn() == nil {
		t.Fatalf("first %+v %v", r1, err)
	}
	r2, err := d.Dispatch(context.Background(), req)
	if err != nil || !r2.GetStartTurn().Deduplicated || b.starts != 1 {
		t.Fatalf("second %+v starts=%d err=%v", r2, b.starts, err)
	}
}

func TestDispatcherUsesExplicitDeadlineAndRPCErrors(t *testing.T) {
	req := &remotev1.Request{RequestId: "host", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}
	d := &Dispatcher{Backend: &errorBackend{err: context.DeadlineExceeded}}
	resp, err := d.Dispatch(context.Background(), req)
	if err != nil || resp.GetError() == nil || resp.GetError().Code != remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED || !resp.GetError().Retryable {
		t.Fatalf("deadline response = %+v, %v", resp, err)
	}
	d.Backend = &errorBackend{err: &RPCError{Detail: &remotev1.Error{Code: remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, Message: "missing"}}}
	resp, err = d.Dispatch(context.Background(), req)
	if err != nil || resp.GetError() == nil || resp.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND {
		t.Fatalf("explicit RPC response = %+v, %v", resp, err)
	}
}

func TestWebSocketHelloRPCWatchAndUnwatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := persistence.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.UpsertCodex(ctx, persistence.CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Title: "x", Origin: "remote", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	events := activity.NewStore(p, nil, 8)
	backend := new(testBackend)
	srv := NewServer(ServerConfig{HostID: "host", HostRunID: "run", HeartbeatInterval: time.Hour, ConnectionTimeout: 2 * time.Hour}, &Dispatcher{Backend: backend, Dedup: p}, events, testIdentity{}, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = srv.Shutdown(shutdown)
		<-done
	}()
	ws, resp, err := websocket.Dial(ctx, "ws://"+ln.Addr().String()+"/connect", &websocket.DialOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		t.Fatalf("dial status=%v err=%v", resp, err)
	}
	defer ws.CloseNow()
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_ClientHello{ClientHello: &remotev1.ClientHello{ClientId: "client", ClientRunId: "client-run", ProtocolVersion: &remotev1.ProtocolVersion{Major: 1, Minor: 0}}}})
	if got := readFrame(t, ctx, ws).GetServerHello(); got == nil || got.ConnectionId == "" {
		t.Fatalf("hello %+v", got)
	}
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "host", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}}})
	if got := readFrame(t, ctx, ws).GetResponse(); got == nil || got.GetGetHost() == nil {
		t.Fatalf("get host %+v", got)
	}
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "watch", Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "c"}}}}})
	watch := readFrame(t, ctx, ws).GetResponse().GetWatchCodex()
	if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESET {
		t.Fatalf("watch %+v", watch)
	}
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "unwatch", Request: &remotev1.Request_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexRequest{CodexId: "c"}}}}})
	if got := readFrame(t, ctx, ws).GetResponse().GetUnwatchCodex(); got == nil || got.CodexId != "c" {
		t.Fatalf("unwatch %+v", got)
	}
	zero := uint64(0)
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "missing-run", Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "c", AfterEventSeq: &zero}}}}})
	if got := readFrame(t, ctx, ws).GetResponse().GetError(); got == nil || got.Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("missing run response %+v", got)
	}
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "old-run", Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "c", AfterEventSeq: &zero, AfterHostRunId: "old"}}}}})
	if got := readFrame(t, ctx, ws).GetResponse().GetWatchCodex(); got == nil || got.Mode != remotev1.WatchMode_WATCH_MODE_RESET || got.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED {
		t.Fatalf("old run response %+v", got)
	}
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "unknown", Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "missing"}}}}})
	if got := readFrame(t, ctx, ws).GetResponse().GetError(); got == nil || got.Code != remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND {
		t.Fatalf("unknown Codex response %+v", got)
	}
}

func TestConnectRejectsMissingSubprotocol(t *testing.T) {
	s := NewServer(ServerConfig{}, &Dispatcher{}, nil, testIdentity{}, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	resp, err := http.Get("http://" + ln.Addr().String() + "/connect")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
	<-done
}

func TestProtocolCloseBypassesFullSendQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &connection{ctx: ctx, cancel: cancel, send: make(chan outbound, 1), control: make(chan outbound, 1)}
	c.send <- outbound{frame: &remotev1.Frame{Payload: &remotev1.Frame_Ping{Ping: &remotev1.Ping{}}}}
	c.protocolClose(remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER, "slow", websocket.StatusCode(4001))
	select {
	case out := <-c.control:
		if out.frame.GetClose() == nil || out.frame.GetClose().Code != remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER || out.closeStatus != websocket.StatusCode(4001) {
			t.Fatalf("control %+v", out)
		}
	default:
		t.Fatal("control close was dropped")
	}
}

func TestWatchAndUnwatchUseEnvelopeValidationAndRPCAudit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	aud := new(recordingAudit)
	c := &connection{server: &Server{cfg: ServerConfig{MaxWatches: 2}, audit: aud}, ctx: ctx, cancel: cancel, send: make(chan outbound, 4), control: make(chan outbound, 1), watches: make(map[string]*activity.Watch)}
	expired := &remotev1.Request{RequestId: "expired", DeadlineUnixMs: time.Now().Add(-time.Second).UnixMilli(), Request: &remotev1.Request_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexRequest{CodexId: "c"}}}
	c.handleRequest(expired, "wire-expired")
	if got := (<-c.send).frame.GetResponse().GetError(); got == nil || got.Code != remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED {
		t.Fatalf("expired Unwatch = %+v", got)
	}
	missingID := &remotev1.Request{Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "c"}}}
	c.handleRequest(missingID, "wire-missing")
	if got := (<-c.send).frame.GetResponse().GetError(); got == nil || got.Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("missing request ID Watch = %+v", got)
	}
	records := aud.snapshot()
	if len(records) != 2 || records[0].Operation != "rpc.unwatch_codex" || records[0].ParentRecordId != "wire-expired" || records[0].Error == nil || records[1].Operation != "rpc.watch_codex" {
		t.Fatalf("RPC audit = %+v", records)
	}
}

func TestRewatchInvalidatesOldBufferedEventsBeforeNewResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := persistence.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.UpsertCodex(ctx, persistence.CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	events := activity.NewStore(p, nil, 128)
	c := &connection{server: &Server{cfg: ServerConfig{MaxWatches: 2, WatchQueueSize: 8}, events: events}, ctx: ctx, cancel: cancel, send: make(chan outbound, 512), control: make(chan outbound, 1), watches: make(map[string]*activity.Watch)}
	for i := 0; i < 50; i++ {
		old, watchErr := events.Watch(ctx, "c", nil, 8)
		if watchErr != nil {
			t.Fatal(watchErr)
		}
		c.watches["c"] = old
		published, publishErr := events.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{}, nil, "")
		if publishErr != nil {
			t.Fatal(publishErr)
		}
		c.watchesMu.Lock()
		oldDone := make(chan struct{})
		go func() {
			c.forwardWatch("c", old)
			close(oldDone)
		}()
		newDone := make(chan struct{})
		envelope := &remotev1.Request{RequestId: "rewatch", Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "c"}}}
		go func() {
			c.handleWatch(ctx, envelope, envelope.GetWatchCodex(), "")
			close(newDone)
		}()
		c.watchesMu.Unlock()
		<-newDone
		<-oldDone
		seenNewResponse := false
		for len(c.send) > 0 {
			out := <-c.send
			if response := out.frame.GetResponse(); response != nil && response.RequestId == "rewatch" {
				seenNewResponse = true
			}
			if event := out.frame.GetEvent(); event != nil && event.EventSeq == published.EventSeq && seenNewResponse {
				t.Fatalf("iteration %d: old buffered event %d emitted after new Watch response", i, event.EventSeq)
			}
		}
		if !seenNewResponse {
			t.Fatalf("iteration %d: missing new Watch response", i)
		}
		c.watchesMu.Lock()
		if current := c.watches["c"]; current != nil {
			current.Cancel()
			delete(c.watches, "c")
		}
		c.watchesMu.Unlock()
	}
}

func TestOversizedOutboundAndInboundUseFormalClose(t *testing.T) {
	t.Run("outbound", func(t *testing.T) {
		backend := &largeHostBackend{}
		ws, ctx := dialGateway(t, ServerConfig{MaxFrameBytes: 600, HeartbeatInterval: time.Hour, ConnectionTimeout: 2 * time.Hour}, backend, nil)
		writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "large", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}}})
		if got := readFrame(t, ctx, ws).GetClose(); got == nil || got.Code != remotev1.CloseCode_CLOSE_CODE_FRAME_TOO_LARGE {
			t.Fatalf("formal outbound close = %+v", got)
		}
	})
	t.Run("inbound", func(t *testing.T) {
		ws, ctx := dialGateway(t, ServerConfig{MaxFrameBytes: 600, HeartbeatInterval: time.Hour, ConnectionTimeout: 2 * time.Hour}, new(testBackend), nil)
		raw := []byte(`{"request":{"requestId":"oversized","padding":"` + strings.Repeat("x", 1000) + `"}}`)
		if err := ws.Write(ctx, websocket.MessageText, raw); err != nil {
			t.Fatal(err)
		}
		if got := readFrame(t, ctx, ws).GetClose(); got == nil || got.Code != remotev1.CloseCode_CLOSE_CODE_FRAME_TOO_LARGE {
			t.Fatalf("formal inbound close = %+v", got)
		}
	})
}

func TestAuditFailureIsReportedButDoesNotBlockRPC(t *testing.T) {
	aud := new(failingAudit)
	var reported atomic.Int32
	ws, ctx := dialGateway(t, ServerConfig{HeartbeatInterval: time.Hour, ConnectionTimeout: 2 * time.Hour, AuditError: func(error) { reported.Add(1) }}, new(testBackend), aud)
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "host", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}}})
	if got := readFrame(t, ctx, ws).GetResponse(); got == nil || got.GetGetHost() == nil {
		t.Fatalf("RPC response after audit failure = %+v", got)
	}
	if aud.calls.Load() == 0 || reported.Load() == 0 {
		t.Fatalf("audit failure visibility calls=%d reported=%d", aud.calls.Load(), reported.Load())
	}
}

type recordingAudit struct {
	mu      sync.Mutex
	records []*remotev1.AuditRecord
}

func (*recordingAudit) RecordWire(context.Context, bool, string, string, string, []byte, *remotev1.Frame, remotev1.AuditOutcome) (string, error) {
	return "wire", nil
}
func (a *recordingAudit) Record(_ context.Context, rec *remotev1.AuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, rec)
	return nil
}
func (a *recordingAudit) snapshot() []*remotev1.AuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*remotev1.AuditRecord(nil), a.records...)
}

type largeHostBackend struct{ testBackend }

func (*largeHostBackend) GetHost(context.Context, *remotev1.GetHostRequest) (*remotev1.GetHostResponse, error) {
	return &remotev1.GetHostResponse{Host: &remotev1.HostInfo{Name: strings.Repeat("large", 1000), Status: remotev1.HostStatus_HOST_STATUS_READY}}, nil
}

func dialGateway(t *testing.T, cfg ServerConfig, backend Backend, aud WireAuditor) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	srv := NewServer(cfg, &Dispatcher{Backend: backend}, nil, testIdentity{}, aud)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	ws, resp, err := websocket.Dial(ctx, "ws://"+ln.Addr().String()+"/connect", &websocket.DialOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		cancel()
		t.Fatalf("dial status=%v err=%v", resp, err)
	}
	t.Cleanup(func() {
		ws.CloseNow()
		shutdown, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = srv.Shutdown(shutdown)
		<-done
		cancel()
	})
	writeFrame(t, ctx, ws, &remotev1.Frame{Payload: &remotev1.Frame_ClientHello{ClientHello: &remotev1.ClientHello{ClientId: "client", ClientRunId: "client-run", ProtocolVersion: &remotev1.ProtocolVersion{Major: 1, Minor: 0}}}})
	if got := readFrame(t, ctx, ws).GetServerHello(); got == nil {
		t.Fatalf("ServerHello = %+v", got)
	}
	return ws, ctx
}

func writeFrame(t *testing.T, ctx context.Context, ws *websocket.Conn, f *remotev1.Frame) {
	t.Helper()
	b, err := protojson.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}
func readFrame(t *testing.T, ctx context.Context, ws *websocket.Conn) *remotev1.Frame {
	t.Helper()
	typ, b, err := ws.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("type %v", typ)
	}
	f := new(remotev1.Frame)
	if err = protojson.Unmarshal(b, f); err != nil {
		t.Fatal(err)
	}
	return f
}
