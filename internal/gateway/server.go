package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/coder/websocket"
	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/tailnet"
	"google.golang.org/protobuf/encoding/protojson"
)

const Subprotocol = "codex-remote.v1.protojson"

type IdentityProvider interface {
	WhoIs(context.Context, string) (tailnet.PeerIdentity, error)
}
type WireAuditor interface {
	RecordWire(context.Context, bool, string, string, string, []byte, *remotev1.Frame, remotev1.AuditOutcome) (string, error)
	Record(context.Context, *remotev1.AuditRecord) error
}

type ServerConfig struct {
	MaxFrameBytes                  int64
	SendQueueSize                  int
	WatchQueueSize                 int
	HeartbeatInterval              time.Duration
	ConnectionTimeout              time.Duration
	MaxWatches                     int
	HostID, HostRunID, HostVersion string
	Hello                          func(context.Context) (*remotev1.ServerHello, error)
	RenewForegroundCodexes         func(context.Context, []string) error
	AuditError                     func(error)
}

type Server struct {
	cfg        ServerConfig
	dispatcher *Dispatcher
	events     *activity.Store
	identity   IdentityProvider
	audit      WireAuditor
	http       *http.Server
}

func NewServer(cfg ServerConfig, dispatcher *Dispatcher, events *activity.Store, identity IdentityProvider, audit WireAuditor) *Server {
	if cfg.MaxFrameBytes <= 0 {
		cfg.MaxFrameBytes = 4 << 20
	}
	if cfg.SendQueueSize <= 0 {
		cfg.SendQueueSize = 256
	}
	if cfg.WatchQueueSize <= 0 {
		cfg.WatchQueueSize = 128
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	if cfg.ConnectionTimeout <= 0 {
		cfg.ConnectionTimeout = 45 * time.Second
	}
	if cfg.MaxWatches <= 0 {
		cfg.MaxWatches = 64
	}
	return &Server{cfg: cfg, dispatcher: dispatcher, events: events, identity: identity, audit: audit}
}

// Serve consumes only the listener supplied by embedded tsnet. It never opens
// a host socket itself.
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("tailnet listener is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", s.handleConnect)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	err := s.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *Server) Shutdown(ctx context.Context) error {
	if s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !hasSubprotocol(r.Header.Values("Sec-WebSocket-Protocol"), Subprotocol) {
		http.Error(w, "required WebSocket subprotocol missing", http.StatusBadRequest)
		return
	}
	var peer tailnet.PeerIdentity
	var err error
	if s.identity == nil {
		http.Error(w, "Tailnet identity unavailable", http.StatusServiceUnavailable)
		return
	}
	peer, err = s.identity.WhoIs(r.Context(), r.RemoteAddr)
	if err != nil {
		http.Error(w, "Tailnet identity required", http.StatusForbidden)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}, OriginPatterns: []string{"*"}})
	if err != nil {
		return
	}
	// Read the full message so an oversized formal frame can receive the V1
	// application Close before the WebSocket 1009 handshake. The protocol is a
	// functionality-first Demo; the configured formal limit is enforced below.
	ws.SetReadLimit(-1)
	ctx, cancel := context.WithCancel(context.Background())
	c := &connection{server: s, ws: ws, id: newConnectionID(), peer: peer, ctx: ctx, cancel: cancel, send: make(chan outbound, s.cfg.SendQueueSize), control: make(chan outbound, 1), watches: make(map[string]*activity.Watch)}
	c.lastValid.Store(time.Now().UnixNano())
	c.auditAction("connection.open", remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, peer.NodeName)
	go c.writeLoop()
	go c.heartbeat()
	c.readLoop()
}

type outbound struct {
	frame       *remotev1.Frame
	closeStatus websocket.StatusCode
	closeReason string
}
type connection struct {
	server                *Server
	ws                    *websocket.Conn
	id                    string
	peer                  tailnet.PeerIdentity
	ctx                   context.Context
	cancel                context.CancelFunc
	send                  chan outbound
	control               chan outbound
	lastValid             atomic.Int64
	clientID, clientRunID string
	watchesMu             sync.Mutex
	watches               map[string]*activity.Watch
	closeOnce             sync.Once
	lifecycleCloseOnce    sync.Once
}

func (c *connection) readLoop() {
	defer c.shutdown(websocket.StatusNormalClosure, "connection closed")
	helloDeadline := time.Now().Add(c.server.cfg.ConnectionTimeout)
	for {
		readCtx := c.ctx
		if c.clientID == "" {
			var cancel context.CancelFunc
			readCtx, cancel = context.WithDeadline(c.ctx, helloDeadline)
			typ, raw, err := c.ws.Read(readCtx)
			cancel()
			if err != nil {
				return
			}
			if !c.handleRaw(typ, raw, true) {
				return
			}
			continue
		}
		typ, raw, err := c.ws.Read(readCtx)
		if err != nil {
			return
		}
		if !c.handleRaw(typ, raw, false) {
			return
		}
	}
}

func (c *connection) handleRaw(typ websocket.MessageType, raw []byte, helloOnly bool) bool {
	if int64(len(raw)) > c.server.cfg.MaxFrameBytes {
		_, err := c.auditWire(true, raw, nil, remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED)
		c.reportAudit(err)
		c.protocolClose(remotev1.CloseCode_CLOSE_CODE_FRAME_TOO_LARGE, "inbound frame exceeds limit", websocket.StatusMessageTooBig)
		return false
	}
	if typ != websocket.MessageText {
		c.protocolClose(remotev1.CloseCode_CLOSE_CODE_INVALID_FRAME, "binary WebSocket messages are unsupported", websocket.StatusUnsupportedData)
		return false
	}
	frame := new(remotev1.Frame)
	err := protojson.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(raw, frame)
	if err != nil || frame.Payload == nil {
		_, auditErr := c.auditWire(true, raw, nil, remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED)
		c.reportAudit(auditErr)
		c.protocolClose(remotev1.CloseCode_CLOSE_CODE_INVALID_FRAME, "invalid ProtoJSON Frame", websocket.StatusProtocolError)
		return false
	}
	parentRecordID, auditErr := c.auditWire(true, raw, frame, remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED)
	c.reportAudit(auditErr)
	c.lastValid.Store(time.Now().UnixNano())
	if helloOnly {
		h := frame.GetClientHello()
		if h == nil {
			c.protocolClose(remotev1.CloseCode_CLOSE_CODE_HELLO_REQUIRED, "ClientHello must be the first frame", websocket.StatusProtocolError)
			return false
		}
		if h.ClientId == "" || h.ClientRunId == "" || h.ProtocolVersion == nil || h.ProtocolVersion.Major != 1 || h.ProtocolVersion.Minor != 2 || h.ProtocolVersion.Patch != 0 {
			c.protocolClose(remotev1.CloseCode_CLOSE_CODE_PROTOCOL_VERSION_UNSUPPORTED, "protocol 1.2.0 and client identity are required", websocket.StatusProtocolError)
			return false
		}
		c.clientID = h.ClientId
		c.clientRunID = h.ClientRunId
		c.sendHello()
		return true
	}
	switch {
	case frame.GetClientHello() != nil:
		c.protocolClose(remotev1.CloseCode_CLOSE_CODE_INVALID_FRAME, "duplicate ClientHello", websocket.StatusProtocolError)
		return false
	case frame.GetRequest() != nil:
		go c.handleRequest(frame.GetRequest(), parentRecordID)
	case frame.GetPing() != nil:
		p := frame.GetPing()
		c.enqueue(&remotev1.Frame{Payload: &remotev1.Frame_Pong{Pong: &remotev1.Pong{Nonce: p.Nonce, PingSentAtUnixMs: p.SentAtUnixMs, PongSentAtUnixMs: time.Now().UnixMilli()}}})
	case frame.GetPong() != nil:
		ids := frame.GetPong().GetForegroundCodexIds()
		if len(ids) > 0 && c.server.cfg.RenewForegroundCodexes != nil {
			c.reportAudit(c.server.cfg.RenewForegroundCodexes(c.ctx, append([]string(nil), ids...)))
		}
	case frame.GetClose() != nil:
		return false
	default:
		c.protocolClose(remotev1.CloseCode_CLOSE_CODE_INVALID_FRAME, "client frame type is not allowed", websocket.StatusProtocolError)
		return false
	}
	return true
}

func (c *connection) sendHello() {
	hello := &remotev1.ServerHello{ConnectionId: c.id, HostId: c.server.cfg.HostID, HostRunId: c.server.cfg.HostRunID, ProtocolVersion: &remotev1.ProtocolVersion{Major: 1, Minor: 2, Patch: 0}, HostVersion: c.server.cfg.HostVersion, HeartbeatIntervalMs: uint64(c.server.cfg.HeartbeatInterval.Milliseconds()), ConnectionTimeoutMs: uint64(c.server.cfg.ConnectionTimeout.Milliseconds()), MaxFrameBytes: uint64(c.server.cfg.MaxFrameBytes)}
	if c.server.cfg.Hello != nil {
		if provided, err := c.server.cfg.Hello(c.ctx); err == nil && provided != nil {
			hello = provided
			hello.ConnectionId = c.id
			hello.HostId = c.server.cfg.HostID
			hello.HostRunId = c.server.cfg.HostRunID
			hello.ProtocolVersion = &remotev1.ProtocolVersion{Major: 1, Minor: 2, Patch: 0}
			hello.HeartbeatIntervalMs = uint64(c.server.cfg.HeartbeatInterval.Milliseconds())
			hello.ConnectionTimeoutMs = uint64(c.server.cfg.ConnectionTimeout.Milliseconds())
			hello.MaxFrameBytes = uint64(c.server.cfg.MaxFrameBytes)
		}
	}
	c.enqueue(&remotev1.Frame{Payload: &remotev1.Frame_ServerHello{ServerHello: hello}})
}

func (c *connection) handleRequest(req *remotev1.Request, parentRecordID string) {
	if invalid := validateRequest(req); invalid != nil {
		c.enqueueRPC(req, invalid, parentRecordID)
		return
	}
	ctx := c.ctx
	if req.DeadlineUnixMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(c.ctx, time.UnixMilli(req.DeadlineUnixMs))
		defer cancel()
	}
	switch x := req.Request.(type) {
	case *remotev1.Request_WatchCodex:
		c.handleWatch(ctx, req, x.WatchCodex, parentRecordID)
		return
	case *remotev1.Request_UnwatchCodex:
		c.handleUnwatch(ctx, req, x.UnwatchCodex, parentRecordID)
		return
	}
	resp, err := c.server.dispatcher.Dispatch(ctx, req)
	if err != nil {
		resp = errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_INTERNAL_ERROR, err.Error(), true)
	}
	c.enqueueRPC(req, resp, parentRecordID)
}

func (c *connection) handleWatch(ctx context.Context, envelope *remotev1.Request, req *remotev1.WatchCodexRequest, parentRecordID string) {
	requestID := envelope.RequestId
	respond := func(resp *remotev1.Response) bool { return c.enqueueRPC(envelope, resp, parentRecordID) }
	if req == nil || req.CodexId == "" {
		respond(errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "codex_id is required", false))
		return
	}
	if err := ctx.Err(); err != nil {
		respond(errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, "request deadline has elapsed", true))
		return
	}
	if c.server.events == nil {
		respond(errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_HOST_NOT_READY, "event store unavailable", true))
		return
	}
	if req.AfterEventSeq != nil && req.AfterHostRunId == "" {
		respond(errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "after_host_run_id is required with after_event_seq", false))
		return
	}
	if req.AfterEventSeq == nil && req.AfterHostRunId != "" {
		respond(errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "after_host_run_id requires after_event_seq", false))
		return
	}
	c.watchesMu.Lock()
	if old := c.watches[req.CodexId]; old != nil {
		old.Cancel()
		delete(c.watches, req.CodexId)
	}
	if len(c.watches) >= c.server.cfg.MaxWatches {
		c.watchesMu.Unlock()
		respond(errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_CONFLICT, "maximum watches reached", false))
		return
	}
	var watch *activity.Watch
	var err error
	if req.AfterEventSeq != nil && req.AfterHostRunId != c.server.cfg.HostRunID {
		watch, err = c.server.events.ForceReset(ctx, req.CodexId, remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED, c.server.cfg.WatchQueueSize)
	} else {
		watch, err = c.server.events.Watch(ctx, req.CodexId, req.AfterEventSeq, c.server.cfg.WatchQueueSize)
	}
	if err != nil {
		c.watchesMu.Unlock()
		code, retry := watchError(err)
		respond(errorResponse(requestID, code, err.Error(), retry))
		return
	}
	c.watches[req.CodexId] = watch
	resp := &remotev1.Response{RequestId: requestID, RespondedAtUnixMs: time.Now().UnixMilli(), Result: &remotev1.Response_WatchCodex{WatchCodex: watch.Response}}
	if !respond(resp) {
		watch.Cancel()
		delete(c.watches, req.CodexId)
		c.watchesMu.Unlock()
		return
	}
	for _, ev := range watch.Replay {
		if !c.enqueueEvent(ev) {
			watch.Cancel()
			delete(c.watches, req.CodexId)
			c.watchesMu.Unlock()
			return
		}
	}
	// Holding watchesMu until Response and replay are queued establishes the
	// observable boundary. A replacement cannot register its new watch until
	// all old-watch output is ahead of the new Watch Response.
	c.watchesMu.Unlock()
	go c.forwardWatch(req.CodexId, watch)
}

func (c *connection) forwardWatch(codexID string, watch *activity.Watch) {
	for {
		select {
		case ev, ok := <-watch.Events:
			if !ok {
				c.watchesMu.Lock()
				if c.watches[codexID] == watch {
					delete(c.watches, codexID)
				}
				c.watchesMu.Unlock()
				return
			}
			c.watchesMu.Lock()
			current := c.watches[codexID] == watch
			if current {
				current = c.enqueueEvent(ev)
			}
			c.watchesMu.Unlock()
			if !current {
				return
			}
		case <-watch.Slow:
			c.watchesMu.Lock()
			current := c.watches[codexID] == watch
			c.watchesMu.Unlock()
			if current {
				c.protocolClose(remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER, "bounded watch queue is full", websocket.StatusCode(4001))
			}
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *connection) handleUnwatch(ctx context.Context, envelope *remotev1.Request, req *remotev1.UnwatchCodexRequest, parentRecordID string) {
	requestID := envelope.RequestId
	if err := ctx.Err(); err != nil {
		c.enqueueRPC(envelope, errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, "request deadline has elapsed", true), parentRecordID)
		return
	}
	if req == nil || req.CodexId == "" {
		c.enqueueRPC(envelope, errorResponse(requestID, remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "codex_id is required", false), parentRecordID)
		return
	}
	c.watchesMu.Lock()
	if watch := c.watches[req.CodexId]; watch != nil {
		watch.Cancel()
		delete(c.watches, req.CodexId)
	}
	resp := &remotev1.Response{RequestId: requestID, RespondedAtUnixMs: time.Now().UnixMilli(), Result: &remotev1.Response_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexResponse{CodexId: req.CodexId}}}
	// The lock makes every old-watch Event precede this response. Once the
	// response is queued, the old buffer and goroutine can no longer emit.
	c.enqueueRPC(envelope, resp, parentRecordID)
	c.watchesMu.Unlock()
}

func watchError(err error) (remotev1.ErrorCode, bool) {
	switch {
	case errors.Is(err, activity.ErrCodexNotFound):
		return remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, false
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, true
	default:
		return remotev1.ErrorCode_ERROR_CODE_INTERNAL_ERROR, true
	}
}

func (c *connection) enqueueRPC(req *remotev1.Request, resp *remotev1.Response, parentRecordID string) bool {
	c.auditRPC(req, resp, parentRecordID)
	return c.enqueueResponse(resp)
}
func (c *connection) enqueueResponse(r *remotev1.Response) bool {
	return c.enqueue(&remotev1.Frame{Payload: &remotev1.Frame_Response{Response: r}})
}
func (c *connection) enqueueEvent(e *remotev1.Event) bool {
	return c.enqueue(&remotev1.Frame{Payload: &remotev1.Frame_Event{Event: e}})
}

func (c *connection) enqueue(frame *remotev1.Frame) bool {
	select {
	case c.send <- outbound{frame: frame}:
		return true
	default:
		c.protocolClose(remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER, "bounded send queue is full", websocket.StatusCode(4001))
		return false
	}
}
func (c *connection) writeLoop() {
	for {
		// Control close bypasses a saturated data queue and is always written
		// before initiating the WebSocket close handshake.
		select {
		case out := <-c.control:
			c.writeOutbound(out)
			return
		default:
		}
		select {
		case out := <-c.control:
			c.writeOutbound(out)
			return
		case out := <-c.send:
			if !c.writeOutbound(out) {
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}
func (c *connection) writeOutbound(out outbound) bool {
	if out.frame != nil {
		raw, err := protojson.Marshal(out.frame)
		if err != nil {
			c.writeFormalClose(remotev1.CloseCode_CLOSE_CODE_INTERNAL_PROTOCOL_ERROR, "outbound frame encoding failed", websocket.StatusInternalError)
			return false
		}
		if int64(len(raw)) > c.server.cfg.MaxFrameBytes {
			c.writeFormalClose(remotev1.CloseCode_CLOSE_CODE_FRAME_TOO_LARGE, "outbound frame exceeds limit", websocket.StatusMessageTooBig)
			return false
		}
		_, auditErr := c.auditWire(false, raw, out.frame, remotev1.AuditOutcome_AUDIT_OUTCOME_STARTED)
		c.reportAudit(auditErr)
		writeCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		err = c.ws.Write(writeCtx, websocket.MessageText, raw)
		cancel()
		outcome := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
		if err != nil {
			outcome = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
		}
		_, auditErr = c.auditWire(false, raw, out.frame, outcome)
		c.reportAudit(auditErr)
		if err != nil {
			c.shutdown(websocket.StatusInternalError, "write failed")
			return false
		}
	}
	if out.closeStatus != 0 {
		_ = c.ws.Close(out.closeStatus, out.closeReason)
		c.cancel()
		return false
	}
	return true
}

// writeFormalClose is used from the writer itself, so it cannot enqueue onto
// the control channel. It best-effort writes the small application Close first
// and only then starts the WebSocket close handshake.
func (c *connection) writeFormalClose(code remotev1.CloseCode, message string, status websocket.StatusCode) {
	frame := &remotev1.Frame{Payload: &remotev1.Frame_Close{Close: &remotev1.Close{Code: code, Message: message, ReconnectAllowed: code == remotev1.CloseCode_CLOSE_CODE_CONNECTION_TIMEOUT || code == remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER}}}
	raw, err := protojson.Marshal(frame)
	if err == nil {
		_, auditErr := c.auditWire(false, raw, frame, remotev1.AuditOutcome_AUDIT_OUTCOME_STARTED)
		c.reportAudit(auditErr)
		writeCtx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
		err = c.ws.Write(writeCtx, websocket.MessageText, raw)
		cancel()
		outcome := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
		if err != nil {
			outcome = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
		}
		_, auditErr = c.auditWire(false, raw, frame, outcome)
		c.reportAudit(auditErr)
	}
	_ = c.ws.Close(status, message)
	c.cancel()
}
func (c *connection) heartbeat() {
	ticker := time.NewTicker(c.server.cfg.HeartbeatInterval)
	defer ticker.Stop()
	var nonce uint64
	for {
		select {
		case now := <-ticker.C:
			if now.Sub(time.Unix(0, c.lastValid.Load())) >= c.server.cfg.ConnectionTimeout {
				c.protocolClose(remotev1.CloseCode_CLOSE_CODE_CONNECTION_TIMEOUT, "connection timed out", websocket.StatusCode(4000))
				return
			}
			nonce++
			c.enqueue(&remotev1.Frame{Payload: &remotev1.Frame_Ping{Ping: &remotev1.Ping{Nonce: nonce, SentAtUnixMs: now.UnixMilli()}}})
		case <-c.ctx.Done():
			return
		}
	}
}
func (c *connection) protocolClose(code remotev1.CloseCode, message string, status websocket.StatusCode) {
	c.closeOnce.Do(func() {
		frame := &remotev1.Frame{Payload: &remotev1.Frame_Close{Close: &remotev1.Close{Code: code, Message: message, ReconnectAllowed: code == remotev1.CloseCode_CLOSE_CODE_CONNECTION_TIMEOUT || code == remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER}}}
		c.control <- outbound{frame: frame, closeStatus: status, closeReason: message}
	})
}
func (c *connection) shutdown(status websocket.StatusCode, reason string) {
	c.closeOnce.Do(func() { c.cancel(); _ = c.ws.Close(status, reason) })
	c.lifecycleCloseOnce.Do(func() {
		c.auditAction("connection.close", remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, reason)
	})
	c.watchesMu.Lock()
	for id, w := range c.watches {
		w.Cancel()
		delete(c.watches, id)
	}
	c.watchesMu.Unlock()
}
func (c *connection) auditWire(in bool, raw []byte, frame *remotev1.Frame, out remotev1.AuditOutcome) (string, error) {
	if c.server.audit == nil {
		return "", nil
	}
	return c.server.audit.RecordWire(c.ctx, in, c.id, c.clientID, c.clientRunID, raw, frame, out)
}
func (c *connection) auditRPC(req *remotev1.Request, resp *remotev1.Response, parentRecordID string) {
	if c.server.audit == nil {
		return
	}
	out := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
	if resp != nil && resp.GetError() != nil {
		out = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
	}
	op, _ := operation(req)
	rec := &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_RPC, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: out, Component: "gateway", Operation: "rpc." + op, RequestId: requestID(req), ConnectionId: c.id, ParentRecordId: parentRecordID}
	if resp != nil && resp.GetError() != nil {
		rec.Error = resp.GetError()
	}
	c.reportAudit(c.server.audit.Record(c.ctx, rec))
}

func (c *connection) auditAction(operation string, outcome remotev1.AuditOutcome, message string) {
	if c.server.audit == nil {
		return
	}
	c.reportAudit(c.server.audit.Record(c.ctx, &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_CONNECTION_LIFECYCLE, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: outcome, Component: "gateway", Operation: operation, Message: message, ConnectionId: c.id, ClientId: c.clientID, ClientRunId: c.clientRunID}))
}

func (c *connection) reportAudit(err error) {
	if err != nil && c.server.cfg.AuditError != nil {
		c.server.cfg.AuditError(err)
	}
}

func hasSubprotocol(values []string, want string) bool {
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			if strings.TrimSpace(part) == want {
				return true
			}
		}
	}
	return false
}
func newConnectionID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "conn_" + hex.EncodeToString(b)
}
