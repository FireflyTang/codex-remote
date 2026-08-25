package codex

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/coder/websocket"
	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/gateway"
	"github.com/kylin1993/codex-remote/internal/persistence"
	hostruntime "github.com/kylin1993/codex-remote/internal/runtime"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type fixedAdapterRuntime struct {
	adapter *adapter.Adapter
}

func (r fixedAdapterRuntime) Adapter() (*adapter.Adapter, error) { return r.adapter, nil }
func (r fixedAdapterRuntime) Events() <-chan adapter.Event       { return r.adapter.Events() }
func (fixedAdapterRuntime) State() hostruntime.State             { return hostruntime.State{} }

func restoreTestAdapter(t *testing.T) *adapter.Adapter {
	return restoreTestAdapterWithName(t, "")
}

func restoreTestAdapterWithName(t *testing.T, threadName string) *adapter.Adapter {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app-server.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var message adapter.Message
			if json.Unmarshal(raw, &message) != nil || len(message.ID) == 0 {
				continue
			}
			result := any(map[string]any{})
			switch message.Method {
			case "thread/read", "thread/resume":
				thread := map[string]any{"id": "thread", "cwd": "/tmp", "turns": []any{}}
				if threadName != "" {
					thread["name"] = threadName
				}
				result = map[string]any{"thread": thread}
			case "turn/start":
				result = map[string]any{"turn": map[string]any{"id": "turn", "status": "inProgress"}}
			}
			response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": result})
			if conn.Write(context.Background(), websocket.MessageText, response) != nil {
				return
			}
		}
	})}
	go server.Serve(listener)
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	client, err := adapter.Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	ad, _, err := adapter.Initialize(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ad.Close() })
	return ad
}

func importTestAdapter(t *testing.T, cwd string) *adapter.Adapter {
	t.Helper()
	path := filepath.Join(t.TempDir(), "import-app-server.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var message adapter.Message
			if json.Unmarshal(raw, &message) != nil || len(message.ID) == 0 {
				continue
			}
			var params struct {
				ThreadID string `json:"threadId"`
			}
			_ = json.Unmarshal(message.Params, &params)
			if params.ThreadID == "empty-thread" && message.Method == "thread/read" {
				response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32600, "message": "thread empty-thread" + unmaterializedIncludeTurnsSuffix}})
				_ = conn.Write(context.Background(), websocket.MessageText, response)
				continue
			}
			if params.ThreadID == "empty-thread" && message.Method == "thread/resume" {
				response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32600, "message": unmaterializedResumePrefix + "empty-thread"}})
				_ = conn.Write(context.Background(), websocket.MessageText, response)
				continue
			}
			result := any(map[string]any{})
			switch message.Method {
			case "thread/list":
				result = map[string]any{"data": []any{}, "nextCursor": nil}
			case "thread/read", "thread/resume":
				result = map[string]any{"thread": map[string]any{"id": "historical-thread", "sessionId": "historical-thread", "cwd": cwd, "name": "historical", "source": "exec", "turns": []any{map[string]any{"id": "old-turn", "status": "completed", "items": []any{}}}}}
			case "turn/start":
				result = map[string]any{"turn": map[string]any{"id": "turn", "status": "inProgress"}}
			}
			response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": result})
			if conn.Write(context.Background(), websocket.MessageText, response) != nil {
				return
			}
		}
	})}
	go server.Serve(listener)
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	client, err := adapter.Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	ad, _, err := adapter.Initialize(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ad.Close() })
	return ad
}

func testManager(t *testing.T) *Manager {
	t.Helper()
	m, _ := testManagerAt(t, filepath.Join(t.TempDir(), "state.db"))
	return m
}

func TestImportSessionAllowsTemporarilyMissingWorkspaceRoot(t *testing.T) {
	m := testManager(t)
	root := filepath.Join(t.TempDir(), "missing", "historical-workspace")
	m.Runtime = fixedAdapterRuntime{adapter: importTestAdapter(t, root)}
	imported, err := m.ImportSession(context.Background(), &remotev1.ImportSessionRequest{SessionId: "historical-thread", Source: "exec"})
	if err != nil {
		t.Fatal(err)
	}
	if imported == nil || imported.Codex == nil || imported.Codex.Cwd != root {
		t.Fatalf("imported=%+v", imported)
	}
	if _, err := m.GetWorkspace(context.Background(), &remotev1.GetWorkspaceRequest{CodexId: imported.Codex.CodexId}); err == nil {
		t.Fatal("GetWorkspace succeeded while imported cwd was missing")
	} else {
		var rpc *gateway.RPCError
		if !errors.As(err, &rpc) || rpc.Detail.GetCode() != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND {
			t.Fatalf("GetWorkspace error=%v", err)
		}
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	workspace, err := m.GetWorkspace(context.Background(), &remotev1.GetWorkspaceRequest{CodexId: imported.Codex.CodexId})
	if err != nil {
		t.Fatal(err)
	}
	if workspace.WorkspaceRoot != root || workspace.AccessState == nil {
		t.Fatalf("workspace=%+v", workspace)
	}
}

func testManagerAt(t *testing.T, path string) (*Manager, string) {
	t.Helper()
	p, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	c := &remotev1.Codex{CodexId: "c", ThreadId: "thread", Cwd: "/tmp", Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, CreatedAtUnixMs: 1, LastActivityAtUnixMs: 1}
	m := &Manager{Persistence: p, Events: activity.NewStore(p, nil, 16), byID: map[string]*remotev1.CurrentView{"c": {Codex: c}}, byThread: map[string]string{"thread": "c"}, chunks: make(map[string]uint64)}
	if err = m.persistState(context.Background(), m.byID["c"]); err != nil {
		t.Fatal(err)
	}
	return m, path
}

func execSQLite(t *testing.T, path, statement string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func TestSetRunningPersistsRegistryAndCorrelation(t *testing.T) {
	m := testManager(t)
	m.setRunning(context.Background(), "c", "turn", "request-1")
	rec, err := m.Persistence.GetCodex(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != remotev1.CodexStatus_CODEX_STATUS_RUNNING.String() || rec.ActiveTurnID != "turn" {
		t.Fatalf("record %+v", rec)
	}
	after := uint64(0)
	w, err := m.Events.Watch(context.Background(), "c", &after, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if len(w.Replay) != 2 || w.Replay[0].CausedByRequestId != "request-1" || w.Replay[1].GetCodexUpdated() == nil {
		t.Fatalf("events %+v", w.Replay)
	}
}

func TestNormalizeUnmaterializedHistoryIsNarrowlyScoped(t *testing.T) {
	m := testManager(t)
	c := m.byID["c"].Codex
	matching := &adapter.RPCError{Code: -32600, Message: "thread " + c.ThreadId + unmaterializedIncludeTurnsSuffix}
	if !m.normalizeUnmaterializedHistory(c, matching) {
		t.Fatal("managed remote-created thread before its first turn was not normalized")
	}
	if !m.normalizeUnmaterializedHistory(c, &adapter.RPCError{Code: -32600, Message: unmaterializedResumePrefix + c.ThreadId}) {
		t.Fatal("known no-rollout thread before its first turn was not normalized")
	}

	tests := []struct {
		name  string
		codex *remotev1.Codex
		err   error
	}{
		{name: "different invalid request", codex: c, err: &adapter.RPCError{Code: -32600, Message: "invalid includeTurns option"}},
		{name: "different code", codex: c, err: &adapter.RPCError{Code: -32601, Message: matching.Message}},
		{name: "different thread", codex: c, err: &adapter.RPCError{Code: -32600, Message: "thread other" + unmaterializedIncludeTurnsSuffix}},
		{name: "unstructured error", codex: c, err: errors.New(matching.Message)},
		{name: "imported thread", codex: &remotev1.Codex{CodexId: c.CodexId, ThreadId: c.ThreadId, Origin: remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING}, err: matching},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if m.normalizeUnmaterializedHistory(tt.codex, tt.err) {
				t.Fatal("unexpected empty-history normalization")
			}
		})
	}
}

func TestNormalizeUnmaterializedResumeIsNarrowlyScoped(t *testing.T) {
	m := testManager(t)
	remoteCreated := &remotev1.Codex{ThreadId: "thread", Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED}
	matching := &adapter.RPCError{Code: -32600, Message: unmaterializedResumePrefix + "thread"}
	tests := []struct {
		name  string
		codex *remotev1.Codex
		err   error
		want  bool
	}{
		{name: "exact unmaterialized remote-created", codex: remoteCreated, err: matching, want: true},
		{name: "imported session", codex: &remotev1.Codex{ThreadId: "thread", Origin: remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING}, err: matching},
		{name: "different code", codex: remoteCreated, err: &adapter.RPCError{Code: -32601, Message: matching.Message}},
		{name: "different message", codex: remoteCreated, err: &adapter.RPCError{Code: -32600, Message: "other invalid request"}},
		{name: "different thread", codex: remoteCreated, err: &adapter.RPCError{Code: -32600, Message: unmaterializedResumePrefix + "other"}},
		{name: "ordinary error", codex: remoteCreated, err: errors.New("no rollout found")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := m.normalizeUnmaterializedResume(tt.codex, tt.err); got != tt.want {
				t.Fatalf("normalizeUnmaterializedResume=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeUnmaterializedHistorySurvivesManagerRebuild(t *testing.T) {
	c := &remotev1.Codex{CodexId: "c", ThreadId: "thread", Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED}
	err := &adapter.RPCError{Code: -32600, Message: "thread " + c.ThreadId + unmaterializedIncludeTurnsSuffix}
	rebuilt := &Manager{}
	if !rebuilt.normalizeUnmaterializedHistory(c, err) {
		t.Fatal("rebuilt Manager did not preserve remote-created unmaterialized history semantics")
	}
	imported := proto.Clone(c).(*remotev1.Codex)
	imported.Origin = remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING
	if rebuilt.normalizeUnmaterializedHistory(imported, err) {
		t.Fatal("rebuilt Manager normalized an imported existing thread")
	}
}

func TestListHistoryValidatesPageTokenBeforeUnmaterializedRead(t *testing.T) {
	m := testManager(t)
	// Runtime is intentionally nil: invalid pagination must return before any
	// thread/read attempt, including the unmaterialized-thread normalization.
	tests := []struct {
		name  string
		token string
	}{
		{name: "malformed", token: "not-a-page-token"},
		{name: "different codex", token: encodePageToken(pageToken{Operation: "history", Query: "other", Offset: 1})},
		{name: "different RPC", token: encodePageToken(pageToken{Operation: "codexes", Query: "c", Offset: 1})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.ListHistory(context.Background(), &remotev1.ListHistoryRequest{CodexId: "c", Page: &remotev1.PageRequest{PageToken: tt.token}})
			var rpc *gateway.RPCError
			if !errors.As(err, &rpc) || rpc.Detail.GetCode() != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
				t.Fatalf("ListHistory error=%v, want INVALID_REQUEST", err)
			}
		})
	}
}

func TestSetRunningPropagatesPersistenceFailure(t *testing.T) {
	m := testManager(t)
	if err := m.Persistence.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.setRunning(context.Background(), "c", "turn", "request"); err == nil {
		t.Fatal("persistence failure must not be reported as a successful transition")
	}
}

func TestAdapterCompletionAndDeltaSequencePersist(t *testing.T) {
	m := testManager(t)
	m.setRunning(context.Background(), "c", "turn", "start")
	m.applyAdapterEvent(context.Background(), adapter.Event{Kind: adapter.EventItemDelta, ThreadID: "thread", TurnID: "turn", ItemID: "item", Params: []byte(`{"delta":"a"}`)})
	m.applyAdapterEvent(context.Background(), adapter.Event{Kind: adapter.EventItemDelta, ThreadID: "thread", TurnID: "turn", ItemID: "item", Params: []byte(`{"delta":"b"}`)})
	m.applyAdapterEvent(context.Background(), adapter.Event{Kind: adapter.EventTurnUpdated, Method: "turn/completed", ThreadID: "thread", TurnID: "turn"})
	rec, err := m.Persistence.GetCodex(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != remotev1.CodexStatus_CODEX_STATUS_IDLE.String() || rec.ActiveTurnID != "" {
		t.Fatalf("record %+v", rec)
	}
	after := uint64(2)
	w, err := m.Events.Watch(context.Background(), "c", &after, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if len(w.Replay) != 4 {
		t.Fatalf("replay %d", len(w.Replay))
	}
	if w.Replay[0].GetItemDelta().ChunkSeq != 1 || w.Replay[1].GetItemDelta().ChunkSeq != 2 {
		t.Fatalf("chunks %+v %+v", w.Replay[0], w.Replay[1])
	}
}

func TestCodexAndWarningCanonicalEvents(t *testing.T) {
	m := testManager(t)
	m.applyAdapterEvent(context.Background(), adapter.Event{Kind: adapter.EventCodexUpdated, Method: "thread/status/changed", ThreadID: "thread", Params: []byte(`{"status":"active"}`)})
	m.applyAdapterEvent(context.Background(), adapter.Event{Kind: adapter.EventWarningRaised, Method: "warning", ThreadID: "thread", Params: []byte(`{"message":"diagnostic"}`)})
	after := uint64(0)
	w, err := m.Events.Watch(context.Background(), "c", &after, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if len(w.Replay) != 2 || w.Replay[0].GetCodexUpdated() == nil || w.Replay[1].GetWarningRaised() == nil || w.Replay[1].GetWarningRaised().Warning.Message != "diagnostic" {
		t.Fatalf("replay %+v", w.Replay)
	}
}

func TestLeaseSweepWarningDedupAndAutomaticUnmanage(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)
	now := base.Add(89*time.Minute + 59*time.Second)
	m.Clock = func() time.Time { return now }
	m.LeaseDuration = 2 * time.Hour
	m.LeaseWarningBefore = 30 * time.Minute
	deadline := base.Add(2 * time.Hour).UnixMilli()
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = deadline
	m.warningDeadline["c"] = 0
	initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(ctx, initial); err != nil {
		t.Fatal(err)
	}
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.lookup("c"); got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED {
		t.Fatalf("89:59 state=%s", got.ManagementState)
	}

	now = base.Add(90 * time.Minute)
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := m.lookup("c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON || got.ManagedUntilUnixMs != deadline || len(got.Warnings) != 1 || got.Warnings[0].Code != remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON || got.Warnings[0].ManagedUntilUnixMs != deadline {
		t.Fatalf("expiring codex=%+v", got)
	}
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil || record.WarningDeadlineUnixMS != deadline || record.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON.String() {
		t.Fatalf("persisted expiring record=%+v err=%v", record, err)
	}
	after := uint64(0)
	w, err := m.Events.Watch(ctx, "c", &after, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Replay) != 2 || w.Replay[0].GetCodexUpdated() == nil || w.Replay[1].GetWarningRaised().GetWarning().GetManagedUntilUnixMs() != deadline {
		t.Fatalf("warning replay=%+v", w.Replay)
	}
	w.Cancel()

	now = base.Add(2 * time.Hour)
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m.lookup("c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || got.ManagedUntilUnixMs != 0 {
		t.Fatalf("expired idle codex=%+v", got)
	}
}

func TestLeaseExpiryAndManualUnmanageRespectBusyState(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	m.Clock = func() time.Time { return now }
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = now.UnixMilli()
	m.byID["c"].Codex.Status = remotev1.CodexStatus_CODEX_STATUS_RUNNING
	m.byID["c"].Codex.ActiveTurnId = "turn"
	m.byID["c"].ActiveTurn = &remotev1.TurnSnapshot{TurnId: "turn", Status: remotev1.TurnStatus_TURN_STATUS_RUNNING}
	m.mu.Unlock()
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := m.lookup("c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON || got.ManagedUntilUnixMs == 0 {
		t.Fatalf("busy expiry codex=%+v", got)
	}
	if _, err := m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"}); err == nil {
		t.Fatal("busy manual unmanage succeeded")
	} else {
		var rpc *gateway.RPCError
		if !errors.As(err, &rpc) || rpc.Detail.Code != remotev1.ErrorCode_ERROR_CODE_CODEX_BUSY {
			t.Fatalf("busy error=%v", err)
		}
	}
	m.mu.Lock()
	m.byID["c"].Codex.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
	m.byID["c"].Codex.ActiveTurnId = ""
	m.byID["c"].ActiveTurn = nil
	m.mu.Unlock()
	response, err := m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"})
	if err != nil || response.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("idle unmanage response=%+v err=%v", response, err)
	}
	response, err = m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"})
	if err != nil || response.Codex.CodexId != "c" {
		t.Fatalf("idempotent unmanage response=%+v err=%v", response, err)
	}
}

const failFirstEventViewTrigger = `CREATE TRIGGER fail_first_lifecycle_event BEFORE UPDATE OF current_view_json ON codexes
WHEN instr(CAST(NEW.current_view_json AS TEXT), '"headEventSeq":"1"') > 0
BEGIN SELECT RAISE(ABORT, 'injected lifecycle event failure'); END`

func TestLeaseWarningPublishFailureRollsBackAndRetriesOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	m, _ := testManagerAt(t, path)
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)
	deadline := base.Add(2 * time.Hour).UnixMilli()
	m.Clock = func() time.Time { return base.Add(90 * time.Minute) }
	m.LeaseWarningBefore = 30 * time.Minute
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = deadline
	initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(ctx, initial); err != nil {
		t.Fatal(err)
	}
	execSQLite(t, path, failFirstEventViewTrigger)
	if err := m.sweepLeases(ctx); err == nil || !strings.Contains(err.Error(), "injected lifecycle event failure") {
		t.Fatalf("sweep error=%v", err)
	}
	got, _ := m.lookup("c")
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || got.ManagedUntilUnixMs != deadline || len(got.Warnings) != 0 || record.WarningDeadlineUnixMS != 0 || record.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED.String() {
		t.Fatalf("failed warning was not rolled back: codex=%+v record=%+v", got, record)
	}
	reset, err := m.Events.Watch(ctx, "c", nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Response.GetResetView().GetCodex().GetManagementState() != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || len(reset.Response.GetResetView().GetCodex().GetWarnings()) != 0 {
		t.Fatalf("rollback RESET=%+v", reset.Response)
	}
	reset.Cancel()
	execSQLite(t, path, `DROP TRIGGER fail_first_lifecycle_event`)
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m.lookup("c")
	record, _ = m.Persistence.GetCodex(ctx, "c")
	head, _ := m.Persistence.EventHead(ctx, "c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON || len(got.Warnings) != 1 || record.WarningDeadlineUnixMS != deadline || head != 2 {
		t.Fatalf("warning retry duplicated or was lost: codex=%+v record=%+v head=%d", got, record, head)
	}
}

func TestLeaseWarningSecondPublishFailureKeepsTransitionAndRetriesWarning(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)
	deadline := base.Add(2 * time.Hour).UnixMilli()
	m.Clock = func() time.Time { return base.Add(90 * time.Minute) }
	m.LeaseWarningBefore = 30 * time.Minute
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = deadline
	initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(ctx, initial); err != nil {
		t.Fatal(err)
	}
	publishes := 0
	m.testPublishEvent = func(ctx context.Context, event *remotev1.Event, view *remotev1.CurrentView, provenance *remotev1.Provenance, parent string) (*remotev1.Event, error) {
		publishes++
		if publishes == 2 {
			return nil, errors.New("injected WarningRaised failure")
		}
		return m.Events.Publish(ctx, event, view, provenance, parent)
	}
	if err := m.sweepLeases(ctx); err == nil || !strings.Contains(err.Error(), "WarningRaised") {
		t.Fatalf("warning failure=%v", err)
	}
	got, _ := m.lookup("c")
	record, _ := m.Persistence.GetCodex(ctx, "c")
	head, _ := m.Persistence.EventHead(ctx, "c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON || len(got.Warnings) != 1 || record.WarningDeadlineUnixMS != 0 || head != 1 {
		t.Fatalf("second publish failure state=%+v record=%+v head=%d", got, record, head)
	}
	m.testPublishEvent = nil
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m.lookup("c")
	record, _ = m.Persistence.GetCodex(ctx, "c")
	head, _ = m.Persistence.EventHead(ctx, "c")
	if len(got.Warnings) != 1 || record.WarningDeadlineUnixMS != deadline || head != 3 {
		t.Fatalf("warning retry state=%+v record=%+v head=%d", got, record, head)
	}
}

func TestLeaseWarningUnknownOutcomesUseConservativeMarkers(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("publish_%d", failAt), func(t *testing.T) {
			m := testManager(t)
			ctx := context.Background()
			deadline := base.Add(2 * time.Hour).UnixMilli()
			m.Clock = func() time.Time { return base.Add(90 * time.Minute) }
			m.LeaseWarningBefore = 30 * time.Minute
			m.mu.Lock()
			m.ensureMapsLocked()
			m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
			m.byID["c"].Codex.ManagedUntilUnixMs = deadline
			initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
			m.mu.Unlock()
			if err := m.persistState(ctx, initial); err != nil {
				t.Fatal(err)
			}
			publishes := 0
			m.testPublishEvent = func(ctx context.Context, event *remotev1.Event, view *remotev1.CurrentView, provenance *remotev1.Provenance, parent string) (*remotev1.Event, error) {
				publishes++
				if publishes == failAt {
					return nil, persistence.ErrEventCommitOutcomeUnknown
				}
				return m.Events.Publish(ctx, event, view, provenance, parent)
			}
			if err := m.sweepLeases(ctx); !errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
				t.Fatalf("unknown outcome=%v", err)
			}
			got, _ := m.lookup("c")
			record, _ := m.Persistence.GetCodex(ctx, "c")
			if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON || len(got.Warnings) != 1 {
				t.Fatalf("unknown warning state=%+v", got)
			}
			wantMarker := int64(0)
			if failAt == 2 {
				wantMarker = deadline
			}
			if record.WarningDeadlineUnixMS != wantMarker {
				t.Fatalf("unknown marker=%d want=%d", record.WarningDeadlineUnixMS, wantMarker)
			}
			m.testPublishEvent = nil
			if err := m.sweepLeases(ctx); failAt == 1 && err != nil {
				t.Fatal(err)
			}
			got, _ = m.lookup("c")
			if len(got.Warnings) != 1 {
				t.Fatalf("unknown outcome duplicated warning=%+v", got.Warnings)
			}
			head, _ := m.Persistence.EventHead(ctx, "c")
			if failAt == 1 && head != 2 {
				t.Fatalf("first unknown did not retry both events: head=%d", head)
			}
			if failAt == 2 && head != 1 {
				t.Fatalf("second unknown was retried: head=%d", head)
			}
		})
	}
}

func TestLifecyclePublishFailureRollsBackManagementTransitions(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	tests := []struct {
		name      string
		configure func(*Manager)
		operation func(context.Context, *Manager) error
		checkOld  func(*testing.T, *remotev1.Codex)
		checkNew  func(*testing.T, *remotev1.Codex)
	}{
		{
			name: "automatic unmanage",
			configure: func(m *Manager) {
				m.Clock = func() time.Time { return base.Add(2 * time.Hour) }
			},
			operation: func(ctx context.Context, m *Manager) error { return m.sweepLeases(ctx) },
			checkOld: func(t *testing.T, c *remotev1.Codex) {
				if c.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || c.ManagedUntilUnixMs != base.Add(2*time.Hour).UnixMilli() {
					t.Fatalf("auto unmanage rollback=%+v", c)
				}
			},
			checkNew: func(t *testing.T, c *remotev1.Codex) {
				if c.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || c.ManagedUntilUnixMs != 0 {
					t.Fatalf("auto unmanage retry=%+v", c)
				}
			},
		},
		{
			name: "manual unmanage",
			configure: func(m *Manager) {
				m.Clock = func() time.Time { return base }
			},
			operation: func(ctx context.Context, m *Manager) error {
				_, err := m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"})
				return err
			},
			checkOld: func(t *testing.T, c *remotev1.Codex) {
				if c.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED {
					t.Fatalf("manual unmanage rollback=%+v", c)
				}
			},
			checkNew: func(t *testing.T, c *remotev1.Codex) {
				if c.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
					t.Fatalf("manual unmanage retry=%+v", c)
				}
			},
		},
		{
			name: "foreground renewal",
			configure: func(m *Manager) {
				m.Clock = func() time.Time { return base }
			},
			operation: func(ctx context.Context, m *Manager) error {
				return m.RenewForegroundCodexes(ctx, []string{"c"})
			},
			checkOld: func(t *testing.T, c *remotev1.Codex) {
				if c.ManagedUntilUnixMs != base.Add(2*time.Hour).UnixMilli() {
					t.Fatalf("renew rollback=%+v", c)
				}
			},
			checkNew: func(t *testing.T, c *remotev1.Codex) {
				if c.ManagedUntilUnixMs != base.Add(3*time.Hour).UnixMilli() {
					t.Fatalf("renew retry=%+v", c)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			m, _ := testManagerAt(t, path)
			m.LeaseDuration = 3 * time.Hour
			m.mu.Lock()
			m.ensureMapsLocked()
			m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
			m.byID["c"].Codex.ManagedUntilUnixMs = base.Add(2 * time.Hour).UnixMilli()
			initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
			m.mu.Unlock()
			tt.configure(m)
			if err := m.persistState(context.Background(), initial); err != nil {
				t.Fatal(err)
			}
			execSQLite(t, path, failFirstEventViewTrigger)
			if err := tt.operation(context.Background(), m); err == nil {
				t.Fatal("injected publish failure was swallowed")
			}
			old, _ := m.lookup("c")
			tt.checkOld(t, old)
			record, _ := m.Persistence.GetCodex(context.Background(), "c")
			if record.ManagementState != old.ManagementState.String() || record.ManagedUntilUnixMS != old.ManagedUntilUnixMs {
				t.Fatalf("durable rollback differs: codex=%+v record=%+v", old, record)
			}
			execSQLite(t, path, `DROP TRIGGER fail_first_lifecycle_event`)
			if err := tt.operation(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			updated, _ := m.lookup("c")
			tt.checkNew(t, updated)
		})
	}
}

func TestSingleLifecycleEventUnknownKeepsNewState(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	tests := []struct {
		name      string
		configure func(*Manager)
		operation func(context.Context, *Manager) error
		check     func(*testing.T, *remotev1.Codex)
	}{
		{name: "automatic unmanage", configure: func(m *Manager) { m.Clock = func() time.Time { return base.Add(2 * time.Hour) } }, operation: func(ctx context.Context, m *Manager) error { return m.sweepLeases(ctx) }, check: func(t *testing.T, c *remotev1.Codex) {
			if c.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
				t.Fatalf("auto unknown=%+v", c)
			}
		}},
		{name: "manual unmanage", configure: func(m *Manager) { m.Clock = func() time.Time { return base } }, operation: func(ctx context.Context, m *Manager) error {
			_, err := m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"})
			return err
		}, check: func(t *testing.T, c *remotev1.Codex) {
			if c.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
				t.Fatalf("manual unknown=%+v", c)
			}
		}},
		{name: "foreground renewal", configure: func(m *Manager) { m.Clock = func() time.Time { return base } }, operation: func(ctx context.Context, m *Manager) error { return m.RenewForegroundCodexes(ctx, []string{"c"}) }, check: func(t *testing.T, c *remotev1.Codex) {
			if c.ManagedUntilUnixMs != base.Add(3*time.Hour).UnixMilli() {
				t.Fatalf("renew unknown=%+v", c)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testManager(t)
			m.LeaseDuration = 3 * time.Hour
			m.mu.Lock()
			m.ensureMapsLocked()
			m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
			m.byID["c"].Codex.ManagedUntilUnixMs = base.Add(2 * time.Hour).UnixMilli()
			initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
			m.mu.Unlock()
			tt.configure(m)
			if err := m.persistState(context.Background(), initial); err != nil {
				t.Fatal(err)
			}
			m.testPublishEvent = func(context.Context, *remotev1.Event, *remotev1.CurrentView, *remotev1.Provenance, string) (*remotev1.Event, error) {
				return nil, persistence.ErrEventCommitOutcomeUnknown
			}
			if err := tt.operation(context.Background(), m); !errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
				t.Fatalf("unknown outcome=%v", err)
			}
			got, _ := m.lookup("c")
			tt.check(t, got)
			record, _ := m.Persistence.GetCodex(context.Background(), "c")
			if record.ManagementState != got.ManagementState.String() || record.ManagedUntilUnixMS != got.ManagedUntilUnixMs {
				t.Fatalf("unknown durable state differs: codex=%+v record=%+v", got, record)
			}
		})
	}
}

func TestLifecycleRollbackFailureIsReportedAsDegraded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	m, _ := testManagerAt(t, path)
	ctx := context.Background()
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = time.Now().Add(time.Hour).UnixMilli()
	initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(ctx, initial); err != nil {
		t.Fatal(err)
	}
	execSQLite(t, path, failFirstEventViewTrigger+`;
CREATE TRIGGER fail_lifecycle_rollback BEFORE UPDATE OF current_view_json ON codexes
WHEN instr(CAST(NEW.current_view_json AS TEXT), 'MANAGEMENT_STATE_MANAGED') > 0
BEGIN SELECT RAISE(ABORT, 'injected rollback failure'); END`)
	_, err := m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"})
	if err == nil || !strings.Contains(err.Error(), "rollback lifecycle state") || !strings.Contains(err.Error(), "injected lifecycle event failure") || !strings.Contains(err.Error(), "injected rollback failure") {
		t.Fatalf("combined rollback error=%v", err)
	}
	m.mu.RLock()
	asyncError := m.asyncError
	m.mu.RUnlock()
	if !strings.Contains(asyncError, "rollback lifecycle state") {
		t.Fatalf("async degradation=%q", asyncError)
	}
}

func TestSetRunningPublishFailureKeepsHonestUpstreamState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	m, _ := testManagerAt(t, path)
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)
	m.Clock = func() time.Time { return base }
	m.LeaseDuration = 2 * time.Hour
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = 0
	initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(ctx, initial); err != nil {
		t.Fatal(err)
	}
	execSQLite(t, path, failFirstEventViewTrigger)
	if err := m.setRunning(ctx, "c", "turn", "request"); err == nil {
		t.Fatal("setRunning publish failure was swallowed")
	}
	got, _ := m.lookup("c")
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := base.Add(2 * time.Hour).UnixMilli()
	if got.Status != remotev1.CodexStatus_CODEX_STATUS_RUNNING || got.ActiveTurnId != "turn" || got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || got.ManagedUntilUnixMs != wantDeadline || record.Status != remotev1.CodexStatus_CODEX_STATUS_RUNNING.String() || record.ManagedUntilUnixMS != wantDeadline {
		t.Fatalf("successful upstream turn was rolled back: codex=%+v record=%+v", got, record)
	}
	m.mu.RLock()
	asyncError := m.asyncError
	m.mu.RUnlock()
	if !strings.Contains(asyncError, "injected lifecycle event failure") {
		t.Fatalf("setRunning degradation=%q", asyncError)
	}
	reset, err := m.Events.Watch(ctx, "c", nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Response.GetResetView().GetCodex().GetStatus() != remotev1.CodexStatus_CODEX_STATUS_RUNNING || reset.Response.GetResetView().GetCodex().GetActiveTurnId() != "turn" {
		t.Fatalf("setRunning failure RESET=%+v", reset.Response)
	}
	reset.Cancel()
}

func TestUnmanagedStartTurnPersistFailureKeepsRunningMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	m, _ := testManagerAt(t, path)
	m.Runtime = fixedAdapterRuntime{adapter: restoreTestAdapter(t)}
	base := time.Unix(1_800_000_000, 0)
	m.Clock = func() time.Time { return base }
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = 0
	initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	execSQLite(t, path, `CREATE TRIGGER fail_start_running_persist BEFORE UPDATE OF current_view_json ON codexes BEGIN SELECT RAISE(ABORT, 'injected running persist failure'); END`)
	if _, err := m.StartTurn(context.Background(), &remotev1.StartTurnRequest{CodexId: "c"}); err == nil || !strings.Contains(err.Error(), "running persist failure") {
		t.Fatalf("StartTurn persist failure=%v", err)
	}
	got, _ := m.lookup("c")
	if got.Status != remotev1.CodexStatus_CODEX_STATUS_RUNNING || got.ActiveTurnId != "turn" || got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || got.ManagedUntilUnixMs != base.Add(2*time.Hour).UnixMilli() {
		t.Fatalf("upstream turn missing from memory=%+v", got)
	}
	m.mu.RLock()
	asyncError := m.asyncError
	m.mu.RUnlock()
	if !strings.Contains(asyncError, "running persist failure") {
		t.Fatalf("persist degradation=%q", asyncError)
	}
	record, _ := m.Persistence.GetCodex(context.Background(), "c")
	if record.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED.String() {
		t.Fatalf("failed persistence unexpectedly changed durable state=%+v", record)
	}
}

func TestManualUnmanageBusyAndAutomaticSafety(t *testing.T) {
	tests := []struct {
		name       string
		view       *remotev1.CurrentView
		manualBusy bool
		autoSafe   bool
	}{
		{name: "nil view"},
		{name: "idle", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_IDLE}}, autoSafe: true},
		{name: "active id", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, ActiveTurnId: "turn"}}, manualBusy: true},
		{name: "active turn", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_IDLE}, ActiveTurn: &remotev1.TurnSnapshot{TurnId: "turn"}}, manualBusy: true},
		{name: "approval pending", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_IDLE}, PendingRequests: []*remotev1.PendingRequest{{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "a"}}}}}, manualBusy: true},
		{name: "user input pending", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_IDLE}, PendingRequests: []*remotev1.PendingRequest{{Request: &remotev1.PendingRequest_UserInput{UserInput: &remotev1.UserInputRequestState{UserInputRequestId: "u"}}}}}, manualBusy: true},
		{name: "running", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_RUNNING}}, manualBusy: true},
		{name: "waiting approval", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL}}, manualBusy: true},
		{name: "waiting input", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_USER_INPUT}}, manualBusy: true},
		{name: "interrupting", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_INTERRUPTING}}, manualBusy: true},
		{name: "error", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_ERROR}}},
		{name: "unavailable", view: &remotev1.CurrentView{Codex: &remotev1.Codex{Status: remotev1.CodexStatus_CODEX_STATUS_UNAVAILABLE}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := manualUnmanageBusy(tt.view); got != tt.manualBusy {
				t.Fatalf("manualUnmanageBusy=%v, want %v", got, tt.manualBusy)
			}
			if got := automaticUnmanageSafe(tt.view); got != tt.autoSafe {
				t.Fatalf("automaticUnmanageSafe=%v, want %v", got, tt.autoSafe)
			}
		})
	}
}

func TestForegroundRenewalAndStartRunningManagementRules(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	m.Clock = func() time.Time { return now }
	m.LeaseDuration = 2 * time.Hour
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON
	m.byID["c"].Codex.ManagedUntilUnixMs = now.Add(time.Minute).UnixMilli()
	m.warningDeadline["c"] = m.byID["c"].Codex.ManagedUntilUnixMs
	m.mu.Unlock()
	if err := m.RenewForegroundCodexes(ctx, []string{"missing", "c", "c"}); err != nil {
		t.Fatal(err)
	}
	got, _ := m.lookup("c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || got.ManagedUntilUnixMs != now.Add(2*time.Hour).UnixMilli() {
		t.Fatalf("foreground renewed codex=%+v", got)
	}
	m.mu.Lock()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED
	m.byID["c"].Codex.ManagedUntilUnixMs = 0
	m.byID["c"].Codex.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
	m.mu.Unlock()
	now = now.Add(time.Hour)
	if err := m.RenewForegroundCodexes(ctx, []string{"c"}); err != nil {
		t.Fatal(err)
	}
	got, _ = m.lookup("c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("foreground restored unmanaged codex=%+v", got)
	}
	if err := m.setRunning(ctx, "c", "turn", "request"); err != nil {
		t.Fatal(err)
	}
	got, _ = m.lookup("c")
	if got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || got.ManagedUntilUnixMs != now.Add(2*time.Hour).UnixMilli() || got.CodexId != "c" {
		t.Fatalf("StartTurn acceptance management=%+v", got)
	}
}

func TestThreadNameUpdatedPersistsAndPublishesCodexTitle(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	event := adapter.Event{Kind: adapter.EventCodexUpdated, Method: "thread/name/updated", ThreadID: "thread", Params: json.RawMessage(`{"threadId":"thread","threadName":"Automatic title"}`)}
	if err := m.applyAdapterEvent(ctx, event); err != nil {
		t.Fatal(err)
	}

	m.mu.RLock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.RUnlock()
	if view.Codex.Title != "Automatic title" {
		t.Fatalf("CurrentView title=%q", view.Codex.Title)
	}
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if record.Title != "Automatic title" {
		t.Fatalf("persisted title=%q", record.Title)
	}
	after := uint64(0)
	w, err := m.Events.Watch(ctx, "c", &after, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if len(w.Replay) != 1 || w.Replay[0].GetCodexUpdated().GetCodex().GetTitle() != "Automatic title" {
		t.Fatalf("CodexUpdated replay=%+v", w.Replay)
	}
}

func TestThreadNameUpdatedDistinguishesNullMissingAndCompatibilityFields(t *testing.T) {
	c := &remotev1.Codex{Title: "original"}
	applyCodexParams(c, "thread/name/updated", json.RawMessage(`{"threadId":"thread"}`))
	if c.Title != "original" {
		t.Fatalf("missing threadName changed title to %q", c.Title)
	}
	applyCodexParams(c, "thread/name/updated", json.RawMessage(`{"threadId":"thread","threadName":""}`))
	if c.Title != "" {
		t.Fatalf("empty threadName did not clear title: %q", c.Title)
	}
	c.Title = "before null"
	applyCodexParams(c, "thread/name/updated", json.RawMessage(`{"threadId":"thread","threadName":null}`))
	if c.Title != "" {
		t.Fatalf("null threadName did not clear title: %q", c.Title)
	}
	applyCodexParams(c, "thread/status/changed", json.RawMessage(`{"thread":{"name":"nested title","status":{"type":"idle"}}}`))
	if c.Title != "nested title" || c.Status != remotev1.CodexStatus_CODEX_STATUS_IDLE {
		t.Fatalf("nested compatibility mapping=%+v", c)
	}
	applyCodexParams(c, "thread/started", json.RawMessage(`{"title":"legacy title"}`))
	if c.Title != "legacy title" {
		t.Fatalf("legacy title mapping=%+v", c)
	}
}

func TestRestoreThreadNameReconciliationPreservesNilAndAppliesEmpty(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	c := proto.Clone(m.byID["c"].Codex).(*remotev1.Codex)
	c.Title = "database title"
	if err := m.persistState(ctx, &remotev1.CurrentView{Codex: c}); err != nil {
		t.Fatal(err)
	}
	reconcileRestoredThreadTitle(c, adapter.Thread{})
	if c.Title != "database title" {
		t.Fatalf("nil app-server name erased title: %q", c.Title)
	}
	if err := m.persistState(ctx, &remotev1.CurrentView{Codex: c}); err != nil {
		t.Fatal(err)
	}
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil || record.Title != "database title" {
		t.Fatalf("nil name persistence title=%q err=%v", record.Title, err)
	}
	empty := ""
	reconcileRestoredThreadTitle(c, adapter.Thread{Name: &empty})
	if c.Title != "" {
		t.Fatalf("explicit empty app-server name not reconciled: %q", c.Title)
	}
	if err := m.persistState(ctx, &remotev1.CurrentView{Codex: c}); err != nil {
		t.Fatal(err)
	}
	record, err = m.Persistence.GetCodex(ctx, "c")
	if err != nil || record.Title != "" {
		t.Fatalf("empty name persistence title=%q err=%v", record.Title, err)
	}
	name := "app-server title"
	reconcileRestoredThreadTitle(c, adapter.Thread{Name: &name})
	if c.Title != name {
		t.Fatalf("app-server name not reconciled: %q", c.Title)
	}
	view := &remotev1.CurrentView{Codex: c}
	if err := m.persistState(ctx, view); err != nil {
		t.Fatal(err)
	}
	record, err = m.Persistence.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if record.Title != name {
		t.Fatalf("reconciled restore title was not persisted: %q", record.Title)
	}
}

func TestThreadNameUpdatedForUnknownThreadDoesNotCrossWrite(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	if err := m.applyAdapterEvent(ctx, adapter.Event{Kind: adapter.EventCodexUpdated, Method: "thread/name/updated", ThreadID: "unknown", Params: json.RawMessage(`{"threadId":"unknown","threadName":"wrong"}`)}); err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	title := m.byID["c"].Codex.Title
	m.mu.RUnlock()
	if title != "" {
		t.Fatalf("unknown thread changed managed title to %q", title)
	}
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if record.Title != "" {
		t.Fatalf("unknown thread persisted title %q", record.Title)
	}
	after := uint64(0)
	w, err := m.Events.Watch(ctx, "c", &after, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if len(w.Replay) != 0 {
		t.Fatalf("unknown thread published events %+v", w.Replay)
	}
}

func TestResolvedPendingTombstoneState(t *testing.T) {
	m := testManager(t)
	m.byID["c"].PendingRequests = []*remotev1.PendingRequest{{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "a", Status: remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING}}}}
	resolved := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "a", Status: remotev1.ApprovalStatus_APPROVAL_STATUS_ALLOWED}}}
	m.resolvePending(context.Background(), "c", "a", resolved, "req")
	kind, raw, err := m.Persistence.ResolvedPending(context.Background(), "c", "a")
	if err != nil || kind != "approval" || len(raw) == 0 {
		t.Fatalf("kind=%q raw=%s err=%v", kind, raw, err)
	}
	if len(m.byID["c"].PendingRequests) != 0 {
		t.Fatalf("pending %+v", m.byID["c"].PendingRequests)
	}
}

func TestPendingCurrentViewHelpers(t *testing.T) {
	view := &remotev1.CurrentView{}
	a := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "a"}}}
	u := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_UserInput{UserInput: &remotev1.UserInputRequestState{UserInputRequestId: "u"}}}
	upsertPending(view, a)
	upsertPending(view, u)
	upsertPending(view, &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "a", Title: "updated"}}})
	if len(view.PendingRequests) != 2 || view.PendingRequests[0].GetApproval().Title != "updated" || pendingIDOf(u) != "u" {
		t.Fatalf("pending %+v", view.PendingRequests)
	}
}

func TestPendingOwnershipAndResolvedAnswers(t *testing.T) {
	pending := adapter.PendingRequest{ID: "input", Method: "item/tool/requestUserInput", ThreadID: "thread", TurnID: "turn", Params: json.RawMessage(`{"startedAtMs":42}`), Questions: []adapter.UserInputQuestion{{ID: "q", Question: "choose", AllowsMultiple: true, Options: []adapter.UserInputOption{{Label: "one"}}}}}
	if !pendingBelongsToCodex(&remotev1.Codex{ThreadId: "thread"}, pending) || pendingBelongsToCodex(&remotev1.Codex{ThreadId: "other"}, pending) {
		t.Fatal("pending ownership was not scoped to codex thread")
	}
	answer := &remotev1.UserInputAnswer{QuestionId: "q", SelectedOptionIds: []string{canonicalOptionID(0, "one")}}
	resolved := resolvedUserInputState(pending, []*remotev1.UserInputAnswer{answer}, 99)
	if !resolved.Resolved || resolved.CreatedAtUnixMs != 42 || resolved.ResolvedAtUnixMs != 99 || len(resolved.Questions) != 1 || !resolved.Questions[0].AllowsMultiple || len(resolved.ResolvedAnswers) != 1 || resolved.ResolvedAnswers[0].QuestionId != "q" {
		t.Fatalf("resolved input %+v", resolved)
	}
}

func TestRestartPendingIsClearedWithHonestCompleteness(t *testing.T) {
	m := testManager(t)
	old := &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c", ThreadId: "thread"}, PendingRequests: []*remotev1.PendingRequest{{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "old"}}}}}
	raw, err := protojson.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Persistence.SetCurrentView(context.Background(), "c", raw); err != nil {
		t.Fatal(err)
	}
	view := &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c", ThreadId: "thread"}}
	m.noteUnrecoverablePending(context.Background(), "c", view)
	if len(view.PendingRequests) != 0 || view.Completeness == nil || !view.Completeness.Incomplete || len(view.Codex.Warnings) != 1 {
		t.Fatalf("restart view %+v", view)
	}
}

func TestRestartPreservesLeaseWarningWithoutRepublishing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_800_000_000, 0)
	deadline := base.Add(2 * time.Hour).UnixMilli()
	persisted := &remotev1.CurrentView{
		Codex: &remotev1.Codex{
			CodexId:              "c",
			ThreadId:             "thread",
			Cwd:                  "/tmp",
			Origin:               remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED,
			Status:               remotev1.CodexStatus_CODEX_STATUS_RUNNING,
			ActiveTurnId:         "lost-turn",
			ManagementState:      remotev1.ManagementState_MANAGEMENT_STATE_MANAGED,
			ManagedUntilUnixMs:   deadline,
			CreatedAtUnixMs:      base.UnixMilli(),
			LastActivityAtUnixMs: base.UnixMilli(),
		},
		ActiveTurn:   &remotev1.TurnSnapshot{TurnId: "lost-turn", Status: remotev1.TurnStatus_TURN_STATUS_RUNNING},
		Completeness: &remotev1.Completeness{Incomplete: true, Reason: "persisted diagnostic"},
	}
	raw, err := protojson.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	record := recordFromCodex(persisted.Codex, "cli")
	record.CurrentViewJSON = raw
	if err := first.UpsertCodex(ctx, record); err != nil {
		t.Fatal(err)
	}
	beforeRestart := &Manager{
		Persistence:        first,
		Events:             activity.NewStore(first, nil, 16),
		Clock:              func() time.Time { return base.Add(90 * time.Minute) },
		LeaseWarningBefore: 30 * time.Minute,
	}
	beforeRestart.registerRestored(record, persisted)
	if err := beforeRestart.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	head, err := first.EventHead(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if head != 2 {
		t.Fatalf("initial warning events head=%d, want 2", head)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	record, err = reopened.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{Persistence: reopened, Clock: func() time.Time { return base.Add(90 * time.Minute) }}
	restoredCodex := codexFromRecord(record)
	restoredCodex.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
	restoredCodex.ActiveTurnId = ""
	restored := &remotev1.CurrentView{Codex: restoredCodex, GeneratedAtUnixMs: m.now().UnixMilli()}
	m.noteUnrecoverablePending(ctx, "c", restored)
	m.registerRestored(record, restored)

	if restored.ActiveTurn != nil || len(restored.PendingRequests) != 0 || restored.Codex.ActiveTurnId != "" {
		t.Fatalf("runtime state was unexpectedly restored: %+v", restored)
	}
	if len(restored.Codex.Warnings) != 1 || restored.Codex.Warnings[0].Code != remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON || restored.Codex.Warnings[0].ManagedUntilUnixMs != deadline {
		t.Fatalf("restored warnings=%+v", restored.Codex.Warnings)
	}
	if restored.Completeness == nil || !restored.Completeness.Incomplete || restored.Completeness.Reason != "persisted diagnostic" {
		t.Fatalf("restored completeness=%+v", restored.Completeness)
	}
	if err := m.sweepLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m.lookup("c")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Warnings) != 1 {
		t.Fatalf("warning was republished after restart: %+v", got.Warnings)
	}
	head, err = reopened.EventHead(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if head != 2 {
		t.Fatalf("restart republished lifecycle events: head=%d", head)
	}
}

func TestRestoreSerializesConcurrentLifecycleCommits(t *testing.T) {
	ad := restoreTestAdapter(t)
	base := time.Unix(1_800_000_000, 0)
	tests := []struct {
		name      string
		operation func(context.Context, *Manager) error
		check     func(*testing.T, *remotev1.Codex, persistence.CodexRecord)
	}{
		{
			name: "manual unmanage",
			operation: func(ctx context.Context, m *Manager) error {
				_, err := m.UnmanageCodex(ctx, &remotev1.UnmanageCodexRequest{CodexId: "c"})
				return err
			},
			check: func(t *testing.T, codex *remotev1.Codex, record persistence.CodexRecord) {
				t.Helper()
				if codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || record.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED.String() {
					t.Fatalf("unmanage was overwritten: codex=%+v record=%+v", codex, record)
				}
			},
		},
		{
			name: "foreground renewal",
			operation: func(ctx context.Context, m *Manager) error {
				return m.RenewForegroundCodexes(ctx, []string{"c"})
			},
			check: func(t *testing.T, codex *remotev1.Codex, record persistence.CodexRecord) {
				t.Helper()
				want := base.Add(2 * time.Hour).UnixMilli()
				if codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || codex.ManagedUntilUnixMs != want || record.ManagedUntilUnixMS != want {
					t.Fatalf("renewal was overwritten: codex=%+v record=%+v", codex, record)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testManager(t)
			m.Runtime = fixedAdapterRuntime{adapter: ad}
			m.Clock = func() time.Time { return base }
			m.LeaseDuration = 2 * time.Hour
			m.mu.Lock()
			m.ensureMapsLocked()
			m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
			m.byID["c"].Codex.ManagedUntilUnixMs = base.Add(time.Hour).UnixMilli()
			initial := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
			m.mu.Unlock()
			if err := m.persistState(context.Background(), initial); err != nil {
				t.Fatal(err)
			}

			restorePaused := make(chan struct{})
			restoreContinue := make(chan struct{})
			m.testBeforeCommit = func(operation string) {
				if operation == "restore" {
					close(restorePaused)
					<-restoreContinue
				}
			}
			restoreDone := make(chan error, 1)
			go func() { restoreDone <- m.Restore(context.Background()) }()
			<-restorePaused
			if m.commitMu.TryLock() {
				m.commitMu.Unlock()
				close(restoreContinue)
				<-restoreDone
				t.Fatal("Restore did not hold the state commit boundary while paused")
			}
			operationDone := make(chan error, 1)
			go func() { operationDone <- tt.operation(context.Background(), m) }()
			close(restoreContinue)
			if err := <-restoreDone; err != nil {
				t.Fatal(err)
			}
			if err := <-operationDone; err != nil {
				t.Fatal(err)
			}
			codex, err := m.lookup("c")
			if err != nil {
				t.Fatal(err)
			}
			record, err := m.Persistence.GetCodex(context.Background(), "c")
			if err != nil {
				t.Fatal(err)
			}
			tt.check(t, codex, record)
		})
	}
}

func TestLargeContentIsExplicitlyBounded(t *testing.T) {
	m := &Manager{ContentBudget: 16}
	params := []byte(`{"item":{"id":"i","type":"agentMessage","text":"abcdefghijklmnopqrstuvwxyz"}}`)
	item := translateItem(params, "t", "i", "item/completed", adapter.SemanticAgentText, remotev1.ItemStatus_ITEM_STATUS_COMPLETED, 16, remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE)
	if len(item.GetAgentMessage().Text) != 16 || item.Completeness == nil || !item.Completeness.Truncated || item.Completeness.OriginalSizeBytes != 26 {
		t.Fatalf("item %+v", item)
	}
	turn := &remotev1.TurnSnapshot{TurnId: "t"}
	sent := m.appendItemText(turn, "i", "abcdefghijklmnopqrstuvwxyz")
	if len(sent) != 16 || turn.Items[0].Completeness == nil {
		t.Fatalf("sent=%q turn=%+v", sent, turn)
	}
	raw := []byte(`{"type":"agentMessage","text":"abcdefghijklmnopqrstuvwxyz"}`)
	snapshot := m.turnSnapshot(adapter.Turn{ID: "t", Status: "completed", Items: []json.RawMessage{raw}})
	if snapshot.Completeness == nil || !snapshot.Completeness.Incomplete || len(snapshot.Items) != 0 {
		t.Fatalf("snapshot %+v", snapshot)
	}
}

func TestCollectionBudgetTrimsManyItemsHonestly(t *testing.T) {
	m := &Manager{ContentBudget: 4096}
	turn := &remotev1.TurnSnapshot{TurnId: "turn", Status: remotev1.TurnStatus_TURN_STATUS_RUNNING}
	for i := 0; i < 12; i++ {
		turn.Items = append(turn.Items, &remotev1.Item{ItemId: string(rune('a' + i)), TurnId: "turn", Status: remotev1.ItemStatus_ITEM_STATUS_COMPLETED, Content: &remotev1.Item_AgentMessage{AgentMessage: &remotev1.AgentMessageItem{Text: strings.Repeat("abcdefghijklmnopqrstuvwxyz", 40)}}})
	}
	view := &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c", ThreadId: "thread"}, ActiveTurn: turn}
	original := len(turn.Items)
	m.boundCurrentView(view)
	if view.ActiveTurn == nil || len(view.ActiveTurn.Items) != original || view.Completeness == nil || !view.Completeness.Truncated || protoJSONSize(view) > m.collectionBudget() {
		t.Fatalf("bounded view size=%d view=%+v", protoJSONSize(view), view)
	}
	history := &remotev1.HistoryPage{CodexId: "c", HistoryComplete: true}
	for i := 0; i < 10; i++ {
		history.Turns = append(history.Turns, proto.Clone(turn).(*remotev1.TurnSnapshot))
	}
	m.boundHistoryPage(history)
	if history.HistoryComplete || history.Completeness == nil || protoJSONSize(history) > m.collectionBudget() {
		t.Fatalf("bounded history size=%d history=%+v", protoJSONSize(history), history)
	}
}

func TestSyntheticPlanAndDiffIDsAreStablePerTurn(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	for _, event := range []adapter.Event{
		{Kind: adapter.EventItemUpdated, Semantic: adapter.SemanticPlanUpdated, Method: "turn/plan/updated", ThreadID: "thread", TurnID: "turn", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","plan":[{"step":"first","status":"inProgress"}]}`)},
		{Kind: adapter.EventItemUpdated, Semantic: adapter.SemanticPlanUpdated, Method: "turn/plan/updated", ThreadID: "thread", TurnID: "turn", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","plan":[{"step":"second","status":"completed"}]}`)},
		{Kind: adapter.EventItemUpdated, Semantic: adapter.SemanticDiffUpdated, Method: "turn/diff/updated", ThreadID: "thread", TurnID: "turn", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","diff":"@@ changed"}`)},
	} {
		if err := m.applyAdapterEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	m.mu.RLock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.RUnlock()
	if len(view.ActiveTurn.Items) != 2 {
		t.Fatalf("items=%+v", view.ActiveTurn.Items)
	}
	items := map[string]*remotev1.Item{}
	for _, item := range view.ActiveTurn.Items {
		items[item.ItemId] = item
	}
	if items["turn:plan"] == nil || items["turn:plan"].GetPlan().Steps[0].Text != "second" || items["turn:diff"] == nil || items["turn:diff"].GetFileChange().UnifiedDiff != "@@ changed" {
		t.Fatalf("stable synthetic items=%+v", items)
	}
}

func TestSetRunningSerializesConcurrentEventAndPreservesState(t *testing.T) {
	m := testManager(t)
	pending := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "approval", TurnId: "turn", Status: remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING}}}
	m.mu.Lock()
	m.byID["c"].PendingRequests = []*remotev1.PendingRequest{pending}
	m.byID["c"].Codex.Status = remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL
	m.mu.Unlock()

	eventAtCommit := make(chan struct{})
	releaseEvent := make(chan struct{})
	var once sync.Once
	m.testBeforeCommit = func(operation string) {
		if operation == "adapter_event" {
			once.Do(func() { close(eventAtCommit) })
			<-releaseEvent
		}
	}
	eventDone := make(chan error, 1)
	go func() {
		eventDone <- m.applyAdapterEvent(context.Background(), adapter.Event{Kind: adapter.EventItemUpdated, Semantic: adapter.SemanticPlanUpdated, Method: "turn/plan/updated", ThreadID: "thread", TurnID: "turn", Params: json.RawMessage(`{"plan":[{"step":"arrived-before-response","status":"inProgress"}]}`)})
	}()
	<-eventAtCommit
	setStarted := make(chan struct{})
	setDone := make(chan error, 1)
	go func() {
		close(setStarted)
		setDone <- m.setRunning(context.Background(), "c", "turn", "request")
	}()
	<-setStarted
	close(releaseEvent)
	if err := <-eventDone; err != nil {
		t.Fatal(err)
	}
	if err := <-setDone; err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.RUnlock()
	if view.ActiveTurn == nil || findItem(view.ActiveTurn, "turn:plan") == nil || len(view.PendingRequests) != 1 || view.Codex.Status != remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL {
		t.Fatalf("concurrent state was overwritten: %+v", view)
	}
	raw, err := m.Persistence.CurrentView(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	persisted := new(remotev1.CurrentView)
	if err := protojson.Unmarshal(raw, persisted); err != nil || findItem(persisted.ActiveTurn, "turn:plan") == nil || len(persisted.PendingRequests) != 1 {
		t.Fatalf("persisted concurrent state=%+v err=%v", persisted, err)
	}
}

func TestEventAndResetBudgetPreserveActionablePending(t *testing.T) {
	m := &Manager{ContentBudget: 1400}
	large := strings.Repeat("long presentation ", 400)
	questions := make([]*remotev1.UserInputQuestion, 0, 8)
	for i := 0; i < 8; i++ {
		questions = append(questions, &remotev1.UserInputQuestion{QuestionId: "question-" + string(rune('a'+i)), Header: large, Prompt: large, Options: []*remotev1.UserInputOption{{OptionId: "option-a", Label: large, Description: large}, {OptionId: "option-b", Label: large, Description: large}}})
	}
	input := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_UserInput{UserInput: &remotev1.UserInputRequestState{UserInputRequestId: "input-id", TurnId: "turn", ItemId: "tool", Questions: questions}}}
	approval := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "approval-id", TurnId: "turn", ItemId: "command", Status: remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING, Explanation: large, Command: []string{large, large}, AllowedDecisions: []remotev1.ApprovalDecision{remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW, remotev1.ApprovalDecision_APPROVAL_DECISION_DENY}}}}
	for _, pending := range []*remotev1.PendingRequest{input, approval} {
		event := &remotev1.Event{CodexId: "c", Event: &remotev1.Event_PendingRequestUpdated{PendingRequestUpdated: &remotev1.PendingRequestUpdated{Request: pending}}}
		if complete := m.boundCanonicalEvent(event); complete == nil || protoJSONSize(event) > m.eventPayloadBudget() {
			t.Fatalf("pending event size=%d complete=%+v", protoJSONSize(event), complete)
		}
		if event.Completeness == nil || !event.Completeness.Incomplete {
			t.Fatalf("live pending event lacks completeness: %+v", event)
		}
	}
	if input.GetUserInput().UserInputRequestId != "input-id" || len(input.GetUserInput().Questions) != 8 || len(input.GetUserInput().Questions[0].Options) != 2 || input.GetUserInput().Questions[0].QuestionId == "" || input.GetUserInput().Questions[0].Options[0].OptionId == "" || input.GetUserInput().Questions[0].Options[0].Label == "" || input.GetUserInput().Completeness == nil {
		t.Fatalf("user input lost actionable identity: %+v", input)
	}
	if approval.GetApproval().ApprovalId != "approval-id" || len(approval.GetApproval().AllowedDecisions) != 2 {
		t.Fatalf("approval lost actionable identity: %+v", approval)
	}
	view := &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c", ThreadId: "thread", Warnings: []*remotev1.Warning{{Message: large}}}, ActiveTurn: &remotev1.TurnSnapshot{TurnId: "turn", Items: []*remotev1.Item{{ItemId: "huge", TurnId: "turn", Content: &remotev1.Item_AgentMessage{AgentMessage: &remotev1.AgentMessageItem{Text: large}}}}}, PendingRequests: []*remotev1.PendingRequest{input, approval}}
	m.boundCurrentView(view)
	if protoJSONSize(view) > m.collectionBudget() || len(view.PendingRequests) != 2 || pendingIDOf(view.PendingRequests[0]) != "input-id" || pendingIDOf(view.PendingRequests[1]) != "approval-id" || view.Completeness == nil {
		t.Fatalf("bounded RESET size=%d view=%+v", protoJSONSize(view), view)
	}
}

func TestLargeStructuredItemsAndWarningEventsAreBounded(t *testing.T) {
	m := &Manager{ContentBudget: 1024}
	large := strings.Repeat("x", 4096)
	tests := []adapter.Event{
		{Semantic: adapter.SemanticPlanUpdated, Method: "turn/plan/updated", TurnID: "turn", Params: json.RawMessage(`{"plan":[{"step":"` + large + `"},{"step":"` + large + `"}]}`)},
		{Semantic: adapter.SemanticDiffUpdated, Method: "turn/diff/updated", TurnID: "turn", Params: json.RawMessage(`{"changes":[{"path":"` + large + `","kind":"modified"},{"path":"` + large + `","kind":"added"}],"diff":"` + large + `"}`)},
	}
	for _, source := range tests {
		item := m.canonicalItem(source, remotev1.ItemStatus_ITEM_STATUS_RUNNING)
		event := &remotev1.Event{CodexId: "c", Event: &remotev1.Event_ItemUpdated{ItemUpdated: &remotev1.ItemUpdated{Item: item}}}
		m.boundCanonicalEvent(event)
		if protoJSONSize(event) > m.eventPayloadBudget() || item.Completeness == nil || event.Completeness == nil {
			t.Fatalf("item event size=%d item=%+v", protoJSONSize(event), item)
		}
	}
	warning := &remotev1.Event{CodexId: "c", Event: &remotev1.Event_WarningRaised{WarningRaised: &remotev1.WarningRaised{Warning: &remotev1.Warning{Message: large, Metadata: map[string]string{"raw": large, "other": large}}}}}
	if complete := m.boundCanonicalEvent(warning); complete == nil || protoJSONSize(warning) > m.eventPayloadBudget() {
		t.Fatalf("warning event size=%d complete=%+v", protoJSONSize(warning), complete)
	}
}

func TestLargeApprovalBoundingIsSubquadraticAndDoesNotBlockNextPending(t *testing.T) {
	m := &Manager{ContentBudget: 32 << 10}
	command := make([]string, 6000)
	for i := range command {
		command[i] = "approval-argument-abcdefghijklmnopqrstuvwxyz"
	}
	large := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "large", Explanation: strings.Repeat("explanation ", 12000), Command: command, Status: remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING}}}
	small := &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: &remotev1.Approval{ApprovalId: "small", Explanation: "next", Command: []string{"true"}, Status: remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING}}}
	largeLocked := make(chan struct{})
	largeDone := make(chan struct{})
	smallDone := make(chan struct{})
	started := time.Now()
	go func() {
		m.commitMu.Lock()
		close(largeLocked)
		m.boundPendingRequest(large, m.eventPayloadBudget())
		m.commitMu.Unlock()
		close(largeDone)
	}()
	<-largeLocked
	go func() {
		m.commitMu.Lock()
		m.boundPendingRequest(small, m.eventPayloadBudget())
		m.commitMu.Unlock()
		close(smallDone)
	}()
	select {
	case <-largeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("6000-argv approval bounding exceeded 2 seconds")
	}
	select {
	case <-smallDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second pending remained blocked after large approval bounding")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("large approval bounding took %s", elapsed)
	}
	if len(large.GetApproval().Command) == 0 || large.GetApproval().ApprovalId != "large" {
		t.Fatalf("large approval lost actionable identity: %+v", large)
	}
}

func TestRenameCodexAllowsBusyAndAllManagementStatesAndProtectsManualTitle(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	states := []remotev1.ManagementState{
		remotev1.ManagementState_MANAGEMENT_STATE_MANAGED,
		remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON,
		remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED,
	}
	for i, state := range states {
		m.mu.Lock()
		m.byID["c"].Codex.ManagementState = state
		m.byID["c"].Codex.Status = remotev1.CodexStatus_CODEX_STATUS_RUNNING
		m.byID["c"].Codex.ActiveTurnId = "turn"
		m.mu.Unlock()
		want := fmt.Sprintf("manual %d", i)
		response, err := m.RenameCodex(ctx, &remotev1.RenameCodexRequest{CodexId: "c", Title: "  " + want + "  "})
		if err != nil || response.GetCodex().GetTitle() != want {
			t.Fatalf("state=%v response=%+v err=%v", state, response, err)
		}
	}
	record, err := m.Persistence.GetCodex(ctx, "c")
	if err != nil || !record.ManualTitleOverride || record.Title != "manual 2" {
		t.Fatalf("persisted record=%+v err=%v", record, err)
	}
	if err = m.applyAdapterEvent(ctx, adapter.Event{Kind: adapter.EventCodexUpdated, Method: "thread/name/updated", ThreadID: "thread", Params: json.RawMessage(`{"threadId":"thread","threadName":"automatic"}`)}); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.lookup("c"); got.Title != "manual 2" {
		t.Fatalf("automatic event replaced manual title: %q", got.Title)
	}
	m.mu.Lock()
	m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	m.mu.Unlock()
	if err = m.persistState(ctx, m.byID["c"]); err != nil {
		t.Fatal(err)
	}
	m.Runtime = fixedAdapterRuntime{adapter: restoreTestAdapterWithName(t, "automatic after restore")}
	if err = m.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.lookup("c"); got.Title != "manual 2" {
		t.Fatalf("restore replaced manual title: %q", got.Title)
	}
	if _, err = m.RenameCodex(ctx, &remotev1.RenameCodexRequest{CodexId: "c", Title: "  "}); err == nil {
		t.Fatal("whitespace-only title succeeded")
	}
}

func TestForgetCodexRequiresUnmanagedAndTerminatesWatch(t *testing.T) {
	t.Run("managed conflict", func(t *testing.T) {
		m := testManager(t)
		m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
		_, err := m.ForgetCodex(context.Background(), &remotev1.ForgetCodexRequest{CodexId: "c"})
		var rpc *gateway.RPCError
		if !errors.As(err, &rpc) || rpc.Detail.GetCode() != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
			t.Fatalf("ForgetCodex error=%v", err)
		}
		if _, err = m.Persistence.GetCodex(context.Background(), "c"); err != nil {
			t.Fatalf("managed record was removed: %v", err)
		}
	})

	t.Run("unmanaged terminal removal", func(t *testing.T) {
		m := testManager(t)
		ctx := context.Background()
		m.byID["c"].Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED
		m.byID["c"].Codex.ManagedUntilUnixMs = 0
		if err := m.persistState(ctx, m.byID["c"]); err != nil {
			t.Fatal(err)
		}
		if err := m.ensureWorkspace("c", "/tmp", m.byID["c"]); err != nil {
			t.Fatal(err)
		}
		if err := m.Persistence.SaveResolvedPending(ctx, "c", "pending", "approval", []byte(`{"done":true}`)); err != nil {
			t.Fatal(err)
		}
		watch, err := m.Events.Watch(ctx, "c", nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		response, err := m.ForgetCodex(ctx, &remotev1.ForgetCodexRequest{CodexId: "c"})
		if err != nil || response.GetCodexId() != "c" {
			t.Fatalf("response=%+v err=%v", response, err)
		}
		event, ok := <-watch.Events
		if !ok || event.GetCodexForgotten() == nil {
			t.Fatalf("terminal event=%+v open=%v", event, ok)
		}
		if _, ok = <-watch.Events; ok {
			t.Fatal("watch remained open")
		}
		if _, err = m.lookup("c"); err == nil {
			t.Fatal("forgotten codex remained in manager")
		}
		if _, err = m.Persistence.GetCodex(ctx, "c"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("persisted codex err=%v", err)
		}
		if _, _, err = m.Persistence.ResolvedPending(ctx, "c", "pending"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("resolved pending err=%v", err)
		}
		if candidate, candidateErr := m.Persistence.GetForgottenSession(ctx, "unknown", "thread"); candidateErr != nil || candidate.Materialized || candidate.CWD != "/tmp" {
			t.Fatalf("forgotten candidate=%+v err=%v", candidate, candidateErr)
		}
		if _, _, err = m.Workspaces.State("c"); err == nil {
			t.Fatal("workspace registry remained")
		}
		if _, err = m.Events.Watch(ctx, "c", nil, 1); !errors.Is(err, activity.ErrCodexNotFound) {
			t.Fatalf("Watch after forget err=%v", err)
		}
	})
}

func TestForgottenUnmaterializedSessionCanBeListedImportedAndStarted(t *testing.T) {
	m := testManager(t)
	m.Runtime = fixedAdapterRuntime{adapter: importTestAdapter(t, "/tmp")}
	ctx := context.Background()
	forgotten := persistence.ForgottenSessionRecord{
		Source: "appServer", SessionID: "empty-thread", CWD: "/tmp", Title: "empty candidate",
		Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED.String(), CreatedAtUnixMS: 10, UpdatedAtUnixMS: 20,
	}
	if err := m.Persistence.UpsertForgottenSession(ctx, forgotten); err != nil {
		t.Fatal(err)
	}
	listed, err := m.ListSessionCandidates(ctx, &remotev1.ListSessionCandidatesRequest{Cwd: "/tmp", Page: &remotev1.PageRequest{PageSize: 20}})
	if err != nil {
		t.Fatal(err)
	}
	var candidate *remotev1.SessionCandidate
	for _, value := range listed.Sessions {
		if value.SessionId == forgotten.SessionID && value.Source == forgotten.Source {
			candidate = value
		}
	}
	if candidate == nil || candidate.Availability != remotev1.SessionAvailability_SESSION_AVAILABILITY_RESUMABLE || candidate.ManagedCodexId != "" {
		t.Fatalf("candidate=%+v listed=%+v", candidate, listed)
	}
	imported, err := m.ImportSession(ctx, &remotev1.ImportSessionRequest{SessionId: forgotten.SessionID, Source: forgotten.Source})
	if err != nil {
		t.Fatal(err)
	}
	if imported.Codex == nil || imported.Codex.CodexId == "c" || imported.Codex.ThreadId != forgotten.SessionID || imported.Codex.Origin != remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED {
		t.Fatalf("imported=%+v", imported)
	}
	if _, err = m.Persistence.GetForgottenSession(ctx, forgotten.Source, forgotten.SessionID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("forgotten candidate was not consumed: %v", err)
	}
	history, err := m.ListHistory(ctx, &remotev1.ListHistoryRequest{CodexId: imported.Codex.CodexId})
	if err != nil || history.History == nil || len(history.History.Turns) != 0 || !history.History.HistoryComplete {
		t.Fatalf("unmaterialized history=%+v err=%v", history, err)
	}
	started, err := m.StartTurn(ctx, &remotev1.StartTurnRequest{CodexId: imported.Codex.CodexId, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "materialize"}}}}})
	if err != nil || started.TurnId != "turn" {
		t.Fatalf("StartTurn=%+v err=%v", started, err)
	}
}

func TestForgottenMaterializedSessionReimportsWithHistoryAndNewCodexID(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	cwd := t.TempDir()
	m.Runtime = fixedAdapterRuntime{adapter: importTestAdapter(t, cwd)}
	old := &remotev1.Codex{
		CodexId: "old-codex", ThreadId: "historical-thread", Cwd: cwd, Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED,
		Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, ManagementState: remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED,
		CreatedAtUnixMs: 1, LastActivityAtUnixMs: 2,
	}
	if err := m.saveCodex(ctx, old, "exec"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ForgetCodex(ctx, &remotev1.ForgetCodexRequest{CodexId: old.CodexId}); err != nil {
		t.Fatal(err)
	}
	if candidate, err := m.Persistence.GetForgottenSession(ctx, "exec", old.ThreadId); err != nil || !candidate.Materialized {
		t.Fatalf("forgotten candidate=%+v err=%v", candidate, err)
	}
	imported, err := m.ImportSession(ctx, &remotev1.ImportSessionRequest{SessionId: old.ThreadId, Source: "exec"})
	if err != nil {
		t.Fatal(err)
	}
	if imported.Codex == nil || imported.Codex.CodexId == old.CodexId || imported.Codex.ThreadId != old.ThreadId {
		t.Fatalf("imported=%+v", imported)
	}
	history, err := m.ListHistory(ctx, &remotev1.ListHistoryRequest{CodexId: imported.Codex.CodexId})
	if err != nil || history.History == nil || len(history.History.Turns) != 1 || history.History.Turns[0].TurnId != "old-turn" {
		t.Fatalf("history=%+v err=%v", history, err)
	}
}
