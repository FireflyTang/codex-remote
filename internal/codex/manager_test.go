package codex

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/gateway"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func testManager(t *testing.T) *Manager {
	t.Helper()
	p, err := persistence.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	c := &remotev1.Codex{CodexId: "c", ThreadId: "thread", Cwd: "/tmp", Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, CreatedAtUnixMs: 1, LastActivityAtUnixMs: 1}
	m := &Manager{Persistence: p, Events: activity.NewStore(p, nil, 16), byID: map[string]*remotev1.CurrentView{"c": {Codex: c}}, byThread: map[string]string{"thread": "c"}, chunks: make(map[string]uint64)}
	if err = m.persistState(context.Background(), m.byID["c"]); err != nil {
		t.Fatal(err)
	}
	return m
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
