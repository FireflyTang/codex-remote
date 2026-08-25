package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/persistence"
	workspacecore "github.com/kylin1993/codex-remote/internal/workspace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func collabToolEvent(threadID, turnID, itemID, tool string, states map[string]string, receivers ...string) adapter.Event {
	agents := make(map[string]map[string]string, len(states))
	for id, status := range states {
		agents[id] = map[string]string{"status": status}
	}
	params, _ := json.Marshal(map[string]any{"threadId": threadID, "turnId": turnID, "item": map[string]any{"type": "collabAgentToolCall", "id": itemID, "senderThreadId": threadID, "receiverThreadIds": receivers, "agentsStates": agents, "status": "inProgress", "tool": tool}})
	return adapter.Event{Kind: adapter.EventItemUpdated, Method: "turn/plan/updated", ThreadID: threadID, TurnID: turnID, ItemID: itemID, Params: params}
}

func collabEvent(threadID, turnID, itemID string, states map[string]string, receivers ...string) adapter.Event {
	return collabToolEvent(threadID, turnID, itemID, "spawnAgent", states, receivers...)
}

func workspaceManager(t *testing.T) (*Manager, string) {
	t.Helper()
	m := testManager(t)
	root := t.TempDir()
	service, err := workspacecore.New(workspacecore.Config{MaxTextFileBytes: 1024, MaxInlineUploadBytes: 2048, MaxInlineDownloadBytes: 2048, MaxArchiveExpandedBytes: 4096, MaxArchiveEntryCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	m.SetWorkspaceService(service)
	m.mu.Lock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	view.Codex.Cwd = root
	m.mu.Unlock()
	if err := m.ensureWorkspace("c", root, view); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.byID["c"] = view
	m.mu.Unlock()
	if err := m.persistState(context.Background(), view); err != nil {
		t.Fatal(err)
	}
	return m, root
}

func TestWorkspaceMutationStatePersistsAndPublishes(t *testing.T) {
	m, _ := workspaceManager(t)
	ctx := context.Background()
	workspace, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := m.WriteWorkspaceTextFile(ctx, &remotev1.WriteWorkspaceTextFileRequest{CodexId: "c", RelativePath: "note.txt", Utf8Text: "hello", Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, ExpectedQuiescenceToken: workspace.AccessState.QuiescenceToken})
	if err != nil {
		t.Fatal(err)
	}
	if response.Entry.Revision == "" {
		t.Fatalf("response=%+v", response)
	}
	m.mu.RLock()
	memory := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	m.mu.RUnlock()
	if memory.WorkspaceAccessState.Generation != 2 || memory.WorkspaceAccessState.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED {
		t.Fatalf("memory=%+v", memory.WorkspaceAccessState)
	}
	raw, err := m.Persistence.CurrentView(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	persisted := new(remotev1.CurrentView)
	if err := protojson.Unmarshal(raw, persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.WorkspaceAccessState.Generation != memory.WorkspaceAccessState.Generation {
		t.Fatalf("persisted=%+v memory=%+v", persisted.WorkspaceAccessState, memory.WorkspaceAccessState)
	}
	after := uint64(0)
	watch, err := m.Events.Watch(ctx, "c", &after, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Cancel()
	if len(watch.Replay) != 1 || watch.Replay[0].GetWorkspaceAccessStateUpdated().GetAccessState().GetGeneration() != 2 {
		t.Fatalf("replay=%+v", watch.Replay)
	}
}

func TestWorkspaceStateSinkIgnoresGenerationOlderThanRestoredView(t *testing.T) {
	m, _ := workspaceManager(t)
	m.mu.Lock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	view.WorkspaceAccessState.Generation = 10
	m.byID["c"] = view
	m.mu.Unlock()

	stale := proto.Clone(view.WorkspaceAccessState).(*remotev1.WorkspaceAccessState)
	stale.Generation = 9
	stale.ActiveAgentCount = 1
	stale.MutationStatus = remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY
	stale.QuiescenceToken = ""
	if err := m.commitWorkspaceState(context.Background(), "c", stale); err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	got := proto.Clone(m.byID["c"].WorkspaceAccessState).(*remotev1.WorkspaceAccessState)
	m.mu.RUnlock()
	if got.Generation != 10 || got.ActiveAgentCount != 0 {
		t.Fatalf("stale sink replaced restored state: %+v", got)
	}
}

func TestWorkspaceTransitionAdoptedDurablyByRestoreDoesNotRollBack(t *testing.T) {
	m, _ := workspaceManager(t)
	transition := make(chan *remotev1.WorkspaceAccessState, 1)
	releaseSink := make(chan struct{})
	m.Workspaces.SetStateSink(func(ctx context.Context, codexID string, state *remotev1.WorkspaceAccessState) error {
		transition <- proto.Clone(state).(*remotev1.WorkspaceAccessState)
		<-releaseSink
		return m.commitWorkspaceState(ctx, codexID, state)
	})
	ctx, cancel := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() { agentDone <- m.Workspaces.AgentStarted(ctx, "c", "turn-during-restore") }()
	adopted := <-transition

	// Model Restore adopting and durably persisting the exact pending live
	// transition before the original sink resumes with a canceled context.
	m.commitMu.Lock()
	m.mu.Lock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	view.WorkspaceAccessState = proto.Clone(adopted).(*remotev1.WorkspaceAccessState)
	m.byID["c"] = view
	m.mu.Unlock()
	if err := m.persistState(context.Background(), view); err != nil {
		m.commitMu.Unlock()
		t.Fatal(err)
	}
	m.commitMu.Unlock()
	cancel()
	close(releaseSink)
	if err := <-agentDone; err != nil {
		t.Fatalf("durably adopted transition rolled back: %v", err)
	}

	serviceState, _, err := m.Workspaces.State("c")
	if err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	memoryState := proto.Clone(m.byID["c"].WorkspaceAccessState).(*remotev1.WorkspaceAccessState)
	m.mu.RUnlock()
	raw, err := m.Persistence.CurrentView(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	durable := new(remotev1.CurrentView)
	if err := protojson.Unmarshal(raw, durable); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(serviceState, adopted) || !proto.Equal(memoryState, adopted) || !proto.Equal(durable.WorkspaceAccessState, adopted) {
		t.Fatalf("state split after adoption: service=%+v memory=%+v durable=%+v adopted=%+v", serviceState, memoryState, durable.WorkspaceAccessState, adopted)
	}
}

func TestWorkspaceTransitionMatchingOnlyMemoryStillPublishes(t *testing.T) {
	m, _ := workspaceManager(t)
	m.mu.Lock()
	view := proto.Clone(m.byID["c"]).(*remotev1.CurrentView)
	view.WorkspaceAccessState.Generation++
	m.byID["c"] = view
	m.mu.Unlock()
	called := false
	m.testPublishEvent = func(context.Context, *remotev1.Event, *remotev1.CurrentView, *remotev1.Provenance, string) (*remotev1.Event, error) {
		called = true
		return nil, context.Canceled
	}
	if err := m.commitWorkspaceState(context.Background(), "c", view.WorkspaceAccessState); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit error=%v", err)
	}
	if !called {
		t.Fatal("matching in-memory state bypassed publish without durable adoption")
	}
}

func TestWorkspaceMutationDeterministicPublishFailureRollsBackAndRetries(t *testing.T) {
	m, root := workspaceManager(t)
	ctx := context.Background()
	initial, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected workspace publish failure")
	m.testPublishEvent = func(context.Context, *remotev1.Event, *remotev1.CurrentView, *remotev1.Provenance, string) (*remotev1.Event, error) {
		return nil, injected
	}
	req := &remotev1.WriteWorkspaceTextFileRequest{CodexId: "c", RelativePath: "transactional.txt", Utf8Text: "once", Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, ExpectedQuiescenceToken: initial.AccessState.QuiescenceToken}
	if _, err := m.WriteWorkspaceTextFile(ctx, req); !errors.Is(err, injected) {
		t.Fatalf("write error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "transactional.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed write remained: %v", err)
	}
	afterFailure, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if afterFailure.AccessState.Generation != initial.AccessState.Generation || afterFailure.AccessState.QuiescenceToken != initial.AccessState.QuiescenceToken {
		t.Fatalf("failed write advanced state: initial=%+v after=%+v", initial.AccessState, afterFailure.AccessState)
	}
	m.testPublishEvent = nil
	if _, err := m.WriteWorkspaceTextFile(ctx, req); err != nil {
		t.Fatalf("retry=%v", err)
	}
	if content, err := os.ReadFile(filepath.Join(root, "transactional.txt")); err != nil || string(content) != "once" {
		t.Fatalf("retry content=%q err=%v", content, err)
	}
}

func TestWorkspaceMutationUnknownButDurableNewStateCommitsOnce(t *testing.T) {
	m, root := workspaceManager(t)
	ctx := context.Background()
	initial, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	m.testPublishEvent = func(ctx context.Context, event *remotev1.Event, view *remotev1.CurrentView, provenance *remotev1.Provenance, parentRecordID string) (*remotev1.Event, error) {
		if _, err := m.Events.Publish(ctx, event, view, provenance, parentRecordID); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: injected after durable commit", persistence.ErrEventCommitOutcomeUnknown)
	}
	response, err := m.WriteWorkspaceTextFile(ctx, &remotev1.WriteWorkspaceTextFileRequest{CodexId: "c", RelativePath: "unknown.txt", Utf8Text: "committed", Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, ExpectedQuiescenceToken: initial.AccessState.QuiescenceToken})
	if err != nil {
		t.Fatal(err)
	}
	if response.Entry == nil {
		t.Fatalf("response=%+v", response)
	}
	if content, err := os.ReadFile(filepath.Join(root, "unknown.txt")); err != nil || string(content) != "committed" {
		t.Fatalf("content=%q err=%v", content, err)
	}
	after, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if after.AccessState.Generation != initial.AccessState.Generation+1 || after.AccessState.QuiescenceToken == initial.AccessState.QuiescenceToken {
		t.Fatalf("initial=%+v after=%+v", initial.AccessState, after.AccessState)
	}
	head, err := m.Persistence.EventHead(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if head != 1 {
		t.Fatalf("event head=%d, want one commit", head)
	}
}

func TestParentTurnControlsWorkspaceAccessUntilTerminal(t *testing.T) {
	m, _ := workspaceManager(t)
	m.Runtime = fixedAdapterRuntime{adapter: restoreTestAdapter(t)}
	ctx := context.Background()
	response, err := m.StartTurn(ctx, &remotev1.StartTurnRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if response.TurnId != "turn" {
		t.Fatalf("response=%+v", response)
	}
	busy, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if busy.AccessState.ActiveAgentCount != 1 || busy.AccessState.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY || busy.AccessState.QuiescenceToken != "" {
		t.Fatalf("busy=%+v", busy.AccessState)
	}
	if err := m.applyAdapterEvent(ctx, adapter.Event{Kind: adapter.EventTurnUpdated, Method: "turn/completed", ThreadID: "thread", TurnID: "turn"}); err != nil {
		t.Fatal(err)
	}
	quiet, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if quiet.AccessState.ActiveAgentCount != 0 || quiet.AccessState.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED || quiet.AccessState.QuiescenceToken == "" || quiet.AccessState.Generation <= busy.AccessState.Generation {
		t.Fatalf("quiet=%+v busy=%+v", quiet.AccessState, busy.AccessState)
	}
}

func TestWorkspacePaginationTokenIsScoped(t *testing.T) {
	m, root := workspaceManager(t)
	for _, name := range []string{"a", "b", "c"} {
		if err := atomicTestWrite(filepath.Join(root, name), name); err != nil {
			t.Fatal(err)
		}
	}
	first, err := m.ListWorkspaceEntries(context.Background(), &remotev1.ListWorkspaceEntriesRequest{CodexId: "c", Page: &remotev1.PageRequest{PageSize: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Entries) != 2 || first.Page.NextPageToken == "" {
		t.Fatalf("first=%+v", first)
	}
	if _, err := m.ListWorkspaceEntries(context.Background(), &remotev1.ListWorkspaceEntriesRequest{CodexId: "c", RelativeDirectory: "other", Page: &remotev1.PageRequest{PageToken: first.Page.NextPageToken}}); err == nil {
		t.Fatal("cross-directory page token succeeded")
	}
}

func TestCollabSubagentsContributeIndependentlyToWorkspaceAccess(t *testing.T) {
	m, _ := workspaceManager(t)
	ctx := context.Background()
	if err := m.WorkspaceAgentStarted(ctx, "c", "parent-turn"); err != nil {
		t.Fatal(err)
	}
	spawn := collabEvent("thread", "parent-turn", "spawn-1", map[string]string{"child-a": "pendingInit", "child-b": "running"}, "child-a", "child-b")
	if err := m.applyAdapterEvent(ctx, spawn); err != nil {
		t.Fatal(err)
	}
	state, err := m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if state.AccessState.ActiveAgentCount != 3 {
		t.Fatalf("parent + children=%+v", state.AccessState)
	}

	m.mu.RLock()
	parentItems := len(m.byID["c"].GetActiveTurn().GetItems())
	m.mu.RUnlock()
	if err := m.applyAdapterEvent(ctx, adapter.Event{Kind: adapter.EventTurnUpdated, Method: "turn/completed", ThreadID: "child-a", TurnID: "child-turn"}); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 2 {
		t.Fatalf("child terminal=%+v", state.AccessState)
	}
	m.mu.RLock()
	afterChildItems := len(m.byID["c"].GetActiveTurn().GetItems())
	m.mu.RUnlock()
	if afterChildItems != parentItems {
		t.Fatalf("child canonical event was routed to parent: before=%d after=%d", parentItems, afterChildItems)
	}
	if err := m.applyAdapterEvent(ctx, spawn); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 2 {
		t.Fatalf("old spawn item revived terminal child=%+v", state.AccessState)
	}
	lateActive := collabToolEvent("thread", "parent-turn", "late-status", "wait", map[string]string{"child-a": "running"}, "child-a")
	if err := m.applyAdapterEvent(ctx, lateActive); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 2 {
		t.Fatalf("late non-resume item revived terminal child=%+v", state.AccessState)
	}
	resume := collabToolEvent("thread", "parent-turn", "resume-1", "resumeAgent", map[string]string{"child-a": "running"}, "child-a")
	if err := m.applyAdapterEvent(ctx, resume); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 3 {
		t.Fatalf("new resume item did not reactivate child=%+v", state.AccessState)
	}
	if err := m.applyAdapterEvent(ctx, adapter.Event{Kind: adapter.EventTurnUpdated, Method: "turn/completed", ThreadID: "child-a", TurnID: "child-turn-2"}); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 2 {
		t.Fatalf("resumed child terminal=%+v", state.AccessState)
	}
	descendant := collabEvent("child-b", "child-b-turn", "spawn-2", map[string]string{"grandchild": "running"}, "grandchild")
	if err := m.applyAdapterEvent(ctx, descendant); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 3 {
		t.Fatalf("descendant=%+v", state.AccessState)
	}

	terminalThenStale := collabEvent("child-b", "child-b-turn", "spawn-2", map[string]string{"grandchild": "completed"}, "grandchild")
	if err := m.applyAdapterEvent(ctx, terminalThenStale); err != nil {
		t.Fatal(err)
	}
	if err := m.applyAdapterEvent(ctx, terminalThenStale); err != nil {
		t.Fatal(err)
	}
	if err := m.applyAdapterEvent(ctx, descendant); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 2 {
		t.Fatalf("duplicate/out-of-order=%+v", state.AccessState)
	}

	childBTerminal := collabEvent("thread", "parent-turn", "spawn-1", map[string]string{"child-b": "shutdown"}, "child-b")
	if err := m.applyAdapterEvent(ctx, childBTerminal); err != nil {
		t.Fatal(err)
	}
	if err := m.WorkspaceAgentStopped(ctx, "c", "parent-turn"); err != nil {
		t.Fatal(err)
	}
	state, _ = m.GetWorkspace(ctx, &remotev1.GetWorkspaceRequest{CodexId: "c"})
	if state.AccessState.ActiveAgentCount != 0 || state.AccessState.QuiescenceToken == "" || state.AccessState.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED {
		t.Fatalf("final state=%+v", state.AccessState)
	}
}

func TestRestoreCollabAgentsUsesLatestPerChildState(t *testing.T) {
	m, _ := workspaceManager(t)
	active, _ := json.Marshal(map[string]any{"type": "collabAgentToolCall", "id": "spawn", "senderThreadId": "thread", "receiverThreadIds": []string{"child-a", "child-b"}, "agentsStates": map[string]any{"child-a": map[string]string{"status": "running"}, "child-b": map[string]string{"status": "pendingInit"}}})
	terminal, _ := json.Marshal(map[string]any{"type": "collabAgentToolCall", "id": "close", "senderThreadId": "thread", "receiverThreadIds": []string{"child-b"}, "agentsStates": map[string]any{"child-b": map[string]string{"status": "completed"}}})
	state, err := m.restoreCollabAgents("c", adapter.Thread{ID: "thread", Turns: []adapter.Turn{{ID: "turn", Items: []json.RawMessage{active, terminal}}}})
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.ActiveAgentCount != 1 || state.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY {
		t.Fatalf("restored state=%+v", state)
	}
	m.mu.RLock()
	childA, childB := m.workspaceChildCodex["child-a"], m.workspaceChildCodex["child-b"]
	childBTerminal := m.workspaceChildState["child-b"].Terminal
	m.mu.RUnlock()
	if childA != "c" || childB != "c" || !childBTerminal {
		t.Fatalf("restored associations: child-a=%q child-b=%q terminal=%v", childA, childB, childBTerminal)
	}
}

func atomicTestWrite(path, content string) error { return os.WriteFile(path, []byte(content), 0o644) }
