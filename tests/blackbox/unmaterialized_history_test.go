package blackbox_test

import (
	"path/filepath"
	"strings"
	"testing"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func TestUnmaterializedCreatedThreadHasEmptyHistoryUntilFirstUserMessage(t *testing.T) {
	requireScenario(t, "unmaterialized-history")
	c := dial(t)
	c.hello(t)

	created := c.request(t, request("unmaterialized-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{
		Cwd: filepath.Join(testWorkspace(t), "created-before-first-message"), CreateDirectoryIfMissing: true, Title: "unmaterialized history",
	}})).GetCreateCodex()
	if created == nil || created.Codex == nil || created.Codex.CodexId == "" {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId

	invalidPage := c.request(t, request("unmaterialized-invalid-page", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{
		CodexId: codexID, Page: &remotev1.PageRequest{PageToken: "not-a-page-token"},
	}}))
	if invalidPage.GetListHistory() != nil || invalidPage.GetError() == nil || invalidPage.GetError().Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("unmaterialized history normalized malformed page token: %+v", invalidPage)
	}

	empty := c.request(t, request("unmaterialized-empty-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if empty == nil || empty.History == nil {
		t.Fatalf("pre-message ListHistory=%+v, want successful empty history", empty)
	}
	if empty.History.CodexId != codexID || len(empty.History.Turns) != 0 || !empty.History.HistoryComplete {
		t.Fatalf("pre-message history=%+v, want codex_id=%q, no turns, history_complete=true", empty.History, codexID)
	}

	other := c.request(t, request("other-invalid-request-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{
		Cwd: filepath.Join(testWorkspace(t), "other-invalid-request"), CreateDirectoryIfMissing: true, Title: "other invalid request",
	}})).GetCreateCodex()
	if other == nil || other.Codex == nil || other.Codex.CodexId == "" {
		t.Fatalf("second CreateCodex=%+v", other)
	}
	negative := c.request(t, request("other-invalid-request-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: other.Codex.CodexId}}))
	if negative.GetListHistory() != nil || negative.GetError() == nil {
		t.Fatalf("unrelated -32600 was swallowed: %+v", negative)
	}
	if !strings.Contains(negative.GetError().Message, "different deterministic reason") {
		t.Fatalf("unrelated -32600 lost its diagnostic: %+v", negative.GetError())
	}

	watch := c.request(t, request("unmaterialized-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil || watch.ResetView.Codex == nil || watch.ResetView.Codex.CodexId != codexID {
		t.Fatalf("WatchCodex=%+v", watch)
	}
	started := c.request(t, request("unmaterialized-first-turn", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "first user message"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}
	for {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		turn := event.GetTurnUpdated()
		if turn != nil && turn.TurnId == started.TurnId && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			break
		}
	}

	history := c.request(t, request("unmaterialized-materialized-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil {
		t.Fatalf("post-message ListHistory=%+v", history)
	}
	if history.History.CodexId != codexID || len(history.History.Turns) != 1 || history.History.Turns[0].TurnId != started.TurnId || !history.History.HistoryComplete {
		t.Fatalf("post-message history=%+v, want completed turn %q for codex %q", history.History, started.TurnId, codexID)
	}
}
