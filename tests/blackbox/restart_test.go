package blackbox_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

type restartCheckpoint struct {
	HostID, HostRunID, CodexID, ThreadID, TurnID, CWD, CreateRequestID, PendingID string
	LastEventSeq                                                                  uint64
}

func TestPendingRestartCreate(t *testing.T) {
	requireScenario(t, "pending-restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "create" {
		t.Skip("pending restart create phase only")
	}
	c := dial(t)
	hello := c.hello(t)
	codexID := createWatchedCodex(t, c)
	pending := startAndWaitPending(t, c, codexID).GetUserInput()
	if pending == nil || pending.UserInputRequestId == "" {
		t.Fatalf("pending user input=%+v", pending)
	}
	writeCheckpoint(t, restartCheckpoint{HostID: hello.HostId, HostRunID: hello.HostRunId, CodexID: codexID, PendingID: pending.UserInputRequestId})
}

func TestPendingRestartDoesNotPretendRequestIsActionable(t *testing.T) {
	requireScenario(t, "pending-restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "verify" {
		t.Skip("pending restart verify phase only")
	}
	checkpoint := readCheckpoint(t)
	c := dial(t)
	c.hello(t)
	watch := c.request(t, request("pending-restart-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: checkpoint.CodexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil {
		t.Fatalf("restart Watch=%+v", watch)
	}
	for _, pending := range watch.ResetView.PendingRequests {
		if value := pending.GetUserInput(); value != nil && value.UserInputRequestId == checkpoint.PendingID {
			t.Fatalf("unrecoverable pending request still advertised as actionable: %+v", value)
		}
	}
	response := c.request(t, request("pending-restart-response", &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{CodexId: checkpoint.CodexID, UserInputRequestId: checkpoint.PendingID, Answers: []*remotev1.UserInputAnswer{{QuestionId: "choice", FreeFormText: "A"}}}}))
	if response.GetRespondUserInput() != nil || response.GetError() == nil {
		t.Fatalf("unrecoverable pending request falsely accepted: %+v", response)
	}
	if watch.ResetView.Completeness == nil || !watch.ResetView.Completeness.Incomplete {
		t.Fatalf("cleared pending request not disclosed as incomplete: %+v", watch.ResetView)
	}
}

func TestRestartCreateCompletedSession(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "create" {
		t.Skip("restart create phase only")
	}
	c := dial(t)
	hello := c.hello(t)
	cwd := filepath.Join(testWorkspace(t), "restart-project")
	checkpoint := restartCheckpoint{HostID: hello.HostId, HostRunID: hello.HostRunId, CWD: cwd, CreateRequestID: "restart-stable-create"}
	created := c.request(t, request(checkpoint.CreateRequestID, &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true, Title: "restart"}})).GetCreateCodex()
	if created == nil || created.Codex == nil {
		t.Fatalf("CreateCodex=%+v", created)
	}
	checkpoint.CodexID, checkpoint.ThreadID = created.Codex.CodexId, created.Codex.ThreadId
	c.request(t, request("restart-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: checkpoint.CodexID}}))
	started := c.request(t, request("restart-turn", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: checkpoint.CodexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "complete before restart"}}}}}})).GetStartTurn()
	if started == nil {
		t.Fatal("StartTurn missing response")
	}
	checkpoint.TurnID = started.TurnId
	for {
		ev := c.readUntil(t, func(f *remotev1.Frame) bool { return f.GetEvent() != nil }).GetEvent()
		checkpoint.LastEventSeq = ev.EventSeq
		if turn := ev.GetTurnUpdated(); turn != nil && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			break
		}
	}
	writeCheckpoint(t, checkpoint)
}

func TestRestartRestoresAndResetsWithoutReplay(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "verify" {
		t.Skip("restart verify phase only")
	}
	checkpoint := readCheckpoint(t)
	c := dial(t)
	hello := c.hello(t)
	if hello.HostId != checkpoint.HostID || hello.HostRunId == checkpoint.HostRunID {
		t.Fatalf("restart identities host=%q/%q want stable %q and new run != %q", hello.HostId, hello.HostRunId, checkpoint.HostID, checkpoint.HostRunID)
	}
	codexes := c.request(t, request("restart-list", &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{}})).GetListCodexes()
	if codexes == nil || !containsCodex(codexes.Codexes, checkpoint.CodexID) {
		t.Fatalf("restored ListCodexes=%+v", codexes)
	}
	history := c.request(t, request("restart-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: checkpoint.CodexID}})).GetListHistory()
	if history == nil || history.History == nil || len(history.History.Turns) != 1 || history.History.Turns[0].TurnId != checkpoint.TurnID {
		t.Fatalf("restored history=%+v", history)
	}
	watch := c.request(t, request("restart-reset", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: checkpoint.CodexID, AfterEventSeq: &checkpoint.LastEventSeq, AfterHostRunId: checkpoint.HostRunID}})).GetWatchCodex()
	if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESET || watch.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED || watch.ResetView == nil || watch.ResetView.Codex == nil || watch.ResetView.Codex.ActiveTurnId != "" {
		t.Fatalf("restart Watch=%+v", watch)
	}
	replay := c.request(t, request(checkpoint.CreateRequestID, &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: checkpoint.CWD, CreateDirectoryIfMissing: true, Title: "restart"}})).GetCreateCodex()
	if replay == nil || !replay.Deduplicated || replay.Codex.CodexId != checkpoint.CodexID {
		t.Fatalf("restart dedup=%+v", replay)
	}
}

func writeCheckpoint(t *testing.T, value restartCheckpoint) {
	t.Helper()
	path := os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT")
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readCheckpoint(t *testing.T) restartCheckpoint {
	t.Helper()
	raw, err := os.ReadFile(os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	var value restartCheckpoint
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
