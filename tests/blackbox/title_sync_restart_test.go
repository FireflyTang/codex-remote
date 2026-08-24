package blackbox_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

const automaticThreadTitle = "Automatic title from first user message"

type titleSyncCheckpoint struct {
	HostID       string `json:"hostId"`
	HostRunID    string `json:"hostRunId"`
	CodexID      string `json:"codexId"`
	ThreadID     string `json:"threadId"`
	TurnID       string `json:"turnId"`
	LastEventSeq uint64 `json:"lastEventSeq"`
}

func TestRestartAutomaticThreadNameUpdatesAndPersists(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "create" {
		t.Skip("automatic title create phase only")
	}
	c := dial(t)
	hello := c.hello(t)
	created := c.request(t, request("title-sync-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{
		Cwd: filepath.Join(testWorkspace(t), "automatic-title"), CreateDirectoryIfMissing: true, Title: "Title before first message",
	}})).GetCreateCodex()
	if created == nil || created.Codex == nil || created.Codex.CodexId == "" || created.Codex.ThreadId == "" {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId
	watch := c.request(t, request("title-sync-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil || watch.ResetView.Codex == nil {
		t.Fatalf("WatchCodex=%+v", watch)
	}
	started := c.request(t, request("title-sync-first-turn", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "first user message names this thread"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}

	checkpoint := titleSyncCheckpoint{HostID: hello.HostId, HostRunID: hello.HostRunId, CodexID: codexID, ThreadID: created.Codex.ThreadId, TurnID: started.TurnId}
	seenTitle, seenTerminal := false, false
	for !seenTitle || !seenTerminal {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		if event.CodexId != codexID {
			continue
		}
		if event.EventSeq > checkpoint.LastEventSeq {
			checkpoint.LastEventSeq = event.EventSeq
		}
		if updated := event.GetCodexUpdated(); updated != nil && updated.Codex != nil && updated.Codex.Title == automaticThreadTitle {
			seenTitle = true
		}
		if turn := event.GetTurnUpdated(); turn != nil && turn.TurnId == started.TurnId && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			seenTerminal = true
		}
	}
	assertListedTitle(t, c, codexID, automaticThreadTitle)
	writeTitleSyncCheckpoint(t, checkpoint)
}

func TestRestartAutomaticThreadNameSurvivesReset(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "verify" {
		t.Skip("automatic title verify phase only")
	}
	checkpoint := readTitleSyncCheckpoint(t)
	c := dial(t)
	hello := c.hello(t)
	if hello.HostId != checkpoint.HostID || hello.HostRunId == checkpoint.HostRunID {
		t.Fatalf("restart identities host=%q/%q want stable %q and new run != %q", hello.HostId, hello.HostRunId, checkpoint.HostID, checkpoint.HostRunID)
	}
	assertListedTitle(t, c, checkpoint.CodexID, automaticThreadTitle)
	watch := c.request(t, request("title-sync-restart-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{
		CodexId: checkpoint.CodexID, AfterEventSeq: &checkpoint.LastEventSeq, AfterHostRunId: checkpoint.HostRunID,
	}})).GetWatchCodex()
	if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESET || watch.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED || watch.ResetView == nil || watch.ResetView.Codex == nil {
		t.Fatalf("restart Watch=%+v", watch)
	}
	if watch.ResetView.Codex.CodexId != checkpoint.CodexID || watch.ResetView.Codex.ThreadId != checkpoint.ThreadID || watch.ResetView.Codex.Title != automaticThreadTitle {
		t.Fatalf("restart reset Codex=%+v, want persisted automatic title %q", watch.ResetView.Codex, automaticThreadTitle)
	}
}

func assertListedTitle(t *testing.T, c *wireClient, codexID, want string) {
	t.Helper()
	listed := c.request(t, request("title-sync-list-"+codexID, &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{}})).GetListCodexes()
	if listed == nil {
		t.Fatal("ListCodexes missing response")
	}
	for _, codex := range listed.Codexes {
		if codex.CodexId == codexID {
			if codex.Title != want {
				t.Fatalf("ListCodexes title=%q, want %q", codex.Title, want)
			}
			return
		}
	}
	t.Fatalf("ListCodexes missing %s: %+v", codexID, listed.Codexes)
}

func titleSyncCheckpointPath(t *testing.T) string {
	t.Helper()
	base := os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT")
	if base == "" {
		t.Fatal("CODEX_REMOTE_BLACKBOX_CHECKPOINT is unset")
	}
	return base + ".auto-title"
}

func writeTitleSyncCheckpoint(t *testing.T, checkpoint titleSyncCheckpoint) {
	t.Helper()
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(titleSyncCheckpointPath(t), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTitleSyncCheckpoint(t *testing.T) titleSyncCheckpoint {
	t.Helper()
	raw, err := os.ReadFile(titleSyncCheckpointPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint titleSyncCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
