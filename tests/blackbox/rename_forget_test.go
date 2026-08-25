package blackbox_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func TestRenameAndForgetOverFormalWire(t *testing.T) {
	requireScenario(t, "rename-forget")
	c := dial(t)
	c.hello(t)
	root := testWorkspace(t)

	managed := createLifecycleCodex(t, c, filepath.Join(root, "rename-managed"))
	watchCodexIdentity(t, c, "rename-managed-watch", managed)
	renamedManaged := renameAndAwait(t, c, "rename-managed", managed, "Manual managed title")
	if renamedManaged.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED {
		t.Fatalf("RenameCodex changed managed lifecycle: %+v", renamedManaged)
	}
	assertListedTitle(t, c, managed.CodexId, "Manual managed title")

	materializedTurn := startAndAwaitTurn(t, c, "rename-managed-materialize", managed.CodexId, "materialize before forgetting")
	if materializedTurn == "" {
		t.Fatal("materialized turn id is empty")
	}
	assertListedTitle(t, c, managed.CodexId, "Manual managed title")

	managedForget := c.request(t, request("forget-managed-rejected", &remotev1.Request_ForgetCodex{ForgetCodex: &remotev1.ForgetCodexRequest{CodexId: managed.CodexId}}))
	if managedForget.GetError() == nil || managedForget.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("ForgetCodex managed=%+v, want CONFLICT", managedForget)
	}

	running := createLifecycleCodex(t, c, filepath.Join(root, "rename-running"))
	watchCodexIdentity(t, c, "rename-running-watch", running)
	runningTurn := c.request(t, request("rename-running-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: running.CodexId, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "remain running while renamed"}}}},
	}})).GetStartTurn()
	if runningTurn == nil || runningTurn.TurnId == "" {
		t.Fatalf("running StartTurn=%+v", runningTurn)
	}
	waitForTurnStatus(t, c, running.CodexId, runningTurn.TurnId, remotev1.TurnStatus_TURN_STATUS_RUNNING)
	renameAndAwait(t, c, "rename-running", running, "Manual running title")
	if got := listCodex(t, c, running.CodexId); got.Status != remotev1.CodexStatus_CODEX_STATUS_RUNNING || got.Title != "Manual running title" {
		t.Fatalf("running rename changed lifecycle: %+v", got)
	}
	interrupted := c.request(t, request("rename-running-interrupt", &remotev1.Request_InterruptTurn{InterruptTurn: &remotev1.InterruptTurnRequest{CodexId: running.CodexId, TurnId: runningTurn.TurnId}})).GetInterruptTurn()
	if interrupted == nil {
		t.Fatal("InterruptTurn returned no response")
	}
	waitForTurnStatus(t, c, running.CodexId, runningTurn.TurnId, remotev1.TurnStatus_TURN_STATUS_INTERRUPTED)

	unmanagedResponse := c.request(t, request("rename-forget-unmanage", &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: managed.CodexId}})).GetUnmanageCodex()
	if unmanagedResponse == nil || unmanagedResponse.Codex == nil || unmanagedResponse.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("UnmanageCodex=%+v", unmanagedResponse)
	}
	renamedUnmanaged := renameAndAwait(t, c, "rename-unmanaged", unmanagedResponse.Codex, "Manual unmanaged title")
	if renamedUnmanaged.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || renamedUnmanaged.ManagedUntilUnixMs != 0 {
		t.Fatalf("RenameCodex changed unmanaged lifecycle: %+v", renamedUnmanaged)
	}
	assertListedTitle(t, c, managed.CodexId, "Manual unmanaged title")

	forgetRequest := request("forget-unmanaged", &remotev1.Request_ForgetCodex{ForgetCodex: &remotev1.ForgetCodexRequest{CodexId: managed.CodexId}})
	forgotten := c.request(t, forgetRequest).GetForgetCodex()
	if forgotten == nil || forgotten.CodexId != managed.CodexId || forgotten.Deduplicated {
		t.Fatalf("ForgetCodex=%+v", forgotten)
	}
	terminal := c.readUntil(t, func(frame *remotev1.Frame) bool {
		event := frame.GetEvent()
		return event != nil && event.CodexId == managed.CodexId && event.GetCodexForgotten() != nil
	}).GetEvent()
	if terminal.CausedByRequestId != forgetRequest.RequestId || terminal.EventSeq == 0 {
		t.Fatalf("CodexForgotten=%+v", terminal)
	}
	assertCodexAbsent(t, c, managed.CodexId)
	watchForgotten := c.request(t, request("watch-forgotten", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: managed.CodexId}}))
	if watchForgotten.GetError() == nil || watchForgotten.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND {
		t.Fatalf("WatchCodex forgotten=%+v", watchForgotten)
	}
	replayed := c.request(t, forgetRequest).GetForgetCodex()
	if replayed == nil || replayed.CodexId != managed.CodexId || !replayed.Deduplicated {
		t.Fatalf("ForgetCodex replay=%+v", replayed)
	}

	candidate := findSessionCandidate(t, c, managed.ThreadId, managed.Cwd)
	if candidate.Availability != remotev1.SessionAvailability_SESSION_AVAILABILITY_RESUMABLE || candidate.ManagedCodexId != "" {
		t.Fatalf("forgotten session candidate=%+v", candidate)
	}
	imported := c.request(t, request("forget-reimport", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: managed.ThreadId, Source: candidate.Source}})).GetImportSession()
	if imported == nil || imported.Codex == nil || imported.Codex.CodexId == managed.CodexId || imported.Codex.ThreadId != managed.ThreadId || imported.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED {
		t.Fatalf("ImportSession after forget=%+v, forgotten=%+v", imported, managed)
	}
	reimportedHistory := c.request(t, request("forget-reimport-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: imported.Codex.CodexId}})).GetListHistory()
	if reimportedHistory == nil || reimportedHistory.History == nil || reimportedHistory.History.CodexId != imported.Codex.CodexId || len(reimportedHistory.History.Turns) != 1 || reimportedHistory.History.Turns[0].TurnId != materializedTurn || !reimportedHistory.History.HistoryComplete {
		t.Fatalf("reimported session lost continuous history: imported=%+v history=%+v want_turn=%q", imported.Codex, reimportedHistory, materializedTurn)
	}

	unmaterialized := createLifecycleCodex(t, c, filepath.Join(root, "forget-unmaterialized"))
	unmaterializedUnmanaged := c.request(t, request("forget-unmaterialized-unmanage", &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: unmaterialized.CodexId}})).GetUnmanageCodex()
	if unmaterializedUnmanaged == nil || unmaterializedUnmanaged.Codex == nil || unmaterializedUnmanaged.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("unmaterialized UnmanageCodex=%+v", unmaterializedUnmanaged)
	}
	unmaterializedForgotten := c.request(t, request("forget-unmaterialized", &remotev1.Request_ForgetCodex{ForgetCodex: &remotev1.ForgetCodexRequest{CodexId: unmaterialized.CodexId}})).GetForgetCodex()
	if unmaterializedForgotten == nil || unmaterializedForgotten.CodexId != unmaterialized.CodexId {
		t.Fatalf("unmaterialized ForgetCodex=%+v", unmaterializedForgotten)
	}
	assertCodexAbsent(t, c, unmaterialized.CodexId)
	unmaterializedCandidate := findSessionCandidate(t, c, unmaterialized.ThreadId, unmaterialized.Cwd)
	if unmaterializedCandidate.Availability != remotev1.SessionAvailability_SESSION_AVAILABILITY_RESUMABLE || unmaterializedCandidate.ManagedCodexId != "" {
		t.Fatalf("forgotten unmaterialized candidate=%+v", unmaterializedCandidate)
	}
	unmaterializedImported := c.request(t, request("forget-unmaterialized-reimport", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{
		SessionId: unmaterialized.ThreadId, Source: unmaterializedCandidate.Source,
	}})).GetImportSession()
	if unmaterializedImported == nil || unmaterializedImported.Codex == nil || unmaterializedImported.Codex.CodexId == unmaterialized.CodexId || unmaterializedImported.Codex.ThreadId != unmaterialized.ThreadId || unmaterializedImported.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED {
		t.Fatalf("ImportSession after unmaterialized forget=%+v, forgotten=%+v", unmaterializedImported, unmaterialized)
	}
	watchCodexIdentity(t, c, "forget-unmaterialized-reimport-watch", unmaterializedImported.Codex)
	firstTurn := startAndAwaitTurn(t, c, "forget-unmaterialized-first-turn", unmaterializedImported.Codex.CodexId, "materialize forgotten unmaterialized session")
	unmaterializedHistory := c.request(t, request("forget-unmaterialized-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: unmaterializedImported.Codex.CodexId}})).GetListHistory()
	if unmaterializedHistory == nil || unmaterializedHistory.History == nil || unmaterializedHistory.History.CodexId != unmaterializedImported.Codex.CodexId || len(unmaterializedHistory.History.Turns) != 1 || unmaterializedHistory.History.Turns[0].TurnId != firstTurn || !unmaterializedHistory.History.HistoryComplete {
		t.Fatalf("reimported unmaterialized session did not materialize: imported=%+v history=%+v want_turn=%q", unmaterializedImported.Codex, unmaterializedHistory, firstTurn)
	}
}

func renameAndAwait(t *testing.T, c *wireClient, requestSuffix string, before *remotev1.Codex, title string) *remotev1.Codex {
	t.Helper()
	requestID := "rename-" + requestSuffix
	response := c.request(t, request(requestID, &remotev1.Request_RenameCodex{RenameCodex: &remotev1.RenameCodexRequest{CodexId: before.CodexId, Title: title}})).GetRenameCodex()
	if response == nil || response.Codex == nil || response.Codex.CodexId != before.CodexId || response.Codex.ThreadId != before.ThreadId || response.Codex.Title != title {
		t.Fatalf("RenameCodex(%s)=%+v", requestSuffix, response)
	}
	event := c.readUntil(t, func(frame *remotev1.Frame) bool {
		value := frame.GetEvent()
		updated := value.GetCodexUpdated()
		return value != nil && value.CodexId == before.CodexId && value.CausedByRequestId == requestID && updated != nil && updated.Codex != nil && updated.Codex.Title == title
	}).GetEvent()
	if event.EventSeq == 0 {
		t.Fatalf("Rename CodexUpdated=%+v", event)
	}
	return response.Codex
}

func watchCodexIdentity(t *testing.T, c *wireClient, requestID string, codex *remotev1.Codex) {
	t.Helper()
	watch := c.request(t, request(requestID, &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codex.CodexId}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil || watch.ResetView.Codex == nil || watch.ResetView.Codex.CodexId != codex.CodexId || watch.ResetView.Codex.ThreadId != codex.ThreadId {
		t.Fatalf("WatchCodex(%s)=%+v", codex.CodexId, watch)
	}
}

func startAndAwaitTurn(t *testing.T, c *wireClient, requestID, codexID, input string) string {
	t.Helper()
	started := c.request(t, request(requestID, &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: input}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn(%s)=%+v", codexID, started)
	}
	waitForTurnStatus(t, c, codexID, started.TurnId, remotev1.TurnStatus_TURN_STATUS_COMPLETED)
	return started.TurnId
}

func assertCodexAbsent(t *testing.T, c *wireClient, codexID string) {
	t.Helper()
	pageToken := ""
	for page := 0; page < 32; page++ {
		listed := c.request(t, request(fmt.Sprintf("forget-list-absent-%s-%d-%d", codexID, page, time.Now().UnixNano()), &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{Page: &remotev1.PageRequest{PageSize: 3, PageToken: pageToken}}})).GetListCodexes()
		if listed == nil {
			t.Fatal("ListCodexes missing response")
		}
		for _, codex := range listed.Codexes {
			if codex.CodexId == codexID {
				t.Fatalf("ListCodexes still contains forgotten Codex %q: %+v", codexID, codex)
			}
		}
		pageToken = listed.GetPage().GetNextPageToken()
		if pageToken == "" {
			return
		}
	}
	t.Fatal("ListCodexes pagination did not terminate")
}

func findSessionCandidate(t *testing.T, c *wireClient, threadID, cwd string) *remotev1.SessionCandidate {
	t.Helper()
	pageToken := ""
	for page := 0; page < 32; page++ {
		listed := c.request(t, request(fmt.Sprintf("forget-candidate-%d-%d", page, time.Now().UnixNano()), &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{
			Cwd: cwd, Page: &remotev1.PageRequest{PageSize: 3, PageToken: pageToken},
		}})).GetListSessionCandidates()
		if listed == nil {
			t.Fatal("ListSessionCandidates missing response")
		}
		for _, candidate := range listed.Sessions {
			if candidate.SessionId == threadID {
				return candidate
			}
		}
		pageToken = listed.GetPage().GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	t.Fatalf("ListSessionCandidates missing forgotten thread %q", threadID)
	return nil
}

type manualRenameCheckpoint struct {
	CodexID  string `json:"codexId"`
	ThreadID string `json:"threadId"`
	Title    string `json:"title"`
}

func TestRestartManualRenamePersistsAndBlocksNativeTitle(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "create" {
		t.Skip("manual rename create phase only")
	}
	c := dial(t)
	c.hello(t)
	created := createLifecycleCodex(t, c, filepath.Join(testWorkspace(t), "manual-title"))
	watchCodexIdentity(t, c, "manual-title-watch", created)
	const title = "Persistent manual title"
	renameAndAwait(t, c, "restart-manual", created, title)
	startAndAwaitTurn(t, c, "manual-title-start", created.CodexId, "native app-server tries to rename this thread")
	assertListedTitle(t, c, created.CodexId, title)
	writeManualRenameCheckpoint(t, manualRenameCheckpoint{CodexID: created.CodexId, ThreadID: created.ThreadId, Title: title})
}

func TestRestartManualRenameSurvivesHostRestart(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "verify" {
		t.Skip("manual rename verify phase only")
	}
	checkpoint := readManualRenameCheckpoint(t)
	c := dial(t)
	c.hello(t)
	assertListedTitle(t, c, checkpoint.CodexID, checkpoint.Title)
	watch := c.request(t, request("manual-title-restart-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: checkpoint.CodexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil || watch.ResetView.Codex == nil || watch.ResetView.Codex.CodexId != checkpoint.CodexID || watch.ResetView.Codex.ThreadId != checkpoint.ThreadID || watch.ResetView.Codex.Title != checkpoint.Title {
		t.Fatalf("manual rename restart Watch=%+v", watch)
	}
}

func manualRenameCheckpointPath(t *testing.T) string {
	t.Helper()
	base := os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT")
	if base == "" {
		t.Fatal("CODEX_REMOTE_BLACKBOX_CHECKPOINT is unset")
	}
	return base + ".manual-title"
}

func writeManualRenameCheckpoint(t *testing.T, checkpoint manualRenameCheckpoint) {
	t.Helper()
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manualRenameCheckpointPath(t), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readManualRenameCheckpoint(t *testing.T) manualRenameCheckpoint {
	t.Helper()
	raw, err := os.ReadFile(manualRenameCheckpointPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint manualRenameCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
