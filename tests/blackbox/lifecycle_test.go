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

func TestManagementLifecycleOverFormalWire(t *testing.T) {
	requireScenario(t, "lifecycle")
	c := dial(t)
	c.hello(t)
	root := testWorkspace(t)

	passive := createLifecycleCodex(t, c, filepath.Join(root, "lifecycle-passive"))
	foreground := createLifecycleCodex(t, c, filepath.Join(root, "lifecycle-foreground"))
	manual := createLifecycleCodex(t, c, filepath.Join(root, "lifecycle-manual"))
	importResponse := c.request(t, request("lifecycle-import", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{
		SessionId: "lifecycle-import-thread", Source: "exec",
	}}))
	imported := importResponse.GetImportSession()
	if imported == nil || imported.Codex == nil {
		t.Fatalf("ImportSession response=%+v", importResponse)
	}
	assertFreshManaged(t, "CreateCodex", passive)
	assertFreshManaged(t, "CreateCodex foreground", foreground)
	assertFreshManaged(t, "ImportSession", imported.Codex)

	watch := c.request(t, request("lifecycle-watch-passive", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: passive.CodexId}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil {
		t.Fatalf("WatchCodex=%+v", watch)
	}
	firstTurn := c.request(t, request("lifecycle-passive-first-turn", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: passive.CodexId,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "history before automatic unmanage"}}}},
	}})).GetStartTurn()
	if firstTurn == nil || firstTurn.TurnId == "" {
		t.Fatalf("initial passive StartTurn=%+v", firstTurn)
	}
	waitForTurnStatus(t, c, passive.CodexId, firstTurn.TurnId, remotev1.TurnStatus_TURN_STATUS_COMPLETED)
	passive = listCodex(t, c, passive.CodexId)
	passiveDeadline := passive.ManagedUntilUnixMs
	beforeExpiryHistory := c.request(t, request("lifecycle-passive-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: passive.CodexId}})).GetListHistory()
	if beforeExpiryHistory == nil || beforeExpiryHistory.History == nil || len(beforeExpiryHistory.History.Turns) != 1 || beforeExpiryHistory.History.Turns[0].TurnId != firstTurn.TurnId {
		t.Fatalf("history before automatic unmanage=%+v", beforeExpiryHistory)
	}
	_ = c.request(t, request("lifecycle-passive-list", &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{}}))
	respondToNextPing(t, c, nil)
	if got := listCodex(t, c, passive.CodexId); got.ManagedUntilUnixMs != passiveDeadline {
		t.Fatalf("passive Watch/ListHistory/ListCodexes/Pong renewed deadline: got=%d want=%d", got.ManagedUntilUnixMs, passiveDeadline)
	}
	waitUntilUnixMs(t, passiveDeadline-500-200)
	preThreshold := c.request(t, request("lifecycle-pre-threshold-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: passive.CodexId}})).GetWatchCodex()
	if preThreshold == nil || preThreshold.ResetView == nil || preThreshold.ResetView.Codex == nil {
		t.Fatalf("pre-threshold Watch=%+v", preThreshold)
	}
	preThresholdCodex := preThreshold.ResetView.Codex
	if preThresholdCodex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || preThresholdCodex.ManagedUntilUnixMs != passiveDeadline || countLeaseWarnings(preThresholdCodex, passiveDeadline) != 0 {
		t.Fatalf("lease changed before 75%% warning threshold: %+v", preThresholdCodex)
	}

	warningEvent := c.readUntil(t, func(frame *remotev1.Frame) bool {
		event := frame.GetEvent()
		if event == nil || event.CodexId != passive.CodexId {
			return false
		}
		warning := event.GetWarningRaised().GetWarning()
		return warning != nil && warning.Code == remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON
	}).GetEvent().GetWarningRaised().GetWarning()
	if warningEvent.ManagedUntilUnixMs != passiveDeadline {
		t.Fatalf("typed warning deadline=%d, want %d", warningEvent.ManagedUntilUnixMs, passiveDeadline)
	}
	passive = waitForManagementState(t, c, passive.CodexId, remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON)
	if passive.ManagedUntilUnixMs != passiveDeadline {
		t.Fatalf("expiring passive Codex=%+v, want original deadline", passive)
	}

	unmanageReq := request("lifecycle-manual-unmanage", &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: manual.CodexId}})
	unmanaged := c.request(t, unmanageReq).GetUnmanageCodex()
	if unmanaged == nil || unmanaged.Codex == nil || unmanaged.Codex.CodexId != manual.CodexId || unmanaged.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || unmanaged.Codex.ManagedUntilUnixMs != 0 {
		t.Fatalf("manual UnmanageCodex=%+v", unmanaged)
	}
	replay := c.request(t, unmanageReq).GetUnmanageCodex()
	if replay == nil || !replay.Deduplicated || replay.Codex == nil || replay.Codex.CodexId != manual.CodexId {
		t.Fatalf("dedup UnmanageCodex=%+v", replay)
	}

	foregroundDeadline := foreground.ManagedUntilUnixMs
	respondToNextPing(t, c, []string{foreground.CodexId, manual.CodexId})
	nonForeground := listCodex(t, c, passive.CodexId)
	if nonForeground.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || nonForeground.ManagedUntilUnixMs != passiveDeadline {
		t.Fatalf("targeted foreground Pong renewed unlisted passive Codex: %+v", nonForeground)
	}
	foreground = waitForDeadlineAfter(t, c, foreground.CodexId, foregroundDeadline)
	if foreground.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED {
		t.Fatalf("foreground renewal state=%v, want MANAGED", foreground.ManagementState)
	}
	manualAfterPong := listCodex(t, c, manual.CodexId)
	if manualAfterPong.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || manualAfterPong.ManagedUntilUnixMs != 0 {
		t.Fatalf("foreground Pong restored unmanaged Codex: %+v", manualAfterPong)
	}

	passive = waitForManagementState(t, c, passive.CodexId, remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED)
	if passive.ThreadId == "" || passive.ManagedUntilUnixMs != 0 {
		t.Fatalf("automatic unmanage lost mapping or retained deadline: %+v", passive)
	}
	history := c.request(t, request("lifecycle-unmanaged-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: passive.CodexId}})).GetListHistory()
	if history == nil || history.History == nil || history.History.CodexId != passive.CodexId || len(history.History.Turns) != 1 || history.History.Turns[0].TurnId != firstTurn.TurnId {
		t.Fatalf("unmanaged history=%+v", history)
	}
	reset := c.request(t, request("lifecycle-unmanaged-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: passive.CodexId}})).GetWatchCodex()
	if reset == nil || reset.ResetView == nil || reset.ResetView.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || countLeaseWarnings(reset.ResetView.Codex, passiveDeadline) != 1 {
		t.Fatalf("unmanaged Watch view=%+v", reset)
	}
	respondToNextPing(t, c, []string{passive.CodexId})
	if got := listCodex(t, c, passive.CodexId); got.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("foreground Pong restored automatically unmanaged Codex: %+v", got)
	}

	started := c.request(t, request("lifecycle-resume-turn", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: passive.CodexId,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "resume the same unmanaged session"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn on unmanaged Codex=%+v", started)
	}
	resumed := listCodex(t, c, passive.CodexId)
	if resumed.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || resumed.ManagedUntilUnixMs <= time.Now().UnixMilli() || resumed.CodexId != passive.CodexId || resumed.ThreadId != passive.ThreadId {
		t.Fatalf("StartTurn did not restore same Codex/session: before=%+v after=%+v", passive, resumed)
	}
	waitForTurnStatus(t, c, passive.CodexId, started.TurnId, remotev1.TurnStatus_TURN_STATUS_COMPLETED)
	history = c.request(t, request("lifecycle-resumed-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: passive.CodexId}})).GetListHistory()
	if history == nil || history.History == nil || history.History.CodexId != passive.CodexId || len(history.History.Turns) != 2 || !historyHasTurns(history.History, firstTurn.TurnId, started.TurnId) {
		t.Fatalf("resumed history lost continuity: %+v", history)
	}

	assertBusyUnmanageDoesNotInterrupt(t, c, filepath.Join(root, "lifecycle-running"), false)
	assertBusyUnmanageDoesNotInterrupt(t, c, filepath.Join(root, "lifecycle-pending"), true)
}

type lifecycleRestartCheckpoint struct {
	WarnedCodexID, ManualCodexID, FreshCodexID string
	WarnedDeadline, FreshDeadline              int64
}

func TestRestartLifecycleCreate(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "create" {
		t.Skip("restart lifecycle create phase only")
	}
	c := dial(t)
	c.hello(t)
	root := testWorkspace(t)
	warned := createLifecycleCodex(t, c, filepath.Join(root, "restart-lifecycle-warned"))
	_ = c.request(t, request("restart-lifecycle-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: warned.CodexId}}))
	warning := c.readUntil(t, func(frame *remotev1.Frame) bool {
		event := frame.GetEvent()
		return event != nil && event.CodexId == warned.CodexId && event.GetWarningRaised().GetWarning().GetCode() == remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON
	}).GetEvent().GetWarningRaised().GetWarning()
	if warning.ManagedUntilUnixMs != warned.ManagedUntilUnixMs {
		t.Fatalf("restart warning deadline=%d, want %d", warning.ManagedUntilUnixMs, warned.ManagedUntilUnixMs)
	}

	manual := createLifecycleCodex(t, c, filepath.Join(root, "restart-lifecycle-manual"))
	manualResult := c.request(t, request("restart-lifecycle-unmanage", &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: manual.CodexId}})).GetUnmanageCodex()
	if manualResult == nil || manualResult.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("restart setup UnmanageCodex=%+v", manualResult)
	}
	fresh := createLifecycleCodex(t, c, filepath.Join(root, "restart-lifecycle-fresh"))
	writeLifecycleCheckpoint(t, lifecycleRestartCheckpoint{
		WarnedCodexID: warned.CodexId, ManualCodexID: manual.CodexId, FreshCodexID: fresh.CodexId,
		WarnedDeadline: warned.ManagedUntilUnixMs, FreshDeadline: fresh.ManagedUntilUnixMs,
	})
}

func TestRestartLifecyclePreservesDeadlineUnmanagedAndWarningDedup(t *testing.T) {
	requireScenario(t, "restart")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "verify" {
		t.Skip("restart lifecycle verify phase only")
	}
	checkpoint := readLifecycleCheckpoint(t)
	c := dial(t)
	c.hello(t)

	manual := listCodex(t, c, checkpoint.ManualCodexID)
	if manual.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED || manual.ManagedUntilUnixMs != 0 {
		t.Fatalf("restart changed manually unmanaged Codex: %+v", manual)
	}
	fresh := listCodex(t, c, checkpoint.FreshCodexID)
	if time.Now().UnixMilli() >= checkpoint.FreshDeadline {
		t.Fatalf("restart verification missed fresh lease deadline %d", checkpoint.FreshDeadline)
	}
	if fresh.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || fresh.ManagedUntilUnixMs != checkpoint.FreshDeadline {
		t.Fatalf("restart gifted or changed fresh lease: got=%+v want deadline=%d", fresh, checkpoint.FreshDeadline)
	}

	watch := c.request(t, request("restart-lifecycle-verify-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: checkpoint.WarnedCodexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil || watch.ResetView.Codex == nil {
		t.Fatalf("restart warned Watch=%+v", watch)
	}
	warned := watch.ResetView.Codex
	if countLeaseWarnings(warned, checkpoint.WarnedDeadline) != 1 {
		t.Fatalf("restart duplicated or lost warning: %+v", warned.Warnings)
	}
	if time.Now().UnixMilli() < checkpoint.WarnedDeadline {
		if warned.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON || warned.ManagedUntilUnixMs != checkpoint.WarnedDeadline {
			t.Fatalf("restart changed warned lease before expiry: %+v", warned)
		}
	} else {
		warned = waitForManagementState(t, c, checkpoint.WarnedCodexID, remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED)
		if warned.ManagedUntilUnixMs != 0 {
			t.Fatalf("expired warned lease retained deadline after restart: %+v", warned)
		}
	}
	time.Sleep(100 * time.Millisecond)
	watch = c.request(t, request("restart-lifecycle-verify-rewatch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: checkpoint.WarnedCodexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil || watch.ResetView.Codex == nil {
		t.Fatalf("restart warned rewatch=%+v", watch)
	}
	warned = watch.ResetView.Codex
	if countLeaseWarnings(warned, checkpoint.WarnedDeadline) != 1 {
		t.Fatalf("post-restart sweep duplicated warning: %+v", warned.Warnings)
	}
}

func createLifecycleCodex(t *testing.T, c *wireClient, cwd string) *remotev1.Codex {
	t.Helper()
	id := fmt.Sprintf("lifecycle-create-%d", time.Now().UnixNano())
	created := c.request(t, request(id, &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true}})).GetCreateCodex()
	if created == nil || created.Codex == nil {
		t.Fatalf("CreateCodex(%s)=%+v", cwd, created)
	}
	return created.Codex
}

func assertFreshManaged(t *testing.T, label string, codex *remotev1.Codex) {
	t.Helper()
	if codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || codex.ManagedUntilUnixMs <= time.Now().UnixMilli() {
		t.Fatalf("%s initial lifecycle=%+v", label, codex)
	}
}

func respondToNextPing(t *testing.T, c *wireClient, foreground []string) {
	t.Helper()
	for i := 0; i < 128; i++ {
		frame := c.readNetworkFrame(t)
		if ping := frame.GetPing(); ping != nil {
			c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Pong{Pong: &remotev1.Pong{
				Nonce: ping.Nonce, PingSentAtUnixMs: ping.SentAtUnixMs, PongSentAtUnixMs: time.Now().UnixMilli(), ForegroundCodexIds: foreground,
			}}})
			return
		}
		c.inbox = append(c.inbox, frame)
	}
	t.Fatal("Ping not received after 128 frames")
}

func listCodex(t *testing.T, c *wireClient, codexID string) *remotev1.Codex {
	t.Helper()
	pageToken := ""
	for pageNumber := 0; pageNumber < 32; pageNumber++ {
		response := c.request(t, request(fmt.Sprintf("lifecycle-list-%d-%d", time.Now().UnixNano(), pageNumber), &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{
			Page: &remotev1.PageRequest{PageSize: 3, PageToken: pageToken},
		}})).GetListCodexes()
		if response == nil {
			t.Fatal("ListCodexes missing response")
		}
		for _, codex := range response.Codexes {
			if codex.CodexId == codexID {
				return codex
			}
		}
		pageToken = response.GetPage().GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	t.Fatalf("ListCodexes missing %q", codexID)
	return nil
}

func waitForManagementState(t *testing.T, c *wireClient, codexID string, want remotev1.ManagementState) *remotev1.Codex {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		codex := listCodex(t, c, codexID)
		if codex.ManagementState == want {
			return codex
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Codex %q did not reach management state %v", codexID, want)
	return nil
}

func waitForDeadlineAfter(t *testing.T, c *wireClient, codexID string, old int64) *remotev1.Codex {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		codex := listCodex(t, c, codexID)
		if codex.ManagedUntilUnixMs > old {
			return codex
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Codex %q deadline was not renewed after %d", codexID, old)
	return nil
}

func waitUntilUnixMs(t *testing.T, target int64) {
	t.Helper()
	wait := time.Until(time.UnixMilli(target))
	if wait <= 0 {
		t.Fatalf("absolute lifecycle checkpoint %d already passed by %s", target, -wait)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	<-timer.C
}

func waitForTurnStatus(t *testing.T, c *wireClient, codexID, turnID string, want remotev1.TurnStatus) {
	t.Helper()
	c.readUntil(t, func(frame *remotev1.Frame) bool {
		event := frame.GetEvent()
		return event != nil && event.CodexId == codexID && event.GetTurnUpdated().GetTurnId() == turnID && event.GetTurnUpdated().GetStatus() == want
	})
}

func assertBusyUnmanageDoesNotInterrupt(t *testing.T, c *wireClient, cwd string, pending bool) {
	t.Helper()
	codex := createLifecycleCodex(t, c, cwd)
	_ = c.request(t, request("lifecycle-busy-watch-"+filepath.Base(cwd), &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codex.CodexId}}))
	started := c.request(t, request("lifecycle-busy-start-"+filepath.Base(cwd), &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codex.CodexId,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "hold busy"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("busy StartTurn(%s)=%+v", cwd, started)
	}
	if pending {
		request := c.readUntil(t, func(frame *remotev1.Frame) bool {
			event := frame.GetEvent()
			return event != nil && event.CodexId == codex.CodexId && event.GetPendingRequestUpdated().GetRequest().GetApproval() != nil
		}).GetEvent().GetPendingRequestUpdated().GetRequest()
		if request.GetApproval().ApprovalId != "lifecycle-approval" {
			t.Fatalf("pending approval=%+v", request.GetApproval())
		}
	} else {
		waitForTurnStatus(t, c, codex.CodexId, started.TurnId, remotev1.TurnStatus_TURN_STATUS_RUNNING)
	}

	response := c.request(t, request("lifecycle-busy-unmanage-"+filepath.Base(cwd), &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: codex.CodexId}}))
	if response.GetError() == nil || response.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CODEX_BUSY {
		t.Fatalf("busy UnmanageCodex(%s)=%+v", cwd, response)
	}
	got := waitForManagementState(t, c, codex.CodexId, remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON)
	if got.ActiveTurnId != started.TurnId || got.Status == remotev1.CodexStatus_CODEX_STATUS_IDLE {
		t.Fatalf("busy unmanage interrupted or cleared turn: %+v", got)
	}
	if pending && got.Status != remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL {
		t.Fatalf("pending approval status lost: %+v", got)
	}
}

func countLeaseWarnings(codex *remotev1.Codex, deadline int64) int {
	count := 0
	for _, warning := range codex.GetWarnings() {
		if warning.Code == remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON && warning.ManagedUntilUnixMs == deadline {
			count++
		}
	}
	return count
}

func historyHasTurns(history *remotev1.HistoryPage, turnIDs ...string) bool {
	found := make(map[string]bool, len(history.GetTurns()))
	for _, turn := range history.GetTurns() {
		found[turn.TurnId] = true
	}
	for _, turnID := range turnIDs {
		if !found[turnID] {
			return false
		}
	}
	return true
}

func writeLifecycleCheckpoint(t *testing.T, value lifecycleRestartCheckpoint) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT")+".lifecycle", raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readLifecycleCheckpoint(t *testing.T) lifecycleRestartCheckpoint {
	t.Helper()
	raw, err := os.ReadFile(os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT") + ".lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	var value lifecycleRestartCheckpoint
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
