package blackbox_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func requireScenario(t *testing.T, want string) {
	t.Helper()
	if got := os.Getenv("CODEX_REMOTE_BLACKBOX_SCENARIO"); got != want {
		t.Skipf("scenario=%q, want %q", got, want)
	}
}

func testWorkspace(t *testing.T) string {
	t.Helper()
	state := os.Getenv("CODEX_REMOTE_BLACKBOX_STATE_DIR")
	if state == "" {
		t.Skip("CODEX_REMOTE_BLACKBOX_STATE_DIR is unset")
	}
	root := filepath.Join(filepath.Dir(state), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestNormalHostVerticalSlice(t *testing.T) {
	requireScenario(t, "normal")
	root := testWorkspace(t)
	for _, name := range []string{"c", "a", "b", "d"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	c := dial(t)
	c.hello(t)

	list := c.request(t, request("list-dirs", &remotev1.Request_ListDirectories{ListDirectories: &remotev1.ListDirectoriesRequest{ParentPath: root, Page: &remotev1.PageRequest{PageSize: 99}}})).GetListDirectories()
	if list == nil || len(list.Directories) != 3 || list.Page == nil || list.Page.NextPageToken == "" {
		t.Fatalf("ListDirectories first page=%+v, want clamped 3 entries and token", list)
	}
	page2 := c.request(t, request("list-dirs-2", &remotev1.Request_ListDirectories{ListDirectories: &remotev1.ListDirectoriesRequest{ParentPath: root, Page: &remotev1.PageRequest{PageSize: 3, PageToken: list.Page.NextPageToken}}})).GetListDirectories()
	if page2 == nil || len(page2.Directories) != 1 || page2.Page.NextPageToken != "" {
		t.Fatalf("ListDirectories second page=%+v", page2)
	}
	badPage := c.request(t, request("bad-page", &remotev1.Request_ListDirectories{ListDirectories: &remotev1.ListDirectoriesRequest{ParentPath: root, Page: &remotev1.PageRequest{PageToken: "not-a-token"}}}))
	if badPage.GetError() == nil || badPage.GetError().Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("bad page response=%+v", badPage)
	}

	cwd := filepath.Join(root, "new", "project")
	createReq := request("create-once", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true, Title: "blackbox"}})
	created := c.request(t, createReq).GetCreateCodex()
	if created == nil || created.Codex == nil || created.Codex.CodexId == "" || !created.DirectoryCreated || created.Codex.Cwd != cwd {
		t.Fatalf("CreateCodex=%+v", created)
	}
	if _, err := os.Stat(cwd); err != nil {
		t.Fatalf("created directory: %v", err)
	}
	codexID := created.Codex.CodexId

	replayed := c.request(t, createReq).GetCreateCodex()
	if replayed == nil || !replayed.Deduplicated || replayed.Codex.CodexId != codexID {
		t.Fatalf("dedup CreateCodex=%+v", replayed)
	}
	conflict := c.request(t, request("create-once", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, Title: "changed"}}))
	if conflict.GetError() == nil || conflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed-payload dedup response=%+v", conflict)
	}

	codexes := c.request(t, request("list-codexes", &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{}})).GetListCodexes()
	if codexes == nil || !containsCodex(codexes.Codexes, codexID) {
		t.Fatalf("ListCodexes=%+v missing %s", codexes, codexID)
	}
	candidates := c.request(t, request("candidates", &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{Cwd: cwd}})).GetListSessionCandidates()
	if candidates == nil || len(candidates.Sessions) != 1 || candidates.Sessions[0].ManagedCodexId != codexID {
		t.Fatalf("ListSessionCandidates=%+v", candidates)
	}
	imported := c.request(t, request("import-managed", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: created.Codex.ThreadId, Source: "appServer"}})).GetImportSession()
	if imported == nil || !imported.AlreadyManaged || imported.Codex.CodexId != codexID {
		t.Fatalf("ImportSession already-managed=%+v", imported)
	}
	importReplay := c.request(t, request("import-managed", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: created.Codex.ThreadId, Source: "appServer"}})).GetImportSession()
	if importReplay == nil || !importReplay.Deduplicated || importReplay.Codex.CodexId != codexID {
		t.Fatalf("dedup ImportSession=%+v", importReplay)
	}
	importConflict := c.request(t, request("import-managed", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: "different", Source: "appServer"}}))
	if importConflict.GetError() == nil || importConflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed-payload ImportSession=%+v", importConflict)
	}

	watch := c.request(t, request("watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESET || watch.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_INITIAL_WATCH || watch.ResetView == nil || watch.ResetView.HeadEventSeq != watch.HeadEventSeq {
		t.Fatalf("initial Watch=%+v", watch)
	}
	startReq := request("start-once", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "hello"}}}}}})
	started := c.request(t, startReq).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}
	startReplay := c.request(t, startReq).GetStartTurn()
	if startReplay == nil || !startReplay.Deduplicated || startReplay.TurnId != started.TurnId {
		t.Fatalf("dedup StartTurn=%+v", startReplay)
	}
	startConflict := c.request(t, request("start-once", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "changed"}}}}}}))
	if startConflict.GetError() == nil || startConflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed-payload StartTurn=%+v", startConflict)
	}

	var lastSeq uint64
	seen := map[string]bool{}
	deadline := time.Now().Add(5 * time.Second)
	for !seen["turn-completed"] && time.Now().Before(deadline) {
		frame := c.readUntil(t, func(f *remotev1.Frame) bool { return f.GetEvent() != nil })
		ev := frame.GetEvent()
		if ev.CodexId != codexID || ev.EventSeq <= lastSeq {
			t.Fatalf("event sequence/id invalid: %+v after %d", ev, lastSeq)
		}
		lastSeq = ev.EventSeq
		switch {
		case ev.GetCodexUpdated() != nil:
			seen["codex-updated"] = true
		case ev.GetTurnUpdated() != nil && ev.GetTurnUpdated().Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED:
			seen["turn-completed"] = true
		case ev.GetItemStarted() != nil:
			seen["item-started"] = true
		case ev.GetItemDelta() != nil:
			seen["item-delta"] = true
		case ev.GetItemUpdated() != nil:
			seen["item-updated"] = true
		case ev.GetItemCompleted() != nil:
			seen["item-completed"] = true
		case ev.GetWarningRaised() != nil:
			seen["warning-raised"] = true
		}
	}
	for _, kind := range []string{"codex-updated", "turn-completed", "item-started", "item-delta", "item-updated", "item-completed", "warning-raised"} {
		if !seen[kind] {
			t.Errorf("missing live %s event; seen=%v", kind, seen)
		}
	}

	history := c.request(t, request("history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil || len(history.History.Turns) != 1 || history.History.Turns[0].TurnId != started.TurnId || !history.History.HistoryComplete {
		t.Fatalf("ListHistory=%+v", history)
	}
	interrupt := c.request(t, request("interrupt-completed", &remotev1.Request_InterruptTurn{InterruptTurn: &remotev1.InterruptTurnRequest{CodexId: codexID, TurnId: started.TurnId}}))
	if interrupt.GetError() == nil || interrupt.GetError().Code != remotev1.ErrorCode_ERROR_CODE_TURN_NOT_RUNNING {
		t.Fatalf("Interrupt completed response=%+v", interrupt)
	}

	unwatch := c.request(t, request("unwatch", &remotev1.Request_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexRequest{CodexId: codexID}})).GetUnwatchCodex()
	if unwatch == nil || unwatch.CodexId != codexID {
		t.Fatalf("Unwatch=%+v", unwatch)
	}

	c2 := dial(t)
	secondHello := c2.hello(t)
	resume := c2.request(t, request("resume-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID, AfterEventSeq: &lastSeq, AfterHostRunId: secondHello.HostRunId}})).GetWatchCodex()
	if resume == nil || resume.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED {
		t.Fatalf("same-run resume Watch=%+v", resume)
	}
	invalid := lastSeq + 100
	reset := c2.request(t, request("invalid-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID, AfterEventSeq: &invalid, AfterHostRunId: secondHello.HostRunId}})).GetWatchCodex()
	if reset == nil || reset.Mode != remotev1.WatchMode_WATCH_MODE_RESET || reset.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_INVALID {
		t.Fatalf("invalid-cursor Watch=%+v", reset)
	}
	missingRun := c2.request(t, request("missing-run-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID, AfterEventSeq: &lastSeq}}))
	if missingRun.GetError() == nil || missingRun.GetError().Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("cursor without after_host_run_id=%+v", missingRun)
	}
}

func request(id string, body any) *remotev1.Request {
	r := &remotev1.Request{RequestId: id, SentAtUnixMs: time.Now().UnixMilli()}
	switch x := body.(type) {
	case *remotev1.Request_GetHost:
		r.Request = x
	case *remotev1.Request_ListDirectories:
		r.Request = x
	case *remotev1.Request_ListSessionCandidates:
		r.Request = x
	case *remotev1.Request_ListCodexes:
		r.Request = x
	case *remotev1.Request_CreateCodex:
		r.Request = x
	case *remotev1.Request_ImportSession:
		r.Request = x
	case *remotev1.Request_WatchCodex:
		r.Request = x
	case *remotev1.Request_UnwatchCodex:
		r.Request = x
	case *remotev1.Request_ListHistory:
		r.Request = x
	case *remotev1.Request_StartTurn:
		r.Request = x
	case *remotev1.Request_InterruptTurn:
		r.Request = x
	case *remotev1.Request_RespondApproval:
		r.Request = x
	case *remotev1.Request_RespondUserInput:
		r.Request = x
	case *remotev1.Request_UnmanageCodex:
		r.Request = x
	case *remotev1.Request_RenameCodex:
		r.Request = x
	case *remotev1.Request_ForgetCodex:
		r.Request = x
	case *remotev1.Request_GetWorkspace:
		r.Request = x
	case *remotev1.Request_ListWorkspaceEntries:
		r.Request = x
	case *remotev1.Request_ReadWorkspaceTextFile:
		r.Request = x
	case *remotev1.Request_WriteWorkspaceTextFile:
		r.Request = x
	case *remotev1.Request_UploadWorkspaceEntry:
		r.Request = x
	case *remotev1.Request_DownloadWorkspaceEntry:
		r.Request = x
	case *remotev1.Request_UploadImageAttachment:
		r.Request = x
	case *remotev1.Request_DownloadImageAttachment:
		r.Request = x
	default:
		panic("unsupported request body")
	}
	return r
}

func containsCodex(values []*remotev1.Codex, id string) bool {
	for _, value := range values {
		if value.CodexId == id {
			return true
		}
	}
	return false
}
