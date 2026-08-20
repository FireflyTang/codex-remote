package blackbox_test

import (
	"os"
	"path/filepath"
	"testing"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func TestDiscoverImportAndContinueUnmanagedSession(t *testing.T) {
	requireScenario(t, "sessions")
	c := dial(t)
	c.hello(t)
	cwd := filepath.Join(testWorkspace(t), "unmanaged")
	if err := os.MkdirAll(cwd, 0o700); err != nil {
		t.Fatal(err)
	}

	first := c.request(t, request("sessions-first", &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{Cwd: cwd, Page: &remotev1.PageRequest{PageSize: 1}}})).GetListSessionCandidates()
	if first == nil || len(first.Sessions) != 1 || first.Page == nil || first.Page.NextPageToken == "" {
		t.Fatalf("first candidates page=%+v", first)
	}
	candidate := first.Sessions[0]
	if candidate.SessionId == "" || candidate.Source != "exec" || candidate.ManagedCodexId != "" || candidate.Availability != remotev1.SessionAvailability_SESSION_AVAILABILITY_RESUMABLE {
		t.Fatalf("unmanaged candidate=%+v", candidate)
	}
	if candidate.CreatedAtUnixMs < 1_000_000_000_000 || candidate.UpdatedAtUnixMs < 1_000_000_000_000 {
		t.Fatalf("candidate timestamps are not Unix milliseconds: %+v", candidate)
	}
	second := c.request(t, request("sessions-second", &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{Cwd: cwd, Page: &remotev1.PageRequest{PageSize: 1, PageToken: first.Page.NextPageToken}}})).GetListSessionCandidates()
	if second == nil || len(second.Sessions) != 1 || second.Sessions[0].SessionId == candidate.SessionId {
		t.Fatalf("second candidates page=%+v", second)
	}

	wrongSource := c.request(t, request("import-wrong-source", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: candidate.SessionId, Source: "vscode"}}))
	if wrongSource.GetError() == nil || wrongSource.GetError().Code != remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED {
		t.Fatalf("wrong-source ImportSession=%+v", wrongSource)
	}
	imported := c.request(t, request("import-existing", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: candidate.SessionId, Source: candidate.Source}})).GetImportSession()
	if imported == nil || imported.Codex == nil || imported.Codex.ThreadId != candidate.SessionId || imported.Codex.Origin != remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING || imported.AlreadyManaged {
		t.Fatalf("ImportSession=%+v", imported)
	}
	codexID := imported.Codex.CodexId
	c.request(t, request("import-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}}))
	started := c.request(t, request("import-continue", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "continue imported session"}}}}}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("continue StartTurn=%+v", started)
	}
	for {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		if turn := event.GetTurnUpdated(); turn != nil && turn.TurnId == started.TurnId && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			break
		}
	}
	history := c.request(t, request("import-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil || len(history.History.Turns) != 1 || history.History.Turns[0].TurnId != started.TurnId {
		t.Fatalf("continued imported history=%+v", history)
	}
}

func TestPageTokensAreBoundToOperationAndNormalizedQuery(t *testing.T) {
	requireScenario(t, "sessions")
	c := dial(t)
	c.hello(t)
	root := testWorkspace(t)
	for _, name := range []string{"a", "b", "c", "d"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	dirs := c.request(t, request("bound-dirs", &remotev1.Request_ListDirectories{ListDirectories: &remotev1.ListDirectoriesRequest{ParentPath: root, Page: &remotev1.PageRequest{PageSize: 1}}})).GetListDirectories()
	if dirs == nil || dirs.Page == nil || dirs.Page.NextPageToken == "" {
		t.Fatalf("directory token source=%+v", dirs)
	}
	crossRPC := c.request(t, request("token-cross-rpc", &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{Page: &remotev1.PageRequest{PageToken: dirs.Page.NextPageToken}}}))
	requireInvalidRequest(t, crossRPC, "directory token reused by ListCodexes")
	crossDir := c.request(t, request("token-cross-dir", &remotev1.Request_ListDirectories{ListDirectories: &remotev1.ListDirectoriesRequest{ParentPath: filepath.Join(root, "a"), Page: &remotev1.PageRequest{PageToken: dirs.Page.NextPageToken}}}))
	requireInvalidRequest(t, crossDir, "directory token reused for another parent")

	unmanaged := filepath.Join(root, "unmanaged")
	other := filepath.Join(root, "other")
	sessions := c.request(t, request("bound-sessions", &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{Cwd: unmanaged, Page: &remotev1.PageRequest{PageSize: 1}}})).GetListSessionCandidates()
	if sessions == nil || sessions.Page == nil || sessions.Page.NextPageToken == "" {
		t.Fatalf("session token source=%+v", sessions)
	}
	crossCWD := c.request(t, request("token-cross-cwd", &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{Cwd: other, Page: &remotev1.PageRequest{PageSize: 1, PageToken: sessions.Page.NextPageToken}}}))
	requireInvalidRequest(t, crossCWD, "session token reused for another cwd")
}

func requireInvalidRequest(t *testing.T, response *remotev1.Response, context string) {
	t.Helper()
	if response.GetError() == nil || response.GetError().Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("%s response=%+v", context, response)
	}
}
