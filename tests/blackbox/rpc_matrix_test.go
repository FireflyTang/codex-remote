package blackbox_test

import (
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

// This table is a compile-time guard that every frozen V1 RPC has a
// constructible formal-wire request. Behavioral scenarios use the external
// deterministic fixture once the Host exposes its local listener seam.
func TestAllTwentyRPCsHaveFormalWireCases(t *testing.T) {
	cases := []struct {
		name string
		req  *remotev1.Request
	}{
		{"GetHost", &remotev1.Request{Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}},
		{"ListDirectories", &remotev1.Request{Request: &remotev1.Request_ListDirectories{ListDirectories: &remotev1.ListDirectoriesRequest{}}}},
		{"ListSessionCandidates", &remotev1.Request{Request: &remotev1.Request_ListSessionCandidates{ListSessionCandidates: &remotev1.ListSessionCandidatesRequest{}}}},
		{"ListCodexes", &remotev1.Request{Request: &remotev1.Request_ListCodexes{ListCodexes: &remotev1.ListCodexesRequest{}}}},
		{"CreateCodex", &remotev1.Request{Request: &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{}}}},
		{"ImportSession", &remotev1.Request{Request: &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{}}}},
		{"WatchCodex", &remotev1.Request{Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{}}}},
		{"UnwatchCodex", &remotev1.Request{Request: &remotev1.Request_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexRequest{}}}},
		{"ListHistory", &remotev1.Request{Request: &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{}}}},
		{"StartTurn", &remotev1.Request{Request: &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{}}}},
		{"InterruptTurn", &remotev1.Request{Request: &remotev1.Request_InterruptTurn{InterruptTurn: &remotev1.InterruptTurnRequest{}}}},
		{"RespondApproval", &remotev1.Request{Request: &remotev1.Request_RespondApproval{RespondApproval: &remotev1.RespondApprovalRequest{}}}},
		{"RespondUserInput", &remotev1.Request{Request: &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{}}}},
		{"UnmanageCodex", &remotev1.Request{Request: &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{}}}},
		{"GetWorkspace", &remotev1.Request{Request: &remotev1.Request_GetWorkspace{GetWorkspace: &remotev1.GetWorkspaceRequest{}}}},
		{"ListWorkspaceEntries", &remotev1.Request{Request: &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{}}}},
		{"ReadWorkspaceTextFile", &remotev1.Request{Request: &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{}}}},
		{"WriteWorkspaceTextFile", &remotev1.Request{Request: &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{}}}},
		{"UploadWorkspaceEntry", &remotev1.Request{Request: &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{}}}},
		{"DownloadWorkspaceEntry", &remotev1.Request{Request: &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{}}}},
	}
	if len(cases) != 20 {
		t.Fatalf("RPC cases=%d, want 20", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.req.GetRequest() == nil {
				t.Fatal("nil request case")
			}
		})
	}
}
