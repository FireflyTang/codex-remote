package blackbox_test

import (
	"testing"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

// This table is a compile-time guard that every frozen V1 RPC has a
// constructible formal-wire request. Behavioral scenarios use the external
// deterministic fixture once the Host exposes its local listener seam.
func TestAllThirteenRPCsHaveFormalWireCases(t *testing.T) {
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
	}
	if len(cases) != 13 {
		t.Fatalf("RPC cases=%d, want 13", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.req.GetRequest() == nil {
				t.Fatal("nil request case")
			}
		})
	}
}
