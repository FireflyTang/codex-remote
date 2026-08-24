package blackbox_test

import (
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func createWatchedCodex(t *testing.T, c *wireClient) string {
	t.Helper()
	root := testWorkspace(t)
	cwd := filepath.Join(root, "pending-project")
	id := "pending-create-" + time.Now().Format("150405.000000000")
	created := c.request(t, request(id, &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true}})).GetCreateCodex()
	if created == nil || created.Codex == nil {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId
	watch := c.request(t, request(id+"-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESET {
		t.Fatalf("Watch=%+v", watch)
	}
	return codexID
}

func startAndWaitPending(t *testing.T, c *wireClient, codexID string) *remotev1.PendingRequest {
	t.Helper()
	id := "pending-start-" + time.Now().Format("150405.000000000")
	start := c.request(t, request(id, &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "trigger pending"}}}}}})).GetStartTurn()
	if start == nil || start.TurnId == "" {
		t.Fatalf("StartTurn=%+v", start)
	}
	return c.readUntil(t, func(f *remotev1.Frame) bool {
		return f.GetEvent() != nil && f.GetEvent().GetPendingRequestUpdated() != nil
	}).GetEvent().GetPendingRequestUpdated().Request
}

func TestApprovalLifecycleAndDedup(t *testing.T) {
	requireScenario(t, "approval")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	pending := startAndWaitPending(t, c, codexID)
	approval := pending.GetApproval()
	if approval == nil || approval.ApprovalId != "approval-1" || approval.Status != remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING || len(approval.AllowedDecisions) != 3 {
		t.Fatalf("pending approval=%+v", approval)
	}
	otherCodexID := createWatchedCodex(t, c)
	cross := c.request(t, request("approval-cross-codex", &remotev1.Request_RespondApproval{RespondApproval: &remotev1.RespondApprovalRequest{CodexId: otherCodexID, ApprovalId: approval.ApprovalId, Decision: remotev1.ApprovalDecision_APPROVAL_DECISION_DENY}}))
	if cross.GetError() == nil {
		t.Fatalf("cross-Codex approval unexpectedly succeeded: %+v", cross)
	}
	id := "approval-response"
	req := request(id, &remotev1.Request_RespondApproval{RespondApproval: &remotev1.RespondApprovalRequest{CodexId: codexID, ApprovalId: approval.ApprovalId, Decision: remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW_FOR_SESSION}})
	resolved := c.request(t, req).GetRespondApproval()
	if resolved == nil || resolved.Approval == nil || resolved.Approval.ResolvedDecision != remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW_FOR_SESSION || resolved.Approval.Status != remotev1.ApprovalStatus_APPROVAL_STATUS_ALLOWED {
		t.Fatalf("RespondApproval=%+v", resolved)
	}
	replay := c.request(t, req).GetRespondApproval()
	if replay == nil || !replay.Deduplicated || replay.Approval.ResolvedDecision != remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW_FOR_SESSION {
		t.Fatalf("dedup RespondApproval=%+v", replay)
	}
	conflict := c.request(t, request(id, &remotev1.Request_RespondApproval{RespondApproval: &remotev1.RespondApprovalRequest{CodexId: codexID, ApprovalId: approval.ApprovalId, Decision: remotev1.ApprovalDecision_APPROVAL_DECISION_DENY}}))
	if conflict.GetError() == nil || conflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed-payload RespondApproval=%+v", conflict)
	}
	loser := c.request(t, request("approval-second-controller", &remotev1.Request_RespondApproval{RespondApproval: &remotev1.RespondApprovalRequest{CodexId: codexID, ApprovalId: approval.ApprovalId, Decision: remotev1.ApprovalDecision_APPROVAL_DECISION_DENY}}))
	if loser.GetError() == nil || loser.GetError().Code != remotev1.ErrorCode_ERROR_CODE_APPROVAL_ALREADY_RESOLVED {
		t.Fatalf("approval CAS loser=%+v", loser)
	}
}

func TestApprovalPendingAppearsInReconnectReset(t *testing.T) {
	requireScenario(t, "approval")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	pending := startAndWaitPending(t, c, codexID)
	approvalID := pending.GetApproval().ApprovalId
	_ = c.conn.CloseNow()

	c2 := dial(t)
	c2.hello(t)
	watch := c2.request(t, request("approval-reconnect", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil {
		t.Fatalf("reconnect Watch=%+v", watch)
	}
	for _, value := range watch.ResetView.PendingRequests {
		if value.GetApproval() != nil && value.GetApproval().ApprovalId == approvalID {
			return
		}
	}
	t.Fatalf("approval %q missing from reconnect CurrentView: %+v", approvalID, watch.ResetView.PendingRequests)
}

func TestStructuredUserInputLifecycle(t *testing.T) {
	requireScenario(t, "user-input")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	pending := startAndWaitPending(t, c, codexID)
	input := pending.GetUserInput()
	if input == nil || input.UserInputRequestId == "" || len(input.Questions) != 1 {
		t.Fatalf("pending user input=%+v", input)
	}
	question := input.Questions[0]
	if question.QuestionId != "choice" || len(question.Options) != 2 {
		t.Fatalf("structured question=%+v", question)
	}
	for i, option := range question.Options {
		if option.OptionId == "" {
			t.Errorf("option %d missing stable option_id: %+v", i, option)
		}
	}
	answer := &remotev1.UserInputAnswer{QuestionId: question.QuestionId, FreeFormText: "A"}
	otherCodexID := createWatchedCodex(t, c)
	cross := c.request(t, request("user-input-cross-codex", &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{CodexId: otherCodexID, UserInputRequestId: input.UserInputRequestId, Answers: []*remotev1.UserInputAnswer{answer}}}))
	if cross.GetError() == nil {
		t.Fatalf("cross-Codex user-input unexpectedly succeeded: %+v", cross)
	}
	id := "user-input-response"
	req := request(id, &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{CodexId: codexID, UserInputRequestId: input.UserInputRequestId, Answers: []*remotev1.UserInputAnswer{answer}}})
	resolved := c.request(t, req).GetRespondUserInput()
	if resolved == nil || resolved.Request == nil || !resolved.Request.Resolved {
		t.Fatalf("RespondUserInput=%+v", resolved)
	}
	if len(resolved.Request.Questions) != len(input.Questions) || len(resolved.Request.ResolvedAnswers) != 1 || resolved.Request.ResolvedAnswers[0].QuestionId != answer.QuestionId || resolved.Request.ResolvedAnswers[0].FreeFormText != answer.FreeFormText || resolved.Request.ResolvedAtUnixMs == 0 {
		t.Fatalf("resolved user-input lost authoritative questions/answers: pending=%+v resolved=%+v", input, resolved.Request)
	}
	replay := c.request(t, req).GetRespondUserInput()
	if replay == nil || !replay.Deduplicated || !replay.Request.Resolved {
		t.Fatalf("dedup RespondUserInput=%+v", replay)
	}
	conflict := c.request(t, request(id, &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{CodexId: codexID, UserInputRequestId: input.UserInputRequestId, Answers: []*remotev1.UserInputAnswer{{QuestionId: question.QuestionId, FreeFormText: "B"}}}}))
	if conflict.GetError() == nil || conflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed-payload RespondUserInput=%+v", conflict)
	}
	loser := c.request(t, request("user-input-second-controller", &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{CodexId: codexID, UserInputRequestId: input.UserInputRequestId, Answers: []*remotev1.UserInputAnswer{{QuestionId: question.QuestionId, FreeFormText: "B"}}}}))
	if loser.GetError() == nil || loser.GetError().Code != remotev1.ErrorCode_ERROR_CODE_USER_INPUT_ALREADY_RESOLVED {
		t.Fatalf("user-input CAS loser=%+v", loser)
	}
}
