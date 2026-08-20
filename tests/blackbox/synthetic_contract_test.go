package blackbox_test

import (
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestSyntheticPlanDiffIDsAreStableAndUpserted(t *testing.T) {
	requireScenario(t, "synthetic-upsert")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	c.request(t, request("synthetic-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "synthetic ids"}}}}}}))
	var planIDs []string
	var diffID string
	for len(planIDs) < 2 || diffID == "" {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		updated := event.GetItemUpdated()
		if updated == nil || updated.Item == nil {
			continue
		}
		item := updated.Item
		if item.GetPlan() != nil {
			planIDs = append(planIDs, item.ItemId)
		}
		if item.GetFileChange() != nil {
			diffID = item.ItemId
		}
	}
	if planIDs[0] == "" || planIDs[0] != planIDs[1] {
		t.Fatalf("repeated plan updates lack stable synthetic ID: %q %q", planIDs[0], planIDs[1])
	}
	if diffID == "" || diffID == planIDs[0] {
		t.Fatalf("plan/diff synthetic IDs must be non-empty and distinct: plan=%q diff=%q", planIDs[0], diffID)
	}

	reset := c.request(t, request("synthetic-reset", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if reset == nil || reset.ResetView == nil || reset.ResetView.ActiveTurn == nil {
		t.Fatalf("synthetic RESET=%+v", reset)
	}
	var plans, diffs int
	for _, item := range reset.ResetView.ActiveTurn.Items {
		switch {
		case item.GetPlan() != nil:
			plans++
			if item.ItemId != planIDs[0] || len(item.GetPlan().Steps) != 2 || item.GetPlan().Steps[0].Status != "completed" {
				t.Errorf("upserted plan=%+v", item)
			}
		case item.GetFileChange() != nil:
			diffs++
			if item.ItemId != diffID || item.GetFileChange().UnifiedDiff == "" {
				t.Errorf("synthetic diff=%+v", item)
			}
		}
	}
	if plans != 1 || diffs != 1 {
		t.Fatalf("RESET should contain one upserted plan and one diff; plans=%d diffs=%d items=%+v", plans, diffs, reset.ResetView.ActiveTurn.Items)
	}
}

func TestEarlyLargeUpdatesSurviveStartResponseAndRemainActionable(t *testing.T) {
	requireScenario(t, "early-large")
	c := dial(t)
	hello := c.hello(t)
	cwd := filepath.Join(testWorkspace(t), "early-large")
	created := c.request(t, request("early-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true}})).GetCreateCodex()
	if created == nil || created.Codex == nil {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId
	watch := c.request(t, request("early-live-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.ResetView == nil {
		t.Fatalf("initial Watch=%+v", watch)
	}
	started := c.request(t, request("early-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "early large updates"}}}}}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}

	seen := map[string]bool{}
	nestedComplete := map[string]bool{}
	eventComplete := map[string]bool{}
	deadline := time.Now().Add(15 * time.Second)
	for !(seen["plan"] && seen["diff"] && seen["file"] && seen["warning"] && seen["approval"] && seen["input"]) {
		frame, err := nextFrameBefore(t, c, deadline)
		if err != nil {
			t.Fatalf("early events missing before RESET; seen=%v err=%v", seen, err)
		}
		if ping := frame.GetPing(); ping != nil {
			continue
		}
		event := frame.GetEvent()
		if event == nil {
			continue
		}
		raw, err := protojson.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		if uint64(len(raw)) > hello.MaxFrameBytes {
			t.Fatalf("canonical early Event exceeds advertised frame: bytes=%d max=%d event=%+v", len(raw), hello.MaxFrameBytes, event)
		}
		kind := ""
		if item := event.GetItemUpdated(); item != nil && item.Item != nil {
			seen["plan"] = seen["plan"] || item.Item.GetPlan() != nil
			seen["diff"] = seen["diff"] || item.Item.GetFileChange() != nil
			if item.Item.GetPlan() != nil {
				kind = "plan"
				nestedComplete["plan"] = nestedComplete["plan"] || explicitlyIncomplete(item.Item.Completeness)
				if len(item.Item.GetPlan().Steps) >= 800 {
					t.Errorf("large plan Event was not collection-truncated: steps=%d", len(item.Item.GetPlan().Steps))
				}
			}
			if item.Item.GetFileChange() != nil {
				kind = "diff"
				nestedComplete["diff"] = nestedComplete["diff"] || explicitlyIncomplete(item.Item.Completeness)
				if len(item.Item.GetFileChange().UnifiedDiff) >= 300000 {
					t.Errorf("large diff Event was not truncated: bytes=%d", len(item.Item.GetFileChange().UnifiedDiff))
				}
			}
		}
		if item := event.GetItemCompleted(); item != nil && item.Item != nil && item.Item.ItemId == "early-large-file" {
			kind = "file"
			seen["file"] = true
			nestedComplete["file"] = explicitlyIncomplete(item.Item.Completeness)
			if len(item.Item.GetFileChange().Changes) >= 1200 {
				t.Errorf("large file-change Event was not collection-truncated: changes=%d", len(item.Item.GetFileChange().Changes))
			}
		}
		if warning := event.GetWarningRaised(); warning != nil {
			kind = "warning"
			seen["warning"] = true
			if warning.Warning == nil || warning.Warning.Message == "" || len(warning.Warning.Message) >= 180000 {
				t.Errorf("large Warning Event did not retain a bounded message: %+v", warning.Warning)
			}
		}
		if pending := event.GetPendingRequestUpdated(); pending != nil && pending.Request != nil {
			if approval := pending.Request.GetApproval(); approval != nil {
				kind = "approval"
				seen["approval"] = true
				if approval.Explanation == "" || len(approval.Command) == 0 || len(approval.Command) >= 6000 {
					t.Errorf("large Approval Event did not retain bounded presentation: explanation=%d command=%d", len(approval.Explanation), len(approval.Command))
				}
			}
			if input := pending.Request.GetUserInput(); input != nil {
				kind = "input"
				seen["input"] = true
				nestedComplete["input"] = explicitlyIncomplete(input.Completeness)
				assertEarlyLargeQuestionIdentity(t, input)
			}
		}
		if kind != "" {
			eventComplete[kind] = eventComplete[kind] || explicitlyIncomplete(event.Completeness)
		}
	}
	for _, kind := range []string{"plan", "diff", "file", "warning", "approval", "input"} {
		if !eventComplete[kind] {
			t.Errorf("large %s Event lacks Event.completeness; complete=%v", kind, eventComplete)
		}
	}
	for _, kind := range []string{"plan", "diff", "file", "input"} {
		if !nestedComplete[kind] {
			t.Errorf("large %s nested model lacks explicit completeness; complete=%v", kind, nestedComplete)
		}
	}

	reset := c.request(t, request("early-reset", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if reset == nil || reset.ResetView == nil || reset.ResetView.ActiveTurn == nil || !explicitlyIncomplete(reset.ResetView.Completeness) {
		var hasView, hasActive bool
		var pendingCount int
		var completeness *remotev1.Completeness
		if reset != nil && reset.ResetView != nil {
			hasView = true
			hasActive = reset.ResetView.ActiveTurn != nil
			pendingCount = len(reset.ResetView.PendingRequests)
			completeness = reset.ResetView.Completeness
		}
		t.Fatalf("early-large RESET lacks active state/completeness: has_view=%v has_active=%v pending=%d completeness=%+v", hasView, hasActive, pendingCount, completeness)
	}
	for _, item := range reset.ResetView.ActiveTurn.Items {
		if item.ItemId == "" || !explicitlyIncomplete(item.Completeness) {
			t.Errorf("retained bounded item missing stable ID/completeness: %+v", item)
		}
	}
	var approval *remotev1.Approval
	var input *remotev1.UserInputRequestState
	for _, pending := range reset.ResetView.PendingRequests {
		if pending.GetApproval() != nil {
			approval = pending.GetApproval()
		}
		if pending.GetUserInput() != nil {
			input = pending.GetUserInput()
		}
	}
	if approval == nil || approval.ApprovalId != "early-large-approval" || approval.Explanation == "" || len(approval.Command) == 0 {
		t.Fatalf("bounded approval missing from RESET: %+v", approval)
	}
	if input == nil || input.UserInputRequestId != "early-input-rpc" || len(input.Questions) == 0 || !explicitlyIncomplete(input.Completeness) {
		t.Fatalf("bounded user input missing from RESET: %+v", input)
	}
	assertEarlyLargeQuestionIdentity(t, input)
	approvalResponse := c.request(t, request("early-approve", &remotev1.Request_RespondApproval{RespondApproval: &remotev1.RespondApprovalRequest{CodexId: codexID, ApprovalId: approval.ApprovalId, Decision: remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW}}))
	if approvalResponse.GetRespondApproval() == nil {
		t.Fatalf("bounded approval is not actionable: %+v", approvalResponse)
	}
	lateQuestion := input.Questions[len(input.Questions)-1]
	selected := []string{lateQuestion.Options[len(lateQuestion.Options)-2].OptionId, lateQuestion.Options[len(lateQuestion.Options)-1].OptionId}
	inputResponse := c.request(t, request("early-answer", &remotev1.Request_RespondUserInput{RespondUserInput: &remotev1.RespondUserInputRequest{CodexId: codexID, UserInputRequestId: input.UserInputRequestId, Answers: []*remotev1.UserInputAnswer{{QuestionId: lateQuestion.QuestionId, SelectedOptionIds: selected}}}}))
	resolved := inputResponse.GetRespondUserInput()
	if resolved == nil || resolved.Request == nil {
		t.Fatalf("bounded user input is not actionable: %+v", inputResponse)
	}
	if len(resolved.Request.ResolvedAnswers) != 1 || resolved.Request.ResolvedAnswers[0].QuestionId != lateQuestion.QuestionId || len(resolved.Request.ResolvedAnswers[0].SelectedOptionIds) != 2 || resolved.Request.ResolvedAnswers[0].SelectedOptionIds[0] != selected[0] || resolved.Request.ResolvedAnswers[0].SelectedOptionIds[1] != selected[1] {
		t.Fatalf("late multi-select answer was not preserved: %+v", resolved.Request.ResolvedAnswers)
	}
	if got := c.request(t, request("early-still-usable", &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}})).GetGetHost(); got == nil {
		t.Fatal("connection unusable after early-large event persistence")
	}
}

func assertEarlyLargeQuestionIdentity(t *testing.T, input *remotev1.UserInputRequestState) {
	t.Helper()
	if input == nil || len(input.Questions) != 20 {
		t.Fatalf("bounded user input must retain all question IDs: questions=%d", len(input.GetQuestions()))
	}
	questionIDs := make(map[string]struct{}, len(input.Questions))
	trimmedPresentation := false
	missingLabels := 0
	for _, question := range input.Questions {
		if question == nil || question.QuestionId == "" {
			t.Fatalf("bounded user input contains empty question identity: %+v", question)
		}
		if _, duplicate := questionIDs[question.QuestionId]; duplicate {
			t.Fatalf("duplicate question identity %q", question.QuestionId)
		}
		questionIDs[question.QuestionId] = struct{}{}
		if len(question.Options) != 40 {
			t.Fatalf("question %q lost selectable option identities: options=%d", question.QuestionId, len(question.Options))
		}
		optionIDs := make(map[string]struct{}, len(question.Options))
		for _, option := range question.Options {
			if option == nil || option.OptionId == "" {
				t.Fatalf("question %q contains non-selectable bounded option: %+v", question.QuestionId, option)
			}
			if option.Label == "" {
				missingLabels++
			}
			if _, duplicate := optionIDs[option.OptionId]; duplicate {
				t.Fatalf("question %q has duplicate option identity %q", question.QuestionId, option.OptionId)
			}
			optionIDs[option.OptionId] = struct{}{}
			trimmedPresentation = trimmedPresentation || len(option.Description) < 1000
		}
	}
	if !trimmedPresentation {
		t.Error("large user-input presentation was not truncated despite exceeding the frame budget")
	}
	if missingLabels != 0 {
		t.Errorf("bounded user input lost %d selectable option labels", missingLabels)
	}
	if !input.Questions[len(input.Questions)-1].AllowsMultiple {
		t.Error("late question lost its multi-select contract")
	}
}
