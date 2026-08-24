package blackbox_test

import (
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func TestStructuredItemsDeltasAndHistory(t *testing.T) {
	requireScenario(t, "structured")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	started := c.request(t, request("structured-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "structured"}}}}}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}
	types := map[string]bool{}
	var commandDelta, reasoningDelta, fileDelta bool
	deadline := time.Now().Add(5 * time.Second)
	for !types["turn-completed"] && time.Now().Before(deadline) {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		var item *remotev1.Item
		switch {
		case event.GetItemStarted() != nil:
			item = event.GetItemStarted().Item
		case event.GetItemUpdated() != nil:
			item = event.GetItemUpdated().Item
		case event.GetItemCompleted() != nil:
			item = event.GetItemCompleted().Item
		case event.GetItemDelta() != nil:
			delta := event.GetItemDelta()
			if value := delta.GetCommandOutput(); value != nil && value.Stream == remotev1.OutputStream_OUTPUT_STREAM_STDERR && value.Text == "command delta" {
				commandDelta = true
			}
			if delta.ItemId == "item-reasoning" && delta.GetText() == " plus delta" {
				reasoningDelta = true
			}
			if delta.ItemId == "item-file" && delta.GetText() == "diff delta" {
				fileDelta = true
			}
		case event.GetTurnUpdated() != nil && event.GetTurnUpdated().Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED:
			turn := event.GetTurnUpdated()
			if turn.StartedAtUnixMs < 1_000_000_000_000 || turn.CompletedAtUnixMs < turn.StartedAtUnixMs {
				t.Fatalf("turn timestamps=%+v", turn)
			}
			types["turn-completed"] = true
		}
		markItemType(types, item)
	}
	for _, kind := range []string{"user", "reasoning", "plan", "command", "file", "tool", "agent", "turn-completed"} {
		if !types[kind] {
			t.Errorf("missing structured %s; seen=%v", kind, types)
		}
	}
	if !commandDelta || !reasoningDelta || !fileDelta {
		t.Fatalf("structured deltas command=%v reasoning=%v file=%v", commandDelta, reasoningDelta, fileDelta)
	}

	history := c.request(t, request("structured-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil || !history.History.HistoryComplete || len(history.History.Turns) != 1 {
		t.Fatalf("ListHistory=%+v", history)
	}
	turn := history.History.Turns[0]
	if turn.StartedAtUnixMs < 1_000_000_000_000 || turn.CompletedAtUnixMs < turn.StartedAtUnixMs || turn.Provenance != remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY {
		t.Fatalf("history turn metadata=%+v", turn)
	}
	historyTypes := map[string]bool{}
	for _, item := range turn.Items {
		markItemType(historyTypes, item)
		if item.Provenance != remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY {
			t.Errorf("history item missing imported provenance: %+v", item)
		}
	}
	for _, kind := range []string{"user", "reasoning", "plan", "command", "file", "tool", "agent"} {
		if !historyTypes[kind] {
			t.Errorf("history missing structured %s; seen=%v", kind, historyTypes)
		}
	}
}

func TestFailedTurnPreservesStatusErrorAndTime(t *testing.T) {
	requireScenario(t, "failed")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	c.request(t, request("failed-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "fail"}}}}}}))
	event := c.readUntil(t, func(frame *remotev1.Frame) bool {
		turn := frame.GetEvent().GetTurnUpdated()
		return turn != nil && turn.Status == remotev1.TurnStatus_TURN_STATUS_FAILED
	}).GetEvent().GetTurnUpdated()
	if event.Failure == nil || event.Failure.Message == "" || event.StartedAtUnixMs < 1_000_000_000_000 || event.CompletedAtUnixMs < event.StartedAtUnixMs {
		t.Fatalf("failed TurnUpdated=%+v", event)
	}
	history := c.request(t, request("failed-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil || len(history.History.Turns) != 1 || history.History.Turns[0].Status != remotev1.TurnStatus_TURN_STATUS_FAILED || history.History.Turns[0].Failure == nil {
		t.Fatalf("failed history=%+v", history)
	}
}

func markItemType(seen map[string]bool, item *remotev1.Item) {
	if item == nil {
		return
	}
	switch {
	case item.GetUserMessage() != nil:
		seen["user"] = true
	case item.GetReasoningSummary() != nil:
		seen["reasoning"] = true
	case item.GetPlan() != nil:
		seen["plan"] = true
	case item.GetCommand() != nil:
		seen["command"] = true
	case item.GetFileChange() != nil:
		seen["file"] = true
	case item.GetTool() != nil:
		seen["tool"] = true
	case item.GetAgentMessage() != nil:
		seen["agent"] = true
	}
}
