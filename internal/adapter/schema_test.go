package adapter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/websocket"
)

func initializeFakeAdapter(t *testing.T, server func(*websocket.Conn)) *Adapter {
	t.Helper()
	path := listenFake(t, func(c *websocket.Conn) {
		init := decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"id": json.RawMessage(init["id"]), "result": map[string]any{"userAgent": "fake", "codexHome": "/tmp", "platformFamily": "unix", "platformOs": "linux"}})
		_ = decodeMap(t, c)
		server(c)
	})
	client, err := Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := Initialize(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestTurnSchemaAndCollaborationModeArePreserved(t *testing.T) {
	request := make(chan map[string]json.RawMessage, 1)
	a := initializeFakeAdapter(t, func(c *websocket.Conn) {
		req := decodeMap(t, c)
		request <- req
		_ = encodeWS(c, map[string]any{"id": json.RawMessage(req["id"]), "result": map[string]any{"turn": map[string]any{"id": "turn-1", "status": "failed", "startedAt": 101, "completedAt": 109, "durationMs": 8000, "error": map[string]any{"message": "boom", "additionalDetails": "detail", "codexErrorInfo": map[string]any{"kind": "other"}}, "items": []any{map[string]any{"type": "agentMessage", "id": "item-1", "text": "partial"}}, "itemsView": "summary"}}})
		c.CloseNow()
	})
	turn, err := a.StartTurn(context.Background(), "thread-1", []TurnInput{{Type: "text", Text: "go"}, {Type: "localImage", Path: "/state/attachments/image"}, {Type: "text", Text: "now"}}, TurnOptions{Model: "gpt-5.6", CollaborationMode: "plan", ReasoningEffort: "high", ApprovalPolicy: "on-request"})
	if err != nil {
		t.Fatal(err)
	}
	if turn.StartedAt == nil || *turn.StartedAt != 101 || turn.CompletedAt == nil || *turn.CompletedAt != 109 || turn.DurationMS == nil || *turn.DurationMS != 8000 || turn.Error == nil || turn.Error.Message != "boom" || turn.ItemsView != "summary" || turn.Completeness != TurnCompletenessPartial || len(turn.Items) != 1 {
		t.Fatalf("turn=%+v", turn)
	}
	req := <-request
	var params map[string]json.RawMessage
	if err := json.Unmarshal(req["params"], &params); err != nil {
		t.Fatal(err)
	}
	if _, ok := params["effort"]; ok {
		t.Fatalf("collaboration mode incorrectly emitted top-level effort: %s", req["params"])
	}
	if _, ok := params["model"]; ok {
		t.Fatalf("collaboration mode incorrectly emitted top-level model: %s", req["params"])
	}
	var input []map[string]any
	if err := json.Unmarshal(params["input"], &input); err != nil {
		t.Fatal(err)
	}
	if len(input) != 3 || input[0]["type"] != "text" || input[0]["text"] != "go" || input[1]["type"] != "localImage" || input[1]["path"] != "/state/attachments/image" || input[2]["text"] != "now" {
		t.Fatalf("mixed input order not preserved: %s", params["input"])
	}
	var mode struct {
		Mode     string `json:"mode"`
		Settings struct {
			Model           string `json:"model"`
			ReasoningEffort string `json:"reasoning_effort"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(params["collaborationMode"], &mode); err != nil || mode.Mode != "plan" || mode.Settings.Model != "gpt-5.6" || mode.Settings.ReasoningEffort != "high" {
		t.Fatalf("mode=%+v err=%v params=%s", mode, err, req["params"])
	}
}

func TestReasoningEffortWithoutCollaborationModeUsesTopLevelField(t *testing.T) {
	request := make(chan map[string]json.RawMessage, 1)
	a := initializeFakeAdapter(t, func(c *websocket.Conn) {
		req := decodeMap(t, c)
		request <- req
		_ = encodeWS(c, map[string]any{"id": json.RawMessage(req["id"]), "result": map[string]any{"turn": map[string]any{"id": "turn", "status": "inProgress", "items": []any{}, "itemsView": "full"}}})
		c.CloseNow()
	})
	if _, err := a.StartTurn(context.Background(), "thread", nil, TurnOptions{Model: "gpt-5.6", ReasoningEffort: "medium"}); err != nil {
		t.Fatal(err)
	}
	req := <-request
	var params map[string]json.RawMessage
	_ = json.Unmarshal(req["params"], &params)
	if string(params["effort"]) != "\"medium\"" || string(params["model"]) != "\"gpt-5.6\"" {
		t.Fatalf("params=%s", req["params"])
	}
	if _, ok := params["collaborationMode"]; ok {
		t.Fatalf("unexpected collaborationMode: %s", req["params"])
	}
}

func TestCamelCaseNotificationSemanticsOverUnixWebSocket(t *testing.T) {
	a := initializeFakeAdapter(t, func(c *websocket.Conn) {
		messages := []any{
			map[string]any{"method": "item/commandExecution/outputDelta", "params": map[string]any{"threadId": "th", "turnId": "tu", "itemId": "cmd", "delta": "stdout"}},
			map[string]any{"method": "turn/plan/updated", "params": map[string]any{"threadId": "th", "turnId": "tu", "plan": []any{map[string]any{"step": "build", "status": "inProgress"}}}},
			map[string]any{"method": "turn/diff/updated", "params": map[string]any{"threadId": "th", "turnId": "tu", "diff": "@@ diff"}},
			map[string]any{"method": "process/outputDelta", "params": map[string]any{"processHandle": "p", "stream": "stderr", "deltaBase64": "YWJj", "capReached": false}},
		}
		for _, m := range messages {
			_ = encodeWS(c, m)
		}
		c.CloseNow()
	})
	want := []struct {
		semantic EventSemantic
		text     string
	}{
		{SemanticCommandOutput, "stdout"}, {SemanticPlanUpdated, ""}, {SemanticDiffUpdated, ""}, {SemanticProcessOutput, "YWJj"},
	}
	for i, w := range want {
		e, ok := <-a.Events()
		if !ok {
			t.Fatalf("event %d missing", i)
		}
		if e.Kind != map[bool]EventKind{true: EventItemUpdated, false: EventItemDelta}[w.semantic == SemanticPlanUpdated || w.semantic == SemanticDiffUpdated] || e.Semantic != w.semantic || e.Text != w.text {
			t.Fatalf("event %d=%+v want=%+v", i, e, w)
		}
		if w.semantic == SemanticPlanUpdated && (len(e.Plan) != 1 || e.Plan[0].Step != "build") {
			t.Fatalf("plan=%+v", e.Plan)
		}
		if w.semantic == SemanticDiffUpdated && e.Diff != "@@ diff" {
			t.Fatalf("diff=%q", e.Diff)
		}
		if w.semantic == SemanticProcessOutput && (e.Encoding != "base64" || e.Stream != "stderr") {
			t.Fatalf("process=%+v", e)
		}
	}
}
