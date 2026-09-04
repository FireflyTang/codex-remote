package codex

import (
	"encoding/json"
	"strings"
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/adapter"
)

func TestTranslateStructuredItemKinds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want func(*remotev1.Item) bool
	}{
		{"user", `{"id":"u","type":"userMessage","input":[{"type":"text","text":"hello"}]}`, func(i *remotev1.Item) bool { return i.GetUserMessage().Parts[0].GetText().Text == "hello" }},
		{"agent", `{"id":"a","type":"agentMessage","text":"answer"}`, func(i *remotev1.Item) bool { return i.GetAgentMessage().Text == "answer" }},
		{"reasoning", `{"id":"r","type":"reasoning","summary":"why"}`, func(i *remotev1.Item) bool { return i.GetReasoningSummary().Text == "why" }},
		{"plan", `{"id":"p","type":"plan","steps":[{"text":"one","status":"completed"}]}`, func(i *remotev1.Item) bool { return i.GetPlan().Steps[0].Text == "one" }},
		{"command", `{"id":"c","type":"commandExecution","command":["sh","-c","true"],"cwd":"/tmp","aggregatedOutput":"ok","exitCode":0}`, func(i *remotev1.Item) bool { return len(i.GetCommand().Argv) == 3 && i.GetCommand().HasExitCode }},
		{"file", `{"id":"f","type":"fileChange","changes":[{"path":"a","kind":"modified"}],"diff":"@@"}`, func(i *remotev1.Item) bool {
			return i.GetFileChange().Changes[0].Kind == remotev1.FileChangeKind_FILE_CHANGE_KIND_MODIFIED
		}},
		{"tool", `{"id":"t","type":"mcpToolCall","toolName":"lookup","arguments":{"q":"x"},"result":"done"}`, func(i *remotev1.Item) bool {
			return i.GetTool().ToolName == "lookup" && i.GetTool().ResultSummary == "done"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := translateItem(json.RawMessage(tt.raw), "turn", "fallback", "item/completed", adapter.SemanticUnknown, remotev1.ItemStatus_ITEM_STATUS_COMPLETED, 4096, remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY)
			if item == nil || !tt.want(item) || item.Provenance != remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY {
				t.Fatalf("translated item: %+v", item)
			}
		})
	}
}

func TestTranslateUserMessagePreservesMixedPartOrderWithoutExposingPath(t *testing.T) {
	raw := json.RawMessage(`{"id":"u","type":"userMessage","content":[{"type":"text","text":"before"},{"type":"localImage","path":"/private/blob"},{"type":"text","text":"after"}]}`)
	descriptor := &remotev1.ImageAttachment{AttachmentId: "att-1", Filename: "image.png", MimeType: "image/png", SizeBytes: 12, Sha256: "abc"}
	item := translateItem(raw, "turn", "fallback", "item/completed", adapter.SemanticUnknown, remotev1.ItemStatus_ITEM_STATUS_COMPLETED, 4096, remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY, func(path string) (*remotev1.ImageAttachment, error) {
		if path != "/private/blob" {
			t.Fatalf("path=%q", path)
		}
		return descriptor, nil
	})
	parts := item.GetUserMessage().GetParts()
	if len(parts) != 3 || parts[0].GetText().Text != "before" || parts[1].GetImage().AttachmentId != "att-1" || parts[2].GetText().Text != "after" {
		t.Fatalf("parts=%+v", parts)
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "/private/blob") {
		t.Fatalf("private path leaked: %s", encoded)
	}
}

func TestTurnTranslationStatusTimeFailureAndCompleteness(t *testing.T) {
	started, completed := int64(101), int64(109)
	details := "details"
	m := &Manager{ContentBudget: 4096}
	turn := m.turnSnapshot(adapter.Turn{
		ID: "turn", Status: "failed", StartedAt: &started, CompletedAt: &completed,
		Error: &adapter.TurnError{Message: "boom", AdditionalDetails: &details, CodexErrorInfo: json.RawMessage(`{"kind":"other"}`)},
		Items: []json.RawMessage{json.RawMessage(`{"id":"a","type":"agentMessage","text":"partial"}`)}, ItemsView: "summary", Completeness: adapter.TurnCompletenessPartial,
	})
	if turn.Status != remotev1.TurnStatus_TURN_STATUS_FAILED || turn.StartedAtUnixMs != 101000 || turn.CompletedAtUnixMs != 109000 {
		t.Fatalf("turn timing/status: %+v", turn)
	}
	if turn.Failure == nil || turn.Failure.Message != "boom" || turn.Failure.Metadata["additional_details"] != "details" {
		t.Fatalf("turn failure: %+v", turn.Failure)
	}
	if turn.Completeness == nil || !turn.Completeness.Incomplete || turn.Provenance != remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY {
		t.Fatalf("turn completeness: %+v", turn)
	}
}

func TestSourceAndBoundPageTokens(t *testing.T) {
	if got := normalizeSource(json.RawMessage(`"cli"`)); got != "cli" {
		t.Fatalf("source=%q", got)
	}
	if got := normalizeSource(json.RawMessage(`{"kind":"vscode"}`)); got != "vscode" {
		t.Fatalf("source=%q", got)
	}
	token := encodePageToken(pageToken{Operation: "history", Query: "codex", Offset: 4})
	if got, _, err := page(&remotev1.PageRequest{PageToken: token}, 10, "history", "codex"); err != nil || got != 4 {
		t.Fatalf("page=%d err=%v", got, err)
	}
	if _, _, err := page(&remotev1.PageRequest{PageToken: token}, 10, "codexes", "all"); err == nil {
		t.Fatal("cross-operation token must be rejected")
	}
	if _, _, err := page(&remotev1.PageRequest{PageToken: token}, 10, "history", "other"); err == nil {
		t.Fatal("cross-query token must be rejected")
	}
}

func TestSemanticCommandDelta(t *testing.T) {
	m := &Manager{ContentBudget: 1024}
	turn := &remotev1.TurnSnapshot{TurnId: "turn"}
	got := m.applyItemDelta(turn, adapter.Event{TurnID: "turn", ItemID: "command", Semantic: adapter.SemanticCommandOutput, Stream: "stderr"}, "failure")
	if got != "failure" || turn.Items[0].GetCommand().Output != "failure" {
		t.Fatalf("delta=%q turn=%+v", got, turn)
	}
	if outputStream("stderr") != remotev1.OutputStream_OUTPUT_STREAM_STDERR {
		t.Fatal("stderr stream not preserved")
	}
}
