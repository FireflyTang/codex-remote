package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/attachment"
	"github.com/kylin1993/codex-remote/internal/gateway"
)

var onePixelPNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
	0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
	0xde, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41,
	0x54, 0x08, 0xd7, 0x63, 0xf8, 0xcf, 0xc0, 0x00, 0x00,
	0x03, 0x01, 0x01, 0x00, 0x18, 0xdd, 0x8d, 0xb0,
	0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44,
	0xae, 0x42, 0x60, 0x82,
}

func TestImageAttachmentFacadeAndHistoryTranslation(t *testing.T) {
	m := testManager(t)
	store, err := attachment.New(t.TempDir(), m.Persistence, attachment.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	m.SetAttachmentService(store)
	ctx := context.Background()
	c := &remotev1.Codex{CodexId: "c", ThreadId: "thread", Cwd: t.TempDir(), Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE}
	if err := m.saveCodex(ctx, c, "appServer"); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(onePixelPNG)
	uploaded, err := m.UploadImageAttachment(ctx, &remotev1.UploadImageAttachmentRequest{CodexId: c.CodexId, Filename: "pixel.png", MimeType: "image/png", Content: onePixelPNG, Sha256: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	downloaded, err := m.DownloadImageAttachment(ctx, &remotev1.DownloadImageAttachmentRequest{CodexId: c.CodexId, AttachmentId: uploaded.Attachment.AttachmentId})
	if err != nil || string(downloaded.Content) != string(onePixelPNG) || downloaded.Attachment.GetWidthPixels() != 1 || downloaded.Attachment.GetHeightPixels() != 1 {
		t.Fatalf("download=%+v err=%v", downloaded, err)
	}
	m.mu.RLock()
	ownerID := m.logicalOwners[c.CodexId]
	m.mu.RUnlock()
	_, path, err := store.Resolve(ctx, ownerID, uploaded.Attachment.AttachmentId)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"id": "user", "type": "userMessage", "input": []any{map[string]any{"type": "text", "text": "before"}, map[string]any{"type": "localImage", "path": path}, map[string]any{"type": "text", "text": "after"}}})
	snapshot := m.turnSnapshot(adapter.Turn{ID: "turn", Status: "completed", Items: []json.RawMessage{raw}, Completeness: adapter.TurnCompletenessFull}, m.imageResolver(ctx, c.CodexId))
	parts := snapshot.Items[0].GetUserMessage().GetParts()
	if len(parts) != 3 || parts[0].GetText().Text != "before" || parts[1].GetImage().AttachmentId != uploaded.Attachment.AttachmentId || parts[2].GetText().Text != "after" {
		t.Fatalf("parts=%+v", parts)
	}
	encoded, _ := json.Marshal(snapshot)
	if string(encoded) == "" || containsBytes(encoded, []byte(path)) {
		t.Fatalf("private attachment path leaked: %s", encoded)
	}
}

func TestDownloadImageAttachmentRejectsAnotherLogicalOwner(t *testing.T) {
	m := testManager(t)
	store, err := attachment.New(t.TempDir(), m.Persistence, attachment.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	m.SetAttachmentService(store)
	ctx := context.Background()
	for _, c := range []*remotev1.Codex{{CodexId: "a", ThreadId: "thread-a", Cwd: t.TempDir()}, {CodexId: "b", ThreadId: "thread-b", Cwd: t.TempDir()}} {
		c.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
		if err := m.saveCodex(ctx, c, "appServer"); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256(onePixelPNG)
	uploaded, err := m.UploadImageAttachment(ctx, &remotev1.UploadImageAttachmentRequest{CodexId: "a", Filename: "pixel.png", MimeType: "image/png", Content: onePixelPNG, Sha256: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.DownloadImageAttachment(ctx, &remotev1.DownloadImageAttachmentRequest{CodexId: "b", AttachmentId: uploaded.Attachment.AttachmentId})
	var rpc *gateway.RPCError
	if !errors.As(err, &rpc) || rpc.Detail.GetCode() != remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_NOT_FOUND {
		t.Fatalf("error=%v", err)
	}
}

func TestUnmarshalCurrentViewMigratesV11UserMessageInput(t *testing.T) {
	raw := []byte(`{"codex":{"codexId":"c"},"activeTurn":{"turnId":"t","items":[{"itemId":"i","userMessage":{"input":[{"text":{"text":"legacy"}}]}}]}}`)
	view := new(remotev1.CurrentView)
	if err := unmarshalCurrentView(raw, view); err != nil {
		t.Fatal(err)
	}
	parts := view.ActiveTurn.Items[0].GetUserMessage().GetParts()
	if len(parts) != 1 || parts[0].GetText().Text != "legacy" {
		t.Fatalf("parts=%+v", parts)
	}
}

func containsBytes(haystack, needle []byte) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == string(needle) {
			return true
		}
	}
	return false
}
