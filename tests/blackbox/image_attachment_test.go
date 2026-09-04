package blackbox_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const onePixelPNGBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="

type imageAttachmentCheckpoint struct {
	CodexID                    string `json:"codexId"`
	ThreadID                   string `json:"threadId"`
	AttachmentID               string `json:"attachmentId"`
	SHA256                     string `json:"sha256"`
	WidthPixels                uint32 `json:"widthPixels"`
	HeightPixels               uint32 `json:"heightPixels"`
	UnmaterializedCodexID      string `json:"unmaterializedCodexId"`
	UnmaterializedThreadID     string `json:"unmaterializedThreadId"`
	UnmaterializedAttachmentID string `json:"unmaterializedAttachmentId"`
	UnmaterializedSHA256       string `json:"unmaterializedSha256"`
	UnmaterializedWidthPixels  uint32 `json:"unmaterializedWidthPixels"`
	UnmaterializedHeightPixels uint32 `json:"unmaterializedHeightPixels"`
}

func TestImageAttachmentRestartCreate(t *testing.T) {
	requireScenario(t, "image-attachments")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "create" {
		t.Skip("image attachment create phase only")
	}
	c := dial(t)
	hello := c.hello(t)
	caps := hello.GetCapabilities().GetImageAttachments()
	if caps == nil || !caps.Supported || caps.MaxUploadBytes == 0 || caps.UnreferencedRetentionMs == 0 || !containsString(caps.SupportedMimeTypes, "image/png") {
		t.Fatalf("image attachment capability is incomplete: %+v", caps)
	}

	root := filepath.Join(testWorkspace(t), "image-attachments")
	created := createLifecycleCodex(t, c, filepath.Join(root, "owner-a"))
	watchCodexIdentity(t, c, "image-watch", created)
	content, err := base64.StdEncoding.DecodeString(onePixelPNGBase64)
	if err != nil {
		t.Fatal(err)
	}
	digestBytes := sha256.Sum256(content)
	digest := hex.EncodeToString(digestBytes[:])
	uploadRequest := request("image-upload-once", &remotev1.Request_UploadImageAttachment{UploadImageAttachment: &remotev1.UploadImageAttachmentRequest{
		CodexId: created.CodexId, Filename: "pixel.png", MimeType: "image/png", Content: content, Sha256: digest,
	}})
	uploaded := c.request(t, uploadRequest).GetUploadImageAttachment()
	assertUploadedImage(t, uploaded, content, digest, false)
	attachment := uploaded.Attachment

	replayed := c.request(t, uploadRequest).GetUploadImageAttachment()
	assertUploadedImage(t, replayed, content, digest, true)
	if replayed.Attachment.AttachmentId != attachment.AttachmentId {
		t.Fatalf("upload replay changed attachment id: first=%q replay=%q", attachment.AttachmentId, replayed.Attachment.AttachmentId)
	}
	conflict := c.request(t, request("image-upload-once", &remotev1.Request_UploadImageAttachment{UploadImageAttachment: &remotev1.UploadImageAttachmentRequest{
		CodexId: created.CodexId, Filename: "changed.png", MimeType: "image/png", Content: content, Sha256: digest,
	}}))
	if conflict.GetError() == nil || conflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed upload replay=%+v, want CONFLICT", conflict)
	}

	other := createLifecycleCodex(t, c, filepath.Join(root, "owner-b"))
	foreign := c.request(t, request("image-cross-owner", &remotev1.Request_DownloadImageAttachment{DownloadImageAttachment: &remotev1.DownloadImageAttachmentRequest{
		CodexId: other.CodexId, AttachmentId: attachment.AttachmentId,
	}}))
	if foreign.GetError() == nil || foreign.GetError().Code != remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_NOT_FOUND {
		t.Fatalf("cross-session download=%+v, want IMAGE_ATTACHMENT_NOT_FOUND", foreign)
	}

	started := c.request(t, request("image-mixed-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: created.CodexId,
		Input: []*remotev1.UserInputPart{
			{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "before image"}}},
			{Content: &remotev1.UserInputPart_Image{Image: &remotev1.ImageInput{AttachmentId: attachment.AttachmentId}}},
			{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "after image"}}},
		},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("mixed StartTurn=%+v", started)
	}
	realtime := awaitRealtimeUserMessage(t, c)
	assertUserMessageParts(t, realtime, attachment)
	waitForTurnStatus(t, c, created.CodexId, started.TurnId, remotev1.TurnStatus_TURN_STATUS_COMPLETED)
	assertImageHistory(t, c, created.CodexId, attachment)
	assertImageDownload(t, c, created.CodexId, attachment, content)

	unmanaged := c.request(t, request("image-unmanage", &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: created.CodexId}})).GetUnmanageCodex()
	if unmanaged == nil || unmanaged.Codex == nil || unmanaged.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("image UnmanageCodex=%+v", unmanaged)
	}
	assertImageDownload(t, c, created.CodexId, attachment, content)
	assertImageHistory(t, c, created.CodexId, attachment)
	forgot := c.request(t, request("image-forget", &remotev1.Request_ForgetCodex{ForgetCodex: &remotev1.ForgetCodexRequest{CodexId: created.CodexId}})).GetForgetCodex()
	if forgot == nil || forgot.CodexId != created.CodexId {
		t.Fatalf("image ForgetCodex=%+v", forgot)
	}
	candidate := findSessionCandidate(t, c, created.ThreadId, created.Cwd)
	imported := c.request(t, request("image-reimport", &remotev1.Request_ImportSession{ImportSession: &remotev1.ImportSessionRequest{SessionId: created.ThreadId, Source: candidate.Source}})).GetImportSession()
	if imported == nil || imported.Codex == nil || imported.Codex.CodexId == created.CodexId {
		t.Fatalf("image ImportSession=%+v", imported)
	}
	assertImageDownload(t, c, imported.Codex.CodexId, attachment, content)
	assertImageHistory(t, c, imported.Codex.CodexId, attachment)
	unmaterialized := createLifecycleCodex(t, c, filepath.Join(root, "unmaterialized-owner"))
	unmaterializedUpload := c.request(t, request("image-unmaterialized-upload", &remotev1.Request_UploadImageAttachment{UploadImageAttachment: &remotev1.UploadImageAttachmentRequest{
		CodexId: unmaterialized.CodexId, Filename: "pixel.png", MimeType: "image/png", Content: content, Sha256: digest,
	}})).GetUploadImageAttachment()
	assertUploadedImage(t, unmaterializedUpload, content, digest, false)

	writeImageCheckpoint(t, imageAttachmentCheckpoint{
		CodexID: imported.Codex.CodexId, ThreadID: imported.Codex.ThreadId, AttachmentID: attachment.AttachmentId, SHA256: digest,
		WidthPixels: attachment.GetWidthPixels(), HeightPixels: attachment.GetHeightPixels(),
		UnmaterializedCodexID: unmaterialized.CodexId, UnmaterializedThreadID: unmaterialized.ThreadId,
		UnmaterializedAttachmentID: unmaterializedUpload.Attachment.AttachmentId, UnmaterializedSHA256: digest,
		UnmaterializedWidthPixels: unmaterializedUpload.Attachment.GetWidthPixels(), UnmaterializedHeightPixels: unmaterializedUpload.Attachment.GetHeightPixels(),
	})
}

func TestImageAttachmentRestartVerify(t *testing.T) {
	requireScenario(t, "image-attachments")
	if os.Getenv("CODEX_REMOTE_BLACKBOX_PHASE") != "verify" {
		t.Skip("image attachment verify phase only")
	}
	checkpoint := readImageCheckpoint(t)
	content, err := base64.StdEncoding.DecodeString(onePixelPNGBase64)
	if err != nil {
		t.Fatal(err)
	}
	c := dial(t)
	c.hello(t)
	restored := listCodex(t, c, checkpoint.CodexID)
	if restored.ThreadId != checkpoint.ThreadID {
		t.Fatalf("restart changed native session mapping: got=%q want=%q", restored.ThreadId, checkpoint.ThreadID)
	}
	attachment := &remotev1.ImageAttachment{
		AttachmentId: checkpoint.AttachmentID, Filename: "pixel.png", MimeType: "image/png", SizeBytes: uint64(len(content)), Sha256: checkpoint.SHA256,
		WidthPixels: uint32Pointer(checkpoint.WidthPixels), HeightPixels: uint32Pointer(checkpoint.HeightPixels),
	}
	assertImageDownload(t, c, checkpoint.CodexID, attachment, content)
	assertImageHistory(t, c, checkpoint.CodexID, attachment)

	unmaterializedAttachment := &remotev1.ImageAttachment{
		AttachmentId: checkpoint.UnmaterializedAttachmentID, Filename: "pixel.png", MimeType: "image/png", SizeBytes: uint64(len(content)), Sha256: checkpoint.UnmaterializedSHA256,
		WidthPixels: uint32Pointer(checkpoint.UnmaterializedWidthPixels), HeightPixels: uint32Pointer(checkpoint.UnmaterializedHeightPixels),
	}
	// Download must remain available even before the replacement native thread
	// receives its first user message.
	assertImageDownload(t, c, checkpoint.UnmaterializedCodexID, unmaterializedAttachment, content)
	restoredUnmaterialized := listCodex(t, c, checkpoint.UnmaterializedCodexID)
	if restoredUnmaterialized.ThreadId == checkpoint.UnmaterializedThreadID {
		t.Fatalf("unmaterialized restart did not replace missing native thread %q", checkpoint.UnmaterializedThreadID)
	}
	watchCodexIdentity(t, c, "image-unmaterialized-restart-watch", restoredUnmaterialized)
	started := startMixedImageTurn(t, c, checkpoint.UnmaterializedCodexID, checkpoint.UnmaterializedAttachmentID, "image-unmaterialized-first-turn")
	realtime := awaitRealtimeUserMessage(t, c)
	assertUserMessageParts(t, realtime, unmaterializedAttachment)
	waitForTurnStatus(t, c, checkpoint.UnmaterializedCodexID, started.TurnId, remotev1.TurnStatus_TURN_STATUS_COMPLETED)
	assertImageDownload(t, c, checkpoint.UnmaterializedCodexID, unmaterializedAttachment, content)
	assertImageHistory(t, c, checkpoint.UnmaterializedCodexID, unmaterializedAttachment)
}

func startMixedImageTurn(t *testing.T, c *wireClient, codexID, attachmentID, requestID string) *remotev1.StartTurnResponse {
	t.Helper()
	response := c.request(t, request(requestID, &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID,
		Input: []*remotev1.UserInputPart{
			{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "before image"}}},
			{Content: &remotev1.UserInputPart_Image{Image: &remotev1.ImageInput{AttachmentId: attachmentID}}},
			{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "after image"}}},
		},
	}})).GetStartTurn()
	if response == nil || response.TurnId == "" {
		t.Fatalf("mixed StartTurn=%+v", response)
	}
	return response
}

func awaitRealtimeUserMessage(t *testing.T, c *wireClient) *remotev1.UserMessageItem {
	t.Helper()
	for {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		for _, item := range []*remotev1.Item{event.GetItemStarted().GetItem(), event.GetItemUpdated().GetItem(), event.GetItemCompleted().GetItem()} {
			if item != nil && item.GetUserMessage() != nil {
				return item.GetUserMessage()
			}
		}
		if turn := event.GetTurnUpdated(); turn != nil && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			t.Fatal("turn completed without a realtime user-message item")
		}
	}
}

func assertUploadedImage(t *testing.T, response *remotev1.UploadImageAttachmentResponse, content []byte, digest string, deduplicated bool) {
	t.Helper()
	if response == nil || response.Attachment == nil || response.Deduplicated != deduplicated {
		t.Fatalf("UploadImageAttachment=%+v, want deduplicated=%v", response, deduplicated)
	}
	attachment := response.Attachment
	if attachment.AttachmentId == "" || attachment.Filename != "pixel.png" || attachment.MimeType != "image/png" || attachment.SizeBytes != uint64(len(content)) || attachment.Sha256 != digest || attachment.WidthPixels == nil || attachment.HeightPixels == nil || attachment.GetWidthPixels() != 1 || attachment.GetHeightPixels() != 1 {
		t.Fatalf("uploaded descriptor=%+v", attachment)
	}
}

func assertImageDownload(t *testing.T, c *wireClient, codexID string, want *remotev1.ImageAttachment, content []byte) {
	t.Helper()
	response := c.request(t, request(fmt.Sprintf("image-download-%s-%d", codexID, time.Now().UnixNano()), &remotev1.Request_DownloadImageAttachment{DownloadImageAttachment: &remotev1.DownloadImageAttachmentRequest{CodexId: codexID, AttachmentId: want.AttachmentId}})).GetDownloadImageAttachment()
	if response == nil || response.Attachment == nil || !bytes.Equal(response.Content, content) {
		t.Fatalf("DownloadImageAttachment=%+v", response)
	}
	assertSameImageDescriptor(t, response.Attachment, want)
	raw, err := protojson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "\"path\"") || strings.Contains(string(raw), "attachments/") {
		t.Fatalf("download exposed Host attachment path: %s", raw)
	}
}

func assertImageHistory(t *testing.T, c *wireClient, codexID string, attachment *remotev1.ImageAttachment) {
	t.Helper()
	response := c.request(t, request(fmt.Sprintf("image-history-%s-%d", codexID, time.Now().UnixNano()), &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if response == nil || response.History == nil || len(response.History.Turns) != 1 {
		t.Fatalf("image ListHistory=%+v", response)
	}
	for _, item := range response.History.Turns[0].Items {
		if message := item.GetUserMessage(); message != nil {
			assertUserMessageParts(t, message, attachment)
			raw, err := protojson.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "\"path\"") || strings.Contains(string(raw), "attachments/") {
				t.Fatalf("history exposed Host attachment path: %s", raw)
			}
			return
		}
	}
	t.Fatalf("history has no user-message item: %+v", response.History.Turns[0])
}

func assertUserMessageParts(t *testing.T, message *remotev1.UserMessageItem, attachment *remotev1.ImageAttachment) {
	t.Helper()
	if message == nil || len(message.Parts) != 3 || message.Parts[0].GetText().GetText() != "before image" || message.Parts[2].GetText().GetText() != "after image" || message.Parts[1].GetImage() == nil {
		t.Fatalf("user-message parts=%+v, want text/image/text", message)
	}
	assertSameImageDescriptor(t, message.Parts[1].GetImage(), attachment)
	raw, err := protojson.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "\"path\"") || strings.Contains(string(raw), "attachments/") {
		t.Fatalf("user message exposed Host attachment path: %s", raw)
	}
}

func assertSameImageDescriptor(t *testing.T, got, want *remotev1.ImageAttachment) {
	t.Helper()
	if got.AttachmentId != want.AttachmentId || got.Filename != want.Filename || got.MimeType != want.MimeType || got.SizeBytes != want.SizeBytes || got.Sha256 != want.Sha256 ||
		(got.WidthPixels == nil) != (want.WidthPixels == nil) || (got.HeightPixels == nil) != (want.HeightPixels == nil) || got.GetWidthPixels() != want.GetWidthPixels() || got.GetHeightPixels() != want.GetHeightPixels() {
		t.Fatalf("image descriptor=%+v, want %+v", got, want)
	}
}

func uint32Pointer(value uint32) *uint32 { return &value }

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func writeImageCheckpoint(t *testing.T, checkpoint imageAttachmentCheckpoint) {
	t.Helper()
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readImageCheckpoint(t *testing.T) imageAttachmentCheckpoint {
	t.Helper()
	raw, err := os.ReadFile(os.Getenv("CODEX_REMOTE_BLACKBOX_CHECKPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint imageAttachmentCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
