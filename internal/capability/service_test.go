package capability

import (
	"strings"
	"testing"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestObserveSessionSourcesFromRuntime(t *testing.T) {
	s := New(4, 20)
	s.ObserveSessionSources("jetbrains", " cli ", "")
	got := s.Get()
	found := false
	for _, source := range got.SessionSourceKinds {
		found = found || source == "jetbrains"
	}
	features := make(map[string]bool, len(got.FeatureIds))
	for _, feature := range got.FeatureIds {
		features[feature] = true
	}
	workspace := got.GetWorkspace()
	if !found || !features["management_lease"] || !features["unmanage_codex"] || !features["workspace"] || workspace == nil || workspace.GetMaxTextFileBytes() == 0 || workspace.GetMaxInlineUploadBytes() == 0 || workspace.GetMaxInlineDownloadBytes() == 0 || workspace.GetMaxArchiveExpandedBytes() == 0 || workspace.GetMaxArchiveEntryCount() == 0 || got.MaxWatchesPerConnection != 4 || got.MaxPageSize != 20 {
		t.Fatalf("capabilities %+v", got)
	}
}

func TestDefaultWorkspacePayloadLimitsFitFrames(t *testing.T) {
	caps, err := WorkspaceCapabilitiesForFrame(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	assertWorkspaceFramesFit(t, caps, 4<<20)
	if caps.GetMaxTextFileBytes() != DefaultMaxTextFileBytes || caps.GetMaxInlineUploadBytes() != DefaultMaxInlineUploadBytes || caps.GetMaxInlineDownloadBytes() != DefaultMaxInlineDownloadBytes {
		t.Fatalf("default four MiB frame capabilities = %+v", caps)
	}

	small, err := WorkspaceCapabilitiesForFrame(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	assertWorkspaceFramesFit(t, small, 64<<10)
	if small.GetMaxTextFileBytes() == 0 || small.GetMaxInlineUploadBytes() == 0 || small.GetMaxInlineDownloadBytes() == 0 {
		t.Fatalf("small-frame capabilities must remain executable: %+v", small)
	}
}

func TestSetWorkspaceCapabilitiesRejectsZeroAndClones(t *testing.T) {
	service := New(4, 20)
	before := service.Get().GetWorkspace()
	if err := service.SetWorkspaceCapabilities(&remotev1.WorkspaceCapabilities{}); err == nil {
		t.Fatal("zero workspace limits were accepted")
	}
	if got := service.Get().GetWorkspace(); got.GetMaxInlineUploadBytes() != before.GetMaxInlineUploadBytes() {
		t.Fatalf("invalid update changed capabilities: %+v", got)
	}
	custom := DefaultWorkspaceCapabilities()
	custom.MaxInlineUploadBytes = 12345
	if err := service.SetWorkspaceCapabilities(custom); err != nil {
		t.Fatal(err)
	}
	custom.MaxInlineUploadBytes = 1
	if got := service.Get().GetWorkspace().GetMaxInlineUploadBytes(); got != 12345 {
		t.Fatalf("workspace capability was not cloned: %d", got)
	}
}

func assertWorkspaceFramesFit(t *testing.T, caps *remotev1.WorkspaceCapabilities, maxFrameBytes int) {
	t.Helper()
	worstCaseText := strings.Repeat("\x00", int(caps.GetMaxTextFileBytes()))
	frames := []*remotev1.Frame{
		{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "write-text", Request: &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{CodexId: "codex", RelativePath: "notes/control.txt", Utf8Text: worstCaseText, ExpectedRevision: strings.Repeat("r", 64), ExpectedQuiescenceToken: strings.Repeat("q", 64), Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY}}}}},
		{Payload: &remotev1.Frame_Response{Response: &remotev1.Response{RequestId: "read-text", Result: &remotev1.Response_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileResponse{Entry: &remotev1.WorkspaceEntry{RelativePath: "notes/control.txt", Name: "control.txt", Kind: remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE, SizeBytes: caps.GetMaxTextFileBytes(), ModifiedAtUnixMs: 1_800_000_000_000, Revision: strings.Repeat("r", 64), TextEditable: true}, Utf8Text: worstCaseText}}}}},
		{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "upload", Request: &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{CodexId: "c", DestinationPath: "payload.bin", Content: make([]byte, caps.GetMaxInlineUploadBytes())}}}}},
		{Payload: &remotev1.Frame_Response{Response: &remotev1.Response{RequestId: "download", Result: &remotev1.Response_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryResponse{Filename: "payload.bin", Content: make([]byte, caps.GetMaxInlineDownloadBytes())}}}}},
	}
	for _, frame := range frames {
		raw, err := protojson.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) >= maxFrameBytes {
			t.Fatalf("maximum workspace %T ProtoJSON size = %d, must conservatively fit below %d", frame.Payload, len(raw), maxFrameBytes)
		}
	}
}
