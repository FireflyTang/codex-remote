package workspace

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/persistence"
)

func testService(t *testing.T, root string, config Config) (*Service, *remotev1.WorkspaceAccessState) {
	t.Helper()
	if config.MaxTextFileBytes == 0 {
		config.MaxTextFileBytes = 64
	}
	if config.MaxInlineUploadBytes == 0 {
		config.MaxInlineUploadBytes = 4096
	}
	if config.MaxInlineDownloadBytes == 0 {
		config.MaxInlineDownloadBytes = 4096
	}
	if config.MaxArchiveExpandedBytes == 0 {
		config.MaxArchiveExpandedBytes = 4096
	}
	if config.MaxArchiveEntryCount == 0 {
		config.MaxArchiveEntryCount = 16
	}
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	state, err := service.Register("c", root, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service, state
}

func errorCode(t *testing.T, err error) remotev1.ErrorCode {
	t.Helper()
	var workspaceErr *Error
	if !errors.As(err, &workspaceErr) {
		t.Fatalf("error=%v", err)
	}
	return workspaceErr.Code
}

func TestCanonicalPathsAndSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "ok.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	service, _ := testService(t, root, Config{})
	for _, invalid := range []string{"/abs", ".", "../x", "a/../x", "a//x", `a\x`} {
		if _, _, err := service.ReadText("c", invalid); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID {
			t.Fatalf("path %q err=%v", invalid, err)
		}
	}
	if _, _, err := service.ReadText("c", "escape"); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED {
		t.Fatalf("symlink err=%v", err)
	}
	entries, err := service.List("c", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].RelativePath != "escape" || entries[0].Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_SYMBOLIC_LINK {
		t.Fatalf("entries=%+v", entries)
	}
}

func TestRegisterMissingRootRecoversWhenDirectoryAppears(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "historical", "workspace")
	service, state := testService(t, root, Config{})
	if state.Generation == 0 || state.QuiescenceToken == "" {
		t.Fatalf("registered state=%+v", state)
	}
	if _, _, err := service.State("c"); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND {
		t.Fatalf("missing root state err=%v", err)
	}
	if _, err := service.List("c", ""); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND {
		t.Fatalf("missing root list err=%v", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	recovered, displayRoot, err := service.State("c")
	if err != nil {
		t.Fatal(err)
	}
	if displayRoot != root || recovered.QuiescenceToken != state.QuiescenceToken {
		t.Fatalf("recovered state=%+v display=%q", recovered, displayRoot)
	}
	if _, err := service.WriteText(context.Background(), "c", "ready.txt", "ready", "", recovered.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterRejectsExistingNonDirectoryRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(root, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Register("c", root, nil); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND {
		t.Fatalf("existing file root err=%v", err)
	}
}

func TestTextRevisionQuiescenceAndGeneration(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(1_800_000_000, 0)
	service, state := testService(t, root, Config{Clock: func() time.Time { return now }})
	var states []*remotev1.WorkspaceAccessState
	service.SetStateSink(func(_ context.Context, _ string, state *remotev1.WorkspaceAccessState) error {
		states = append(states, state)
		return nil
	})
	entry, err := service.WriteText(context.Background(), "c", "note.txt", "hello", "", state.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Revision == "" || !entry.TextEditable {
		t.Fatalf("entry=%+v", entry)
	}
	allowed, _, _ := service.State("c")
	if allowed.Generation != 2 || allowed.QuiescenceToken == state.QuiescenceToken {
		t.Fatalf("state=%+v", allowed)
	}
	if _, err := service.WriteText(context.Background(), "c", "note.txt", "stale", entry.Revision, state.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_BUSY {
		t.Fatalf("stale token err=%v", err)
	}
	if err := service.AgentStarted(context.Background(), "c", "turn"); err != nil {
		t.Fatal(err)
	}
	busy, _, _ := service.State("c")
	if busy.ActiveAgentCount != 1 || busy.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY || busy.QuiescenceToken != "" {
		t.Fatalf("busy=%+v", busy)
	}
	if _, err := service.WriteText(context.Background(), "c", "note.txt", "busy", entry.Revision, allowed.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_BUSY {
		t.Fatalf("busy write=%v", err)
	}
	if err := service.AgentStopped(context.Background(), "c", "turn"); err != nil {
		t.Fatal(err)
	}
	quiet, _, _ := service.State("c")
	updated, err := service.WriteText(context.Background(), "c", "note.txt", "updated", entry.Revision, quiet.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Revision == entry.Revision {
		t.Fatal("revision did not change")
	}
	if len(states) != 4 {
		t.Fatalf("state emissions=%d", len(states))
	}
}

func makeZIP(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestZIPUploadDownloadAndLimits(t *testing.T) {
	root := t.TempDir()
	service, state := testService(t, root, Config{MaxArchiveExpandedBytes: 32, MaxArchiveEntryCount: 4})
	content := makeZIP(t, map[string]string{"a.txt": "a", "sub/b.txt": "bb"})
	entry, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, content, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, "", state.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY {
		t.Fatalf("entry=%+v", entry)
	}
	if got, err := os.ReadFile(filepath.Join(root, "tree", "sub", "b.txt")); err != nil || string(got) != "bb" {
		t.Fatalf("extracted=%q err=%v", got, err)
	}
	_, kind, filename, downloaded, err := service.Download("c", "tree")
	if err != nil {
		t.Fatal(err)
	}
	if kind != remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_ZIP_DIRECTORY || filename != "tree.zip" || len(downloaded) == 0 {
		t.Fatalf("download kind=%v filename=%q bytes=%d", kind, filename, len(downloaded))
	}
	quiet, _, _ := service.State("c")
	bad := makeZIP(t, map[string]string{"../escape": "x"})
	if _, err := service.Upload(context.Background(), "c", "bad", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, bad, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, "", quiet.QuiescenceToken); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID {
		t.Fatalf("bad archive=%v", err)
	}
	large := makeZIP(t, map[string]string{"large": "012345678901234567890123456789012"})
	if _, err := service.Upload(context.Background(), "c", "large", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, large, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, "", quiet.QuiescenceToken); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_EXPANDED_TOO_LARGE {
		t.Fatalf("large archive=%v", err)
	}
}

func TestDeterministicSinkFailureRollsBackTextAndZIP(t *testing.T) {
	root := t.TempDir()
	service, initial := testService(t, root, Config{})
	injected := errors.New("injected state sink failure")
	service.SetStateSink(func(context.Context, string, *remotev1.WorkspaceAccessState) error { return injected })
	if _, err := service.WriteText(context.Background(), "c", "note.txt", "new", "", initial.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY); !errors.Is(err, injected) {
		t.Fatalf("write error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "note.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed write remained on disk: %v", err)
	}
	afterFailure, _, err := service.State("c")
	if err != nil {
		t.Fatal(err)
	}
	if afterFailure.Generation != initial.Generation || afterFailure.QuiescenceToken != initial.QuiescenceToken {
		t.Fatalf("failed write advanced state: before=%+v after=%+v", initial, afterFailure)
	}

	service.SetStateSink(nil)
	if _, err := service.WriteText(context.Background(), "c", "note.txt", "new", "", initial.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY); err != nil {
		t.Fatal(err)
	}
	quiet, _, _ := service.State("c")
	originalZIP := makeZIP(t, map[string]string{"old.txt": "old"})
	tree, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, originalZIP, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY, "", quiet.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	beforeZIPFailure, _, _ := service.State("c")
	service.SetStateSink(func(context.Context, string, *remotev1.WorkspaceAccessState) error { return injected })
	replacement := makeZIP(t, map[string]string{"new.txt": "new"})
	if _, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, replacement, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY, tree.Revision, beforeZIPFailure.QuiescenceToken); !errors.Is(err, injected) {
		t.Fatalf("ZIP replace error=%v", err)
	}
	if content, err := os.ReadFile(filepath.Join(root, "tree", "old.txt")); err != nil || string(content) != "old" {
		t.Fatalf("old ZIP tree not restored: content=%q err=%v", content, err)
	}
	if _, err := os.Stat(filepath.Join(root, "tree", "new.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed ZIP replacement remained: %v", err)
	}
	afterZIPFailure, _, _ := service.State("c")
	if afterZIPFailure.Generation != beforeZIPFailure.Generation || afterZIPFailure.QuiescenceToken != beforeZIPFailure.QuiescenceToken {
		t.Fatalf("failed ZIP advanced state: before=%+v after=%+v", beforeZIPFailure, afterZIPFailure)
	}
	entries, err := service.List("c", "")
	if err != nil {
		t.Fatal(err)
	}
	var restoredRevision string
	for _, entry := range entries {
		if entry.RelativePath == "tree" {
			restoredRevision = entry.Revision
		}
	}
	if restoredRevision == "" {
		t.Fatalf("restored tree missing from entries=%+v", entries)
	}
	service.SetStateSink(nil)
	if _, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, replacement, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY, restoredRevision, beforeZIPFailure.QuiescenceToken); err != nil {
		t.Fatalf("ZIP retry=%v", err)
	}
}

func TestUnknownSinkOutcomeKeepsMutationAndAdvancedState(t *testing.T) {
	root := t.TempDir()
	service, initial := testService(t, root, Config{})
	service.SetStateSink(func(context.Context, string, *remotev1.WorkspaceAccessState) error {
		return persistence.ErrEventCommitOutcomeUnknown
	})
	if _, err := service.WriteText(context.Background(), "c", "note.txt", "possibly committed", "", initial.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY); !errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
		t.Fatalf("write error=%v", err)
	}
	if content, err := os.ReadFile(filepath.Join(root, "note.txt")); err != nil || string(content) != "possibly committed" {
		t.Fatalf("unknown mutation was rolled back: content=%q err=%v", content, err)
	}
	after, _, err := service.State("c")
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != initial.Generation+1 || after.QuiescenceToken == initial.QuiescenceToken {
		t.Fatalf("unknown state=%+v initial=%+v", after, initial)
	}
}

func TestStrongDirectoryRevisionChangesWithContent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "dir", "x")
	if err := os.WriteFile(file, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := strongRevision(filepath.Join(root, "dir"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := strongRevision(filepath.Join(root, "dir"))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("directory revision ignored content change")
	}
}
