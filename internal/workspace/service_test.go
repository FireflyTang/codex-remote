package workspace

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
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
		if _, _, err := service.ReadText(context.Background(), "c", invalid); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID {
			t.Fatalf("path %q err=%v", invalid, err)
		}
	}
	if _, _, err := service.ReadText(context.Background(), "c", "escape"); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED {
		t.Fatalf("symlink err=%v", err)
	}
	entries, _, err := service.List(context.Background(), "c", "", 0, 100)
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
	if _, _, err := service.List(context.Background(), "c", "", 0, 100); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND {
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
	entry, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, content, state.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY {
		t.Fatalf("entry=%+v", entry)
	}
	if entry.Revision != "" {
		t.Fatalf("directory revision=%q, want empty", entry.Revision)
	}
	if got, err := os.ReadFile(filepath.Join(root, "tree", "sub", "b.txt")); err != nil || string(got) != "bb" {
		t.Fatalf("extracted=%q err=%v", got, err)
	}
	_, kind, filename, downloaded, err := service.Download(context.Background(), "c", "tree")
	if err != nil {
		t.Fatal(err)
	}
	if kind != remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_ZIP_DIRECTORY || filename != "tree.zip" || len(downloaded) == 0 {
		t.Fatalf("download kind=%v filename=%q bytes=%d", kind, filename, len(downloaded))
	}
	quiet, _, _ := service.State("c")
	bad := makeZIP(t, map[string]string{"../escape": "x"})
	if _, err := service.Upload(context.Background(), "c", "bad", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, bad, quiet.QuiescenceToken); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID {
		t.Fatalf("bad archive=%v", err)
	}
	large := makeZIP(t, map[string]string{"large": "012345678901234567890123456789012"})
	if _, err := service.Upload(context.Background(), "c", "large", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, large, quiet.QuiescenceToken); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_EXPANDED_TOO_LARGE {
		t.Fatalf("large archive=%v", err)
	}
}

func TestUploadUnconditionallyCreatesAndReplacesRegularAndDirectory(t *testing.T) {
	root := t.TempDir()
	service, state := testService(t, root, Config{})
	entry, err := service.Upload(context.Background(), "c", "target", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE, []byte("one"), state.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Revision == "" || entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE {
		t.Fatalf("regular entry=%+v", entry)
	}
	quiet, _, _ := service.State("c")
	directory := makeZIP(t, map[string]string{"inside.txt": "inside"})
	entry, err = service.Upload(context.Background(), "c", "target", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, directory, quiet.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY || entry.Revision != "" {
		t.Fatalf("directory entry=%+v", entry)
	}
	quiet, _, _ = service.State("c")
	entry, err = service.Upload(context.Background(), "c", "target", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE, []byte("two"), quiet.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(filepath.Join(root, "target")); err != nil || string(content) != "two" {
		t.Fatalf("cross-kind replacement content=%q err=%v", content, err)
	}
	quiet, _, _ = service.State("c")
	if err := os.Symlink("target", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Upload(context.Background(), "c", "link", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE, []byte("no"), quiet.QuiescenceToken); errorCode(t, err) != remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED {
		t.Fatalf("symlink upload error=%v", err)
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
	_, err = service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, originalZIP, quiet.QuiescenceToken)
	if err != nil {
		t.Fatal(err)
	}
	beforeZIPFailure, _, _ := service.State("c")
	service.SetStateSink(func(context.Context, string, *remotev1.WorkspaceAccessState) error { return injected })
	replacement := makeZIP(t, map[string]string{"new.txt": "new"})
	if _, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, replacement, beforeZIPFailure.QuiescenceToken); !errors.Is(err, injected) {
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
	service.SetStateSink(nil)
	if _, err := service.Upload(context.Background(), "c", "tree", remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, replacement, beforeZIPFailure.QuiescenceToken); err != nil {
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

func TestListPaginatesBeforeMetadataAndDoesNotInspectDescendants(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name, "deep.txt"), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	service, _ := testService(t, root, Config{})
	var visited []string
	service.beforeListEntryTest = func(relative string) { visited = append(visited, relative) }
	entries, total, err := service.List(context.Background(), "c", "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(entries) != 1 || entries[0].Name != "b" {
		t.Fatalf("total=%d entries=%+v", total, entries)
	}
	if len(visited) != 1 || visited[0] != "b" {
		t.Fatalf("metadata visited before pagination: %v", visited)
	}
}

func TestListOptimisticallyMarksBoundedRegularTextFilesViewable(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "small.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "large.txt"), make([]byte, 65), 0o644); err != nil {
		t.Fatal(err)
	}
	service, _ := testService(t, root, Config{MaxTextFileBytes: 64})
	entries, _, err := service.List(context.Background(), "c", "", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*remotev1.WorkspaceEntry)
	for _, entry := range entries {
		byName[entry.Name] = entry
	}
	if !byName["small.txt"].TextViewable || !byName["small.txt"].TextEditable {
		t.Fatalf("small entry=%+v", byName["small.txt"])
	}
	if byName["large.txt"].TextViewable || byName["large.txt"].TextEditable {
		t.Fatalf("large entry=%+v", byName["large.txt"])
	}
}

func TestBlockedListDoesNotBlockStateOrAnotherWorkspaceRegistration(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(rootA, "entry"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	service, _ := testService(t, rootA, Config{})
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	service.beforeListEntryTest = func(string) {
		once.Do(func() { close(entered) })
		<-release
	}
	listDone := make(chan error, 1)
	go func() {
		_, _, err := service.List(context.Background(), "c", "", 0, 1)
		listDone <- err
	}()
	<-entered

	stateDone := make(chan error, 1)
	go func() {
		_, _, err := service.State("c")
		stateDone <- err
	}()
	registerDone := make(chan error, 1)
	go func() {
		_, err := service.Register("other", rootB, nil)
		registerDone <- err
	}()
	for name, done := range map[string]<-chan error{"state": stateDone, "register": registerDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s blocked behind unrelated filesystem listing", name)
		}
	}
	close(release)
	if err := <-listDone; err != nil {
		t.Fatal(err)
	}
}

func TestStateSinkDoesNotHoldWorkspaceRegistryOrStateLock(t *testing.T) {
	root := t.TempDir()
	otherRoot := t.TempDir()
	service, _ := testService(t, root, Config{})
	entered := make(chan struct{})
	release := make(chan struct{})
	service.SetStateSink(func(context.Context, string, *remotev1.WorkspaceAccessState) error {
		close(entered)
		<-release
		return nil
	})
	agentDone := make(chan error, 1)
	go func() { agentDone <- service.AgentStarted(context.Background(), "c", "turn") }()
	<-entered
	for name, run := range map[string]func() error{
		"state": func() error { _, _, err := service.State("c"); return err },
		"register": func() error {
			_, err := service.Register("other", otherRoot, nil)
			return err
		},
		"restore-agent": func() error {
			_, err := service.RestoreAgent("c", "restored-while-sink-blocked")
			return err
		},
	} {
		done := make(chan error, 1)
		go func() { done <- run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s blocked while state sink was running", name)
		}
	}
	close(release)
	if err := <-agentDone; err != nil {
		t.Fatal(err)
	}
}

func TestAgentStartWaitsForAdmittedMutationCommit(t *testing.T) {
	root := t.TempDir()
	service, initial := testService(t, root, Config{})
	mutationSinkEntered := make(chan struct{})
	releaseMutationSink := make(chan struct{})
	service.SetStateSink(func(_ context.Context, _ string, state *remotev1.WorkspaceAccessState) error {
		if state.ActiveAgentCount == 0 {
			close(mutationSinkEntered)
			<-releaseMutationSink
		}
		return nil
	})
	writeDone := make(chan error, 1)
	go func() {
		_, err := service.WriteText(context.Background(), "c", "note.txt", "committed", "", initial.QuiescenceToken, remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY)
		writeDone <- err
	}()
	<-mutationSinkEntered

	agentDone := make(chan error, 1)
	go func() { agentDone <- service.AgentStarted(context.Background(), "c", "turn") }()
	select {
	case err := <-agentDone:
		t.Fatalf("agent start passed pending mutation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseMutationSink)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-agentDone; err != nil {
		t.Fatal(err)
	}
	state, _, err := service.State("c")
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveAgentCount != 1 || state.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY || state.QuiescenceToken != "" {
		t.Fatalf("final state=%+v", state)
	}
	if content, err := os.ReadFile(filepath.Join(root, "note.txt")); err != nil || string(content) != "committed" {
		t.Fatalf("mutation content=%q err=%v", content, err)
	}
}

func TestWorkspaceReadOperationsObserveCanceledContext(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	service, _ := testService(t, root, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := service.List(ctx, "c", "", 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("list error=%v", err)
	}
	if _, _, err := service.ReadText(ctx, "c", "file.txt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("read error=%v", err)
	}
	if _, _, _, _, err := service.Download(ctx, "c", "file.txt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("download error=%v", err)
	}
}
