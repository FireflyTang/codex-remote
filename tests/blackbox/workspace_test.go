package blackbox_test

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func TestWorkspaceListingIsShallowAndDoesNotBlockUnrelatedRPCs(t *testing.T) {
	requireScenario(t, "workspace")
	root := filepath.Join(testWorkspace(t), "workspace-shallow-list")
	deep := filepath.Join(root, "deep")
	leaf := filepath.Join(deep, "branch", "leaf.txt")
	if err := os.MkdirAll(filepath.Dir(leaf), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leaf, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}

	setup := dial(t)
	setup.hello(t)
	created := setup.request(t, request("workspace-shallow-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{
		Cwd: root, Title: "workspace shallow listing",
	}})).GetCreateCodex()
	if created == nil || created.Codex == nil || created.Codex.CodexId == "" {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId

	// Keep the formal wire fixture small. Precise descendant-I/O detection lives
	// in the workspace package test; here we only exercise the public behavior.
	for directory := 0; directory < 3; directory++ {
		dir := filepath.Join(deep, "load", fmt.Sprintf("d-%03d", directory))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for file := 0; file < 4; file++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f-%03d", file)), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("list is shallow and bounded", func(t *testing.T) {
		quick := dial(t)
		quick.hello(t)
		response := requestWithin(t, quick, request("workspace-shallow-bounded-list", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{
			CodexId: codexID, Page: &remotev1.PageRequest{PageSize: 3},
		}}), 500*time.Millisecond)
		listed := requireListWorkspace(t, response)
		if got := workspaceEntryNames(listed.Entries); !equalStrings(got, []string{"deep"}) {
			t.Fatalf("root listing must contain only direct children: got %v", got)
		}
		if len(listed.Entries) != 1 || listed.Entries[0].Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY || listed.Entries[0].Revision != "" {
			t.Fatalf("listed directory must have an empty revision: %+v", listed.Entries)
		}
	})

	t.Run("list does not monopolize workspace or create", func(t *testing.T) {
		listClient, getClient, createClient := dial(t), dial(t), dial(t)
		listClient.hello(t)
		getClient.hello(t)
		createClient.hello(t)

		listRequest := request("workspace-shallow-concurrent-list", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{
			CodexId: codexID, Page: &remotev1.PageRequest{PageSize: 3},
		}})
		type result struct {
			response *remotev1.Response
			err      error
		}
		listed := make(chan result, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			response, err := listClient.requestContext(ctx, listRequest)
			listed <- result{response: response, err: err}
		}()
		// Let the Host enter the list handler before exercising the operations
		// that used to queue behind its global workspace mutex.
		time.Sleep(20 * time.Millisecond)

		getResponse := requestWithin(t, getClient, request("workspace-shallow-concurrent-get", &remotev1.Request_GetWorkspace{GetWorkspace: &remotev1.GetWorkspaceRequest{CodexId: codexID}}), 750*time.Millisecond)
		if getResponse.GetError() != nil || getResponse.GetGetWorkspace() == nil {
			t.Fatalf("concurrent GetWorkspace=%+v", getResponse)
		}
		otherRoot := filepath.Join(testWorkspace(t), "workspace-shallow-create-other")
		createResponse := requestWithin(t, createClient, request("workspace-shallow-concurrent-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{
			Cwd: otherRoot, CreateDirectoryIfMissing: true, Title: "workspace create while listing",
		}}), 750*time.Millisecond)
		if createResponse.GetError() != nil || createResponse.GetCreateCodex() == nil || createResponse.GetCreateCodex().Codex == nil {
			t.Fatalf("concurrent CreateCodex=%+v", createResponse)
		}

		listResult := <-listed
		if listResult.err != nil {
			t.Fatalf("ListWorkspaceEntries did not reach a terminal response: %v", listResult.err)
		}
		if listResult.response.GetError() != nil || listResult.response.GetListWorkspaceEntries() == nil {
			t.Fatalf("concurrent ListWorkspaceEntries=%+v", listResult.response)
		}
	})

	t.Run("disconnect does not poison subsequent requests", func(t *testing.T) {
		cancelled := dial(t)
		cancelled.hello(t)
		cancelled.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: request("workspace-shallow-disconnected-list", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{
			CodexId: codexID, Page: &remotev1.PageRequest{PageSize: 3},
		}})}})
		_ = cancelled.conn.CloseNow()

		after := dial(t)
		after.hello(t)
		response := requestWithin(t, after, request("workspace-shallow-after-disconnect", &remotev1.Request_GetWorkspace{GetWorkspace: &remotev1.GetWorkspaceRequest{CodexId: codexID}}), 750*time.Millisecond)
		if response.GetError() != nil || response.GetGetWorkspace() == nil {
			t.Fatalf("GetWorkspace after disconnected ListWorkspaceEntries=%+v", response)
		}
	})
}

func requestWithin(t *testing.T, c *wireClient, request *remotev1.Request, timeout time.Duration) *remotev1.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	response, err := c.requestContext(ctx, request)
	if err != nil {
		t.Fatalf("request_id=%q did not reach a terminal response within %s: %v", request.RequestId, timeout, err)
	}
	return response
}

func TestWorkspaceFormalWireScenario(t *testing.T) {
	requireScenario(t, "workspace")
	root := filepath.Join(testWorkspace(t), "workspace-formal-wire")
	for _, directory := range []string{"paged", "uploads", "oversized-dir"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		if err := os.WriteFile(filepath.Join(root, "paged", name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	c := dial(t)
	hello := c.hello(t)
	caps := hello.GetCapabilities().GetWorkspace()
	if caps == nil {
		t.Fatal("ServerHello.capabilities.workspace is absent")
	}
	if caps.MaxTextFileBytes == 0 || caps.MaxInlineUploadBytes == 0 || caps.MaxInlineDownloadBytes == 0 || caps.MaxArchiveExpandedBytes == 0 || caps.MaxArchiveEntryCount == 0 {
		t.Fatalf("workspace hard limits must all be nonzero: %+v", caps)
	}

	created := c.request(t, request("workspace-create-codex", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{
		Cwd: root, CreateDirectoryIfMissing: true, Title: "workspace formal wire",
	}})).GetCreateCodex()
	if created == nil || created.Codex == nil || created.Codex.CodexId == "" {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId

	watch := c.request(t, request("workspace-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESET || watch.ResetView == nil || watch.ResetView.WorkspaceAccessState == nil {
		t.Fatalf("initial workspace Watch=%+v", watch)
	}
	state := getWorkspace(t, c, codexID, "workspace-get-initial")
	if state.CodexId != codexID || filepath.Clean(state.WorkspaceRoot) != filepath.Clean(root) {
		t.Fatalf("GetWorkspace identity/root=%+v, want codex=%q root=%q", state, codexID, root)
	}
	requireAllowedWorkspaceState(t, state.AccessState)
	requireSameWorkspaceState(t, watch.ResetView.WorkspaceAccessState, state.AccessState)

	writeCreate := request("workspace-write-replay", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "notes.txt", Utf8Text: "created text", ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})
	written := requireWrite(t, c.request(t, writeCreate))
	if written.Entry.RelativePath != "notes.txt" || written.Entry.Revision == "" || written.Entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE {
		t.Fatalf("create WriteWorkspaceTextFile=%+v", written)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-create")
	replayedWrite := requireWrite(t, c.request(t, writeCreate))
	if !replayedWrite.Deduplicated || replayedWrite.Entry.Revision != written.Entry.Revision {
		t.Fatalf("write replay=%+v, first=%+v", replayedWrite, written)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-write-replay", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "notes.txt", Utf8Text: "different payload", ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})), remotev1.ErrorCode_ERROR_CODE_CONFLICT)

	// A fresh Watch reset, GetWorkspace and the just-emitted dedicated event
	// must expose one committed access-state snapshot.
	c2 := dial(t)
	c2.hello(t)
	watch2 := c2.request(t, request("workspace-watch-snapshot", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if watch2 == nil || watch2.ResetView == nil || watch2.ResetView.WorkspaceAccessState == nil {
		t.Fatalf("workspace snapshot Watch=%+v", watch2)
	}
	requireSameWorkspaceState(t, watch2.ResetView.WorkspaceAccessState, state.AccessState)

	read := requireRead(t, c.request(t, request("workspace-read-created", &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{CodexId: codexID, RelativePath: "notes.txt"}})))
	if read.Utf8Text != "created text" || read.Entry.Revision != written.Entry.Revision {
		t.Fatalf("ReadWorkspaceTextFile=%+v", read)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-create-existing", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "notes.txt", Utf8Text: "no", ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})), remotev1.ErrorCode_ERROR_CODE_CONFLICT, remotev1.ErrorCode_ERROR_CODE_WORKSPACE_REVISION_CONFLICT)
	expectWorkspaceError(t, c.request(t, request("workspace-replace-stale", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "notes.txt", Utf8Text: "no", ExpectedRevision: "stale-revision", ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_REVISION_CONFLICT)

	replaced := requireWrite(t, c.request(t, request("workspace-replace", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "notes.txt", Utf8Text: "replaced text", ExpectedRevision: written.Entry.Revision, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY,
	}})))
	if replaced.Entry.Revision == "" || replaced.Entry.Revision == written.Entry.Revision {
		t.Fatalf("replacement did not change strong revision: before=%q after=%q", written.Entry.Revision, replaced.Entry.Revision)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-replace")

	upserted := requireWrite(t, c.request(t, request("workspace-upsert-existing", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "notes.txt", Utf8Text: "upserted text", ExpectedRevision: replaced.Entry.Revision, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_UPSERT,
	}})))
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-upsert-existing")
	if upserted.Entry.Revision == replaced.Entry.Revision {
		t.Fatalf("upsert replacement retained revision %q", upserted.Entry.Revision)
	}
	requireWrite(t, c.request(t, request("workspace-upsert-new", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "upsert-new.txt", Utf8Text: "new by upsert", ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_UPSERT,
	}})))
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-upsert-new")

	firstPage := requireListWorkspace(t, c.request(t, request("workspace-list-page-1", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{
		CodexId: codexID, RelativeDirectory: "paged", Page: &remotev1.PageRequest{PageSize: 2},
	}})))
	if len(firstPage.Entries) != 2 || firstPage.Page == nil || firstPage.Page.NextPageToken == "" {
		t.Fatalf("first workspace page=%+v", firstPage)
	}
	secondPage := requireListWorkspace(t, c.request(t, request("workspace-list-page-2", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{
		CodexId: codexID, RelativeDirectory: "paged", Page: &remotev1.PageRequest{PageSize: 2, PageToken: firstPage.Page.NextPageToken},
	}})))
	if len(secondPage.Entries) != 2 || secondPage.Page == nil || secondPage.Page.NextPageToken != "" {
		t.Fatalf("second workspace page=%+v", secondPage)
	}
	names := workspaceEntryNames(append(append([]*remotev1.WorkspaceEntry{}, firstPage.Entries...), secondPage.Entries...))
	if got, want := names, []string{"a.txt", "b.txt", "c.txt", "d.txt"}; !equalStrings(got, want) {
		t.Fatalf("paged workspace names=%v, want %v", got, want)
	}
	for _, entry := range append(append([]*remotev1.WorkspaceEntry{}, firstPage.Entries...), secondPage.Entries...) {
		if entry == nil || entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE || entry.Revision == "" {
			t.Fatalf("listed regular file must have a non-empty revision: %+v", entry)
		}
	}

	regularContent := []byte{0x00, 0x01, 0xff, 0x02}
	uploadRegular := request("workspace-upload-replay", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "uploads/blob.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE,
		Content: regularContent, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})
	uploaded := requireUpload(t, c.request(t, uploadRegular))
	if uploaded.Entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE || uploaded.Entry.Revision == "" {
		t.Fatalf("regular upload must return a non-empty revision: %+v", uploaded)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-upload")
	replayedUpload := requireUpload(t, c.request(t, uploadRegular))
	if !replayedUpload.Deduplicated || replayedUpload.Entry.Revision != uploaded.Entry.Revision {
		t.Fatalf("upload replay=%+v, first=%+v", replayedUpload, uploaded)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-upload-replay", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "uploads/other.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE,
		Content: []byte("different"), ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_CONFLICT)

	replacementContent := []byte{0x03, 0x04, 0xfe, 0x05}
	replacedRegular := requireUpload(t, c.request(t, request("workspace-upload-replace-regular", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "uploads/blob.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE,
		Content: replacementContent, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})))
	if replacedRegular.Entry.Revision == "" || replacedRegular.Entry.Revision == uploaded.Entry.Revision {
		t.Fatalf("same-kind regular upload did not replace the target: before=%+v after=%+v", uploaded.Entry, replacedRegular.Entry)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-regular-replace")

	archive := makeWorkspaceZIP(t, map[string][]byte{"top.txt": []byte("top"), "nested/inside.txt": []byte("inside")}, []string{"empty/"})
	replacedWithDirectory := requireUpload(t, c.request(t, request("workspace-upload-regular-to-directory", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "uploads/blob.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY,
		Content: archive, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})))
	if replacedWithDirectory.Entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY || replacedWithDirectory.Entry.Revision != "" {
		t.Fatalf("regular-to-directory replacement=%+v", replacedWithDirectory)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-regular-to-directory")
	downloadedReplacementDirectory := requireDownload(t, c.request(t, request("workspace-download-replaced-directory", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "uploads/blob.bin"}})))
	if downloadedReplacementDirectory.Kind != remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_ZIP_DIRECTORY || downloadedReplacementDirectory.Entry.Revision != "" {
		t.Fatalf("replaced directory download=%+v", downloadedReplacementDirectory)
	}

	finalRegularContent := []byte{0x06, 0x07, 0xfd, 0x08}
	replacedWithRegular := requireUpload(t, c.request(t, request("workspace-upload-directory-to-regular", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "uploads/blob.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE,
		Content: finalRegularContent, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})))
	if replacedWithRegular.Entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE || replacedWithRegular.Entry.Revision == "" {
		t.Fatalf("directory-to-regular replacement=%+v", replacedWithRegular)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-directory-to-regular")
	downloaded := requireDownload(t, c.request(t, request("workspace-download-regular", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "uploads/blob.bin"}})))
	if downloaded.Kind != remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_REGULAR_FILE || downloaded.Filename != "blob.bin" || !bytes.Equal(downloaded.Content, finalRegularContent) || downloaded.Entry.Revision != replacedWithRegular.Entry.Revision {
		t.Fatalf("regular DownloadWorkspaceEntry=%+v", downloaded)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-read-binary", &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{CodexId: codexID, RelativePath: "uploads/blob.bin"}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_TEXT_NOT_UTF8)

	uploadedDirectory := requireUpload(t, c.request(t, request("workspace-upload-zip", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "archive", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY,
		Content: archive, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})))
	if uploadedDirectory.Entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY || uploadedDirectory.Entry.Revision != "" {
		t.Fatalf("ZIP directory upload=%+v", uploadedDirectory)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-zip")
	replacementArchive := makeWorkspaceZIP(t, map[string][]byte{"replacement.txt": []byte("replacement")}, nil)
	replacedDirectory := requireUpload(t, c.request(t, request("workspace-upload-replace-zip", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "archive", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY,
		Content: replacementArchive, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})))
	if replacedDirectory.Entry.Kind != remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY || replacedDirectory.Entry.Revision != "" {
		t.Fatalf("same-kind ZIP directory replacement=%+v", replacedDirectory)
	}
	state.AccessState = syncWorkspaceMutation(t, c, codexID, state.AccessState.Generation, "workspace-get-after-zip-replace")
	downloadedDirectory := requireDownload(t, c.request(t, request("workspace-download-zip", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "archive"}})))
	if downloadedDirectory.Kind != remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_ZIP_DIRECTORY || downloadedDirectory.Filename == "" || downloadedDirectory.Entry.Revision != "" {
		t.Fatalf("ZIP directory download=%+v", downloadedDirectory)
	}
	archiveFiles := readWorkspaceZIP(t, downloadedDirectory.Content)
	if len(archiveFiles) != 1 || !bytes.Equal(archiveFiles["replacement.txt"], []byte("replacement")) {
		t.Fatalf("downloaded ZIP contents=%q", archiveFiles)
	}

	expectWorkspaceError(t, c.request(t, request("workspace-invalid-archive", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "invalid-archive", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY,
		Content: []byte("not a zip"), ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID)
	traversingZIP := makeWorkspaceZIP(t, map[string][]byte{"../escape.txt": []byte("escape")}, nil)
	expectWorkspaceError(t, c.request(t, request("workspace-traversing-archive", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "traversing-archive", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY,
		Content: traversingZIP, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID, remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID, remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_OUTSIDE_ROOT)
	expectWorkspaceError(t, c.request(t, request("workspace-invalid-path", &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{CodexId: codexID, RelativePath: "../outside"}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID, remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_OUTSIDE_ROOT)
	expectWorkspaceError(t, c.request(t, request("workspace-root-zip-target", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY, Content: archive,
		ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID)
	expectWorkspaceError(t, c.request(t, request("workspace-missing-parent", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "missing/child.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE, Content: []byte("x"),
		ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND)
	if err := os.Symlink("blob.bin", filepath.Join(root, "uploads", "upload-link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(root, "uploads", "upload-fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	for requestID, destination := range map[string]string{"workspace-upload-reject-symlink": "uploads/upload-link", "workspace-upload-reject-special": "uploads/upload-fifo"} {
		expectWorkspaceError(t, c.request(t, request(requestID, &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
			CodexId: codexID, DestinationPath: destination, Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE,
			Content: []byte("refused"), ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
		}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED)
	}

	if caps.MaxTextFileBytes > 32<<20 || caps.MaxInlineUploadBytes > 32<<20 || caps.MaxInlineDownloadBytes > 32<<20 || caps.MaxArchiveExpandedBytes > 32<<20 {
		t.Fatalf("black-box hard-limit fixture refuses unexpectedly large advertised caps: %+v", caps)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-upload-too-large", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "uploads/too-large.bin", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE,
		Content: make([]byte, int(caps.MaxInlineUploadBytes)+1), ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_UPLOAD_TOO_LARGE)
	if err := os.WriteFile(filepath.Join(root, "too-large.txt"), make([]byte, int(caps.MaxTextFileBytes)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-text-too-large", &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{CodexId: codexID, RelativePath: "too-large.txt"}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_TEXT_TOO_LARGE)
	if err := os.WriteFile(filepath.Join(root, "download-too-large.bin"), make([]byte, int(caps.MaxInlineDownloadBytes)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-download-too-large", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "download-too-large.bin"}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_DOWNLOAD_TOO_LARGE)
	expandedZIP := makeWorkspaceZIP(t, map[string][]byte{"expanded.bin": make([]byte, int(caps.MaxArchiveExpandedBytes)+1)}, nil)
	expectWorkspaceError(t, c.request(t, request("workspace-expanded-too-large", &remotev1.Request_UploadWorkspaceEntry{UploadWorkspaceEntry: &remotev1.UploadWorkspaceEntryRequest{
		CodexId: codexID, DestinationPath: "expanded-too-large", Kind: remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY,
		Content: expandedZIP, ExpectedQuiescenceToken: state.AccessState.QuiescenceToken,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_EXPANDED_TOO_LARGE)
	if err := os.WriteFile(filepath.Join(root, "oversized-dir", "expanded.bin"), make([]byte, int(caps.MaxArchiveExpandedBytes)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	expectWorkspaceError(t, c.request(t, request("workspace-directory-download-too-large", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "oversized-dir"}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_EXPANDED_TOO_LARGE)

	beforeBusy := state.AccessState
	started := c.request(t, request("workspace-start-turn", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "hold workspace busy"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("workspace StartTurn=%+v", started)
	}
	busy := awaitWorkspaceActiveCount(t, c, codexID, beforeBusy.Generation, 1)
	requireSameWorkspaceState(t, busy, getWorkspace(t, c, codexID, "workspace-get-busy").AccessState)
	firstChildRegistered := awaitWorkspaceActiveCount(t, c, codexID, busy.Generation, 2)
	withChildren := awaitWorkspaceActiveCount(t, c, codexID, firstChildRegistered.Generation, 3)
	oneChildTerminal := awaitWorkspaceActiveCount(t, c, codexID, withChildren.Generation, 2)
	busy = awaitWorkspaceActiveCount(t, c, codexID, oneChildTerminal.Generation, 1)
	requireSameWorkspaceState(t, busy, getWorkspace(t, c, codexID, "workspace-get-after-subagents").AccessState)
	expectWorkspaceError(t, c.request(t, request("workspace-write-busy", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "busy.txt", Utf8Text: "blocked", ExpectedQuiescenceToken: beforeBusy.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_BUSY)
	// Read-only operations remain usable while the parent agent is active.
	requireRead(t, c.request(t, request("workspace-read-busy", &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{CodexId: codexID, RelativePath: "notes.txt"}})))
	requireListWorkspace(t, c.request(t, request("workspace-list-busy", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{CodexId: codexID, RelativeDirectory: "paged", Page: &remotev1.PageRequest{PageSize: 1}}})))
	requireDownload(t, c.request(t, request("workspace-download-busy", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "uploads/blob.bin"}})))

	interrupted := c.request(t, request("workspace-interrupt", &remotev1.Request_InterruptTurn{InterruptTurn: &remotev1.InterruptTurnRequest{CodexId: codexID, TurnId: started.TurnId}})).GetInterruptTurn()
	if interrupted == nil {
		t.Fatal("workspace InterruptTurn returned no result")
	}
	afterTerminal := awaitWorkspaceState(t, c, codexID, busy.Generation)
	requireAllowedWorkspaceState(t, afterTerminal)
	if afterTerminal.QuiescenceToken == beforeBusy.QuiescenceToken {
		t.Fatalf("terminal state reused pre-turn quiescence token %q", afterTerminal.QuiescenceToken)
	}
	requireSameWorkspaceState(t, afterTerminal, getWorkspace(t, c, codexID, "workspace-get-terminal").AccessState)
	expectWorkspaceError(t, c.request(t, request("workspace-stale-token", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "stale-token.txt", Utf8Text: "blocked", ExpectedQuiescenceToken: beforeBusy.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})), remotev1.ErrorCode_ERROR_CODE_WORKSPACE_BUSY)
	requireWrite(t, c.request(t, request("workspace-new-token", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "after-terminal.txt", Utf8Text: "allowed", ExpectedQuiescenceToken: afterTerminal.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})))
	afterTerminal = syncWorkspaceMutation(t, c, codexID, afterTerminal.Generation, "workspace-get-after-terminal-write")

	unmanaged := c.request(t, request("workspace-unmanage", &remotev1.Request_UnmanageCodex{UnmanageCodex: &remotev1.UnmanageCodexRequest{CodexId: codexID}})).GetUnmanageCodex()
	if unmanaged == nil || unmanaged.Codex == nil || unmanaged.Codex.CodexId != codexID || unmanaged.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		t.Fatalf("workspace UnmanageCodex=%+v", unmanaged)
	}
	unmanagedWorkspace := getWorkspace(t, c, codexID, "workspace-get-unmanaged")
	requireAllowedWorkspaceState(t, unmanagedWorkspace.AccessState)
	requireRead(t, c.request(t, request("workspace-read-unmanaged", &remotev1.Request_ReadWorkspaceTextFile{ReadWorkspaceTextFile: &remotev1.ReadWorkspaceTextFileRequest{CodexId: codexID, RelativePath: "notes.txt"}})))
	requireListWorkspace(t, c.request(t, request("workspace-list-unmanaged", &remotev1.Request_ListWorkspaceEntries{ListWorkspaceEntries: &remotev1.ListWorkspaceEntriesRequest{CodexId: codexID, RelativeDirectory: "paged", Page: &remotev1.PageRequest{PageSize: 1}}})))
	requireDownload(t, c.request(t, request("workspace-download-unmanaged", &remotev1.Request_DownloadWorkspaceEntry{DownloadWorkspaceEntry: &remotev1.DownloadWorkspaceEntryRequest{CodexId: codexID, RelativePath: "uploads/blob.bin"}})))
	requireWrite(t, c.request(t, request("workspace-write-unmanaged", &remotev1.Request_WriteWorkspaceTextFile{WriteWorkspaceTextFile: &remotev1.WriteWorkspaceTextFileRequest{
		CodexId: codexID, RelativePath: "unmanaged.txt", Utf8Text: "still accessible", ExpectedQuiescenceToken: unmanagedWorkspace.AccessState.QuiescenceToken,
		Condition: remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY,
	}})))
	syncWorkspaceMutation(t, c, codexID, unmanagedWorkspace.AccessState.Generation, "workspace-get-after-unmanaged-write")
}

func getWorkspace(t *testing.T, c *wireClient, codexID, requestID string) *remotev1.GetWorkspaceResponse {
	t.Helper()
	response := c.request(t, request(requestID, &remotev1.Request_GetWorkspace{GetWorkspace: &remotev1.GetWorkspaceRequest{CodexId: codexID}}))
	if response.GetError() != nil || response.GetGetWorkspace() == nil || response.GetGetWorkspace().AccessState == nil {
		t.Fatalf("GetWorkspace response=%+v", response)
	}
	return response.GetGetWorkspace()
}

func awaitWorkspaceState(t *testing.T, c *wireClient, codexID string, afterGeneration uint64) *remotev1.WorkspaceAccessState {
	t.Helper()
	event := c.readUntil(t, func(frame *remotev1.Frame) bool {
		value := frame.GetEvent()
		return value != nil && value.CodexId == codexID && value.GetWorkspaceAccessStateUpdated() != nil && value.GetWorkspaceAccessStateUpdated().AccessState != nil && value.GetWorkspaceAccessStateUpdated().AccessState.Generation > afterGeneration
	}).GetEvent()
	return event.GetWorkspaceAccessStateUpdated().AccessState
}

func awaitWorkspaceActiveCount(t *testing.T, c *wireClient, codexID string, afterGeneration uint64, want uint32) *remotev1.WorkspaceAccessState {
	t.Helper()
	state := awaitWorkspaceState(t, c, codexID, afterGeneration)
	if state.Generation != afterGeneration+1 || state.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY || state.ActiveAgentCount != want || state.QuiescenceToken != "" {
		t.Fatalf("workspace active-agent transition after generation %d: got=%+v want_count=%d", afterGeneration, state, want)
	}
	return state
}

func syncWorkspaceMutation(t *testing.T, c *wireClient, codexID string, afterGeneration uint64, requestID string) *remotev1.WorkspaceAccessState {
	t.Helper()
	eventState := awaitWorkspaceState(t, c, codexID, afterGeneration)
	got := getWorkspace(t, c, codexID, requestID).AccessState
	requireSameWorkspaceState(t, eventState, got)
	requireAllowedWorkspaceState(t, got)
	return got
}

func requireAllowedWorkspaceState(t *testing.T, state *remotev1.WorkspaceAccessState) {
	t.Helper()
	if state == nil || state.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED || state.ActiveAgentCount != 0 || state.QuiescenceToken == "" || state.ObservedAtUnixMs <= 0 || state.Generation == 0 {
		t.Fatalf("workspace state is not a valid ALLOWED snapshot: %+v", state)
	}
}

func requireSameWorkspaceState(t *testing.T, got, want *remotev1.WorkspaceAccessState) {
	t.Helper()
	if got == nil || want == nil || got.MutationStatus != want.MutationStatus || got.ActiveAgentCount != want.ActiveAgentCount || got.QuiescenceToken != want.QuiescenceToken || got.Generation != want.Generation {
		t.Fatalf("workspace access state mismatch: got=%+v want=%+v", got, want)
	}
}

func requireWrite(t *testing.T, response *remotev1.Response) *remotev1.WriteWorkspaceTextFileResponse {
	t.Helper()
	if response.GetError() != nil || response.GetWriteWorkspaceTextFile() == nil || response.GetWriteWorkspaceTextFile().Entry == nil {
		t.Fatalf("WriteWorkspaceTextFile response=%+v", response)
	}
	return response.GetWriteWorkspaceTextFile()
}

func requireUpload(t *testing.T, response *remotev1.Response) *remotev1.UploadWorkspaceEntryResponse {
	t.Helper()
	if response.GetError() != nil || response.GetUploadWorkspaceEntry() == nil || response.GetUploadWorkspaceEntry().Entry == nil {
		t.Fatalf("UploadWorkspaceEntry response=%+v", response)
	}
	return response.GetUploadWorkspaceEntry()
}

func requireDownload(t *testing.T, response *remotev1.Response) *remotev1.DownloadWorkspaceEntryResponse {
	t.Helper()
	if response.GetError() != nil || response.GetDownloadWorkspaceEntry() == nil || response.GetDownloadWorkspaceEntry().Entry == nil {
		t.Fatalf("DownloadWorkspaceEntry response=%+v", response)
	}
	return response.GetDownloadWorkspaceEntry()
}

func requireRead(t *testing.T, response *remotev1.Response) *remotev1.ReadWorkspaceTextFileResponse {
	t.Helper()
	if response.GetError() != nil || response.GetReadWorkspaceTextFile() == nil || response.GetReadWorkspaceTextFile().Entry == nil {
		t.Fatalf("ReadWorkspaceTextFile response=%+v", response)
	}
	return response.GetReadWorkspaceTextFile()
}

func requireListWorkspace(t *testing.T, response *remotev1.Response) *remotev1.ListWorkspaceEntriesResponse {
	t.Helper()
	if response.GetError() != nil || response.GetListWorkspaceEntries() == nil {
		t.Fatalf("ListWorkspaceEntries response=%+v", response)
	}
	return response.GetListWorkspaceEntries()
}

func expectWorkspaceError(t *testing.T, response *remotev1.Response, codes ...remotev1.ErrorCode) {
	t.Helper()
	if response.GetError() == nil {
		t.Fatalf("response=%+v, want one of workspace errors %v", response, codes)
	}
	for _, code := range codes {
		if response.GetError().Code == code {
			return
		}
	}
	t.Fatalf("workspace error=%v, want one of %v (response=%+v)", response.GetError().Code, codes, response)
}

func workspaceEntryNames(entries []*remotev1.WorkspaceEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry != nil {
			names = append(names, entry.Name)
		}
	}
	sort.Strings(names)
	return names
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func makeWorkspaceZIP(t *testing.T, files map[string][]byte, directories []string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, name := range directories {
		header := &zip.FileHeader{Name: name, Method: zip.Store}
		header.SetMode(0o700 | os.ModeDir)
		if _, err := writer.CreateHeader(header); err != nil {
			t.Fatal(err)
		}
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetMode(0o600)
		entry, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(files[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func readWorkspaceZIP(t *testing.T, raw []byte) map[string][]byte {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("open downloaded ZIP: %v", err)
	}
	result := make(map[string][]byte)
	for _, entry := range reader.File {
		if entry.FileInfo().IsDir() {
			continue
		}
		file, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		result[entry.Name] = content
	}
	return result
}
