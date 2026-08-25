package session

import (
	"context"
	"testing"

	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/directory"
)

type fakeAdapter struct {
	page    adapter.ThreadPage
	read    adapter.Thread
	resumed adapter.Thread
	started adapter.Thread
	calls   []string
	sources []string
}

func (f *fakeAdapter) ListThreads(_ context.Context, cwd, cursor string, limit uint32, sources []string) (adapter.ThreadPage, error) {
	f.calls = append(f.calls, "list:"+cwd)
	f.sources = append([]string(nil), sources...)
	return f.page, nil
}
func (f *fakeAdapter) ReadThread(_ context.Context, id string, turns bool) (adapter.Thread, error) {
	f.calls = append(f.calls, "read:"+id)
	return f.read, nil
}
func (f *fakeAdapter) ResumeThread(_ context.Context, id string) (adapter.Thread, error) {
	f.calls = append(f.calls, "resume:"+id)
	return f.resumed, nil
}
func (f *fakeAdapter) StartThread(_ context.Context, cwd string) (adapter.Thread, error) {
	f.calls = append(f.calls, "start:"+cwd)
	return f.started, nil
}

func TestDiscoverFiltersExactCWDAndSubagents(t *testing.T) {
	base := t.TempDir()
	parent := "parent"
	f := &fakeAdapter{page: adapter.ThreadPage{Data: []adapter.Thread{{ID: "ok", CWD: base}, {ID: "other", CWD: base + "-other"}, {ID: "sub", CWD: base, ParentThreadID: &parent}}}}
	s := Service{Adapter: f, Directories: directory.Service{}}
	cwd, page, err := s.Discover(context.Background(), base, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if cwd != base || len(page.Data) != 1 || page.Data[0].ID != "ok" {
		t.Fatalf("cwd=%s page=%+v", cwd, page)
	}
	if len(f.sources) != len(DefaultSourceKinds) {
		t.Fatalf("sources=%v", f.sources)
	}
}
func TestDiscoverAcceptsFilesystemSessionIdentity(t *testing.T) {
	base := t.TempDir()
	f := &fakeAdapter{page: adapter.ThreadPage{Data: []adapter.Thread{{SessionID: "rollout-only", CWD: base}}}}
	_, page, err := (Service{Adapter: f, Directories: directory.Service{}}).Discover(context.Background(), base, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 1 || page.Data[0].ID != "rollout-only" || page.Data[0].SessionID != "rollout-only" {
		t.Fatalf("page=%+v", page)
	}
}
func TestImportReadsHistoryBeforeResume(t *testing.T) {
	f := &fakeAdapter{read: adapter.Thread{ID: "t", Turns: []adapter.Turn{{ID: "old"}}}, resumed: adapter.Thread{ID: "t"}}
	s := Service{Adapter: f}
	got, err := s.Import(context.Background(), "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Turns) != 1 || len(f.calls) != 2 || f.calls[0] != "read:t" || f.calls[1] != "resume:t" {
		t.Fatalf("got=%+v calls=%v", got, f.calls)
	}
}
