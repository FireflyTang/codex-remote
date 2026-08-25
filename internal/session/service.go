package session

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/directory"
)

type Adapter interface {
	ListThreads(context.Context, string, string, uint32, []string) (adapter.ThreadPage, error)
	ReadThread(context.Context, string, bool) (adapter.Thread, error)
	ResumeThread(context.Context, string) (adapter.Thread, error)
	StartThread(context.Context, string) (adapter.Thread, error)
}
type Service struct {
	Adapter     Adapter
	Directories directory.Service
	SourceKinds []string
}

var DefaultSourceKinds = []string{"cli", "vscode", "exec", "appServer", "unknown"}

func (s Service) Discover(ctx context.Context, cwd, cursor string, limit uint32) (string, adapter.ThreadPage, error) {
	p, err := s.Directories.Prepare(cwd, false)
	if err != nil {
		return "", adapter.ThreadPage{}, err
	}
	sources := s.SourceKinds
	if len(sources) == 0 {
		sources = DefaultSourceKinds
	}
	page, err := s.Adapter.ListThreads(ctx, p.Path, cursor, limit, sources)
	if err != nil {
		return p.Path, page, err
	}
	out := page.Data[:0]
	for _, t := range page.Data {
		if !normalizeIdentity(&t) {
			continue
		}
		if t.ParentThreadID != nil {
			continue
		}
		tcwd, err := filepath.Abs(filepath.Clean(t.CWD))
		if err == nil && tcwd == p.Path {
			out = append(out, t)
		}
	}
	page.Data = out
	return p.Path, page, nil
}
func (s Service) Import(ctx context.Context, id string) (adapter.Thread, error) {
	t, err := s.Adapter.ReadThread(ctx, id, true)
	if err != nil {
		return t, err
	}
	if t.ParentThreadID != nil {
		return t, fmt.Errorf("cannot import subagent thread %s", id)
	}
	if !normalizeIdentity(&t) || t.ID != id {
		return t, fmt.Errorf("session identity mismatch for %s", id)
	}
	resumed, err := s.Adapter.ResumeThread(ctx, id)
	if err != nil {
		return t, err
	}
	if len(resumed.Turns) == 0 {
		resumed.Turns = t.Turns
	}
	if !normalizeIdentity(&resumed) || resumed.ID != id {
		return resumed, fmt.Errorf("resumed session identity mismatch for %s", id)
	}
	return resumed, nil
}
func (s Service) Create(ctx context.Context, cwd string, create bool) (adapter.Thread, bool, error) {
	p, err := s.Directories.Prepare(cwd, create)
	if err != nil {
		return adapter.Thread{}, false, err
	}
	t, err := s.Adapter.StartThread(ctx, p.Path)
	if err == nil && !normalizeIdentity(&t) {
		return adapter.Thread{}, p.Created, errors.New("app-server returned an empty thread identity")
	}
	return t, p.Created, err
}

// normalizeIdentity accepts both app-server state-db threads (id) and legacy
// filesystem-discovered sessions (sessionId) as the same import identity.
func normalizeIdentity(thread *adapter.Thread) bool {
	thread.ID = strings.TrimSpace(thread.ID)
	thread.SessionID = strings.TrimSpace(thread.SessionID)
	if thread.ID == "" {
		thread.ID = thread.SessionID
	}
	if thread.SessionID == "" {
		thread.SessionID = thread.ID
	}
	return thread.ID != ""
}
