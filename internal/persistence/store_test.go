package persistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequestDedupPersistsAndDoesNotReplayInProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := s.BeginRequest(ctx, "req-1", "start_turn", []byte("a"))
	if err != nil || r.State != DedupStarted {
		t.Fatalf("first: %+v %v", r, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.BeginRequest(ctx, "req-1", "start_turn", []byte("a"))
	if !errors.Is(err, ErrRequestInProgress) {
		t.Fatalf("want in progress, got %v", err)
	}
	_, err = s.BeginRequest(ctx, "req-1", "start_turn", []byte("b"))
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	if err := s.CompleteRequest(ctx, "req-1", []byte(`{"response":1}`)); err != nil {
		t.Fatal(err)
	}
	r, err = s.BeginRequest(ctx, "req-1", "start_turn", []byte("a"))
	if err != nil || r.State != DedupCompleted {
		t.Fatalf("completed: %+v %v", r, err)
	}
}

func TestCodexAndEventHeadPersist(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	rec := CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Title: "x", Origin: "remote", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 2}
	if err := s.UpsertCodex(ctx, rec); err != nil {
		t.Fatal(err)
	}
	for want := uint64(1); want <= 2; want++ {
		got, err := s.NextEventSequence(ctx, "c")
		if err != nil || got != want {
			t.Fatalf("seq got=%d err=%v", got, err)
		}
	}
	got, err := s.GetCodexByThread(ctx, "t")
	if err != nil || got.CodexID != "c" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestLifecycleColumnsPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record := CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Origin: "remote", Status: "idle", ManagementState: "MANAGEMENT_STATE_EXPIRING_SOON", ManagedUntilUnixMS: 1234, WarningDeadlineUnixMS: 1234, CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}
	if err = s.UpsertCodex(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetCodex(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if got.ManagementState != record.ManagementState || got.ManagedUntilUnixMS != 1234 || got.WarningDeadlineUnixMS != 1234 {
		t.Fatalf("lifecycle record=%+v", got)
	}
}

func TestLifecycleMigrationPreservesV10CodexAsUnspecified(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE codexes (
 codex_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, session_source TEXT NOT NULL DEFAULT 'unknown', cwd TEXT NOT NULL,
 title TEXT NOT NULL, origin TEXT NOT NULL, status TEXT NOT NULL,
 active_turn_id TEXT NOT NULL DEFAULT '', runtime_version TEXT NOT NULL DEFAULT '',
 created_at_ms INTEGER NOT NULL, imported_at_ms INTEGER NOT NULL DEFAULT 0,
 last_activity_at_ms INTEGER NOT NULL, current_view_json BLOB,
 UNIQUE(session_source, thread_id));
 INSERT INTO codexes(codex_id,thread_id,cwd,title,origin,status,created_at_ms,last_activity_at_ms)
 VALUES('legacy','thread','/tmp','legacy title','remote','idle',1,1);`)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record, err := s.GetCodex(context.Background(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if record.Title != "legacy title" || record.ManagementState != "" || record.ManagedUntilUnixMS != 0 || record.WarningDeadlineUnixMS != 0 {
		t.Fatalf("migrated legacy record=%+v", record)
	}
}

func TestCommitEventPersistsHeadAndViewAtomicallyAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertCodex(ctx, CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	seq, err := s.CommitEvent(ctx, "c", func(seq uint64) ([]byte, error) {
		return []byte(`{"headEventSeq":"1","codex":{"codexId":"c"}}`), nil
	})
	if err != nil || seq != 1 {
		t.Fatalf("CommitEvent seq=%d err=%v", seq, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, view, err := s.LoadEventState(ctx, "c")
	if err != nil || head != 1 || string(view) != `{"headEventSeq":"1","codex":{"codexId":"c"}}` {
		t.Fatalf("LoadEventState head=%d view=%s err=%v", head, view, err)
	}
}

func TestCommitEventViewFailureRollsBackHeadAndViewAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertCodex(ctx, CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitEvent(ctx, "c", func(uint64) ([]byte, error) { return []byte(`{"headEventSeq":"1"}`), nil }); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected CurrentView write failure")
	seenSeq := uint64(0)
	if _, err = s.CommitEvent(ctx, "c", func(seq uint64) ([]byte, error) {
		seenSeq = seq
		return nil, injected
	}); !errors.Is(err, injected) || seenSeq != 2 {
		t.Fatalf("failed CommitEvent seq=%d err=%v", seenSeq, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, view, err := s.LoadEventState(ctx, "c")
	if err != nil || head != 1 || string(view) != `{"headEventSeq":"1"}` {
		t.Fatalf("after rollback head=%d view=%s err=%v", head, view, err)
	}
	seq, err := s.CommitEvent(ctx, "c", func(seq uint64) ([]byte, error) { return []byte(`{"headEventSeq":"2"}`), nil })
	if err != nil || seq != 2 {
		t.Fatalf("next CommitEvent seq=%d err=%v", seq, err)
	}
}

func TestCommitEventSQLiteViewUpdateFailureRollsBackSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertCodex(ctx, CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitEvent(ctx, "c", func(uint64) ([]byte, error) { return []byte(`{"headEventSeq":"1"}`), nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(ctx, `CREATE TRIGGER fail_event_view BEFORE UPDATE OF current_view_json ON codexes BEGIN SELECT RAISE(ABORT,'injected view update failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitEvent(ctx, "c", func(uint64) ([]byte, error) { return []byte(`{"headEventSeq":"2"}`), nil }); err == nil || !strings.Contains(err.Error(), "injected view update failure") {
		t.Fatalf("CommitEvent error=%v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, view, err := s.LoadEventState(ctx, "c")
	if err != nil || head != 1 || string(view) != `{"headEventSeq":"1"}` {
		t.Fatalf("after SQLite failure head=%d view=%s err=%v", head, view, err)
	}
}

func TestCommitEventUnknownCodexDoesNotCreateEventHead(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.CommitEvent(context.Background(), "missing", func(uint64) ([]byte, error) { return []byte(`{}`), nil }); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("CommitEvent error=%v, want sql.ErrNoRows", err)
	}
	head, err := s.EventHead(context.Background(), "missing")
	if err != nil || head != 0 {
		t.Fatalf("phantom event head=%d err=%v", head, err)
	}
}

func TestSessionIdentityIncludesSource(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for _, rec := range []CodexRecord{
		{CodexID: "cli", ThreadID: "same", SessionSource: "cli", CWD: "/tmp", Origin: "local", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1},
		{CodexID: "vscode", ThreadID: "same", SessionSource: "vscode", CWD: "/tmp", Origin: "local", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1},
	} {
		if err := s.UpsertCodex(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	cli, err := s.GetCodexBySession(ctx, "cli", "same")
	if err != nil || cli.CodexID != "cli" {
		t.Fatalf("cli=%+v err=%v", cli, err)
	}
	vscode, err := s.GetCodexBySession(ctx, "vscode", "same")
	if err != nil || vscode.CodexID != "vscode" {
		t.Fatalf("vscode=%+v err=%v", vscode, err)
	}
}

func TestResolvedPendingTombstonePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = s.SaveResolvedPending(ctx, "c", "p", "approval", []byte(`{"done":true}`)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	kind, raw, err := s.ResolvedPending(ctx, "c", "p")
	if err != nil || kind != "approval" || string(raw) != `{"done":true}` {
		t.Fatalf("kind=%q raw=%s err=%v", kind, raw, err)
	}
}

func TestManualTitleAndForgetRemoveCodexStateButKeepDedup(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record := CodexRecord{CodexID: "forget-me", ThreadID: "thread", CWD: "/tmp", Title: "manual", Origin: "remote", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1, ManualTitleOverride: true}
	if err = s.UpsertCodex(ctx, record); err != nil {
		t.Fatal(err)
	}
	if got, getErr := s.GetCodex(ctx, record.CodexID); getErr != nil || !got.ManualTitleOverride {
		t.Fatalf("manual override=%v err=%v", got.ManualTitleOverride, getErr)
	}
	if _, err = s.NextEventSequence(ctx, record.CodexID); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveResolvedPending(ctx, record.CodexID, "pending", "approval", []byte(`{"resolved":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginRequest(ctx, "request", "ForgetCodex", []byte("fingerprint")); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteRequest(ctx, "request", []byte(`{"forgotten":true}`)); err != nil {
		t.Fatal(err)
	}
	forgotten := ForgottenSessionRecord{Source: "appServer", SessionID: "thread", CWD: "/tmp", Title: "candidate", Origin: "CODEX_ORIGIN_REMOTE_CREATED", CreatedAtUnixMS: 1, UpdatedAtUnixMS: 2}
	if err = s.UpsertForgottenSession(ctx, forgotten); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCodex(ctx, record.CodexID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetCodex(ctx, record.CodexID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetCodex after delete err=%v", err)
	}
	if head, headErr := s.EventHead(ctx, record.CodexID); headErr != nil || head != 0 {
		t.Fatalf("event head=%d err=%v", head, headErr)
	}
	if _, _, err = s.ResolvedPending(ctx, record.CodexID, "pending"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("resolved pending err=%v", err)
	}
	if got, dedupErr := s.BeginRequest(ctx, "request", "ForgetCodex", []byte("fingerprint")); dedupErr != nil || got.State != DedupCompleted {
		t.Fatalf("dedup state=%v err=%v", got.State, dedupErr)
	}
	if got, getErr := s.GetForgottenSession(ctx, forgotten.Source, forgotten.SessionID); getErr != nil || got.Title != forgotten.Title || got.Materialized {
		t.Fatalf("forgotten candidate=%+v err=%v", got, getErr)
	}
	listed, listErr := s.ListForgottenSessions(ctx, forgotten.CWD)
	if listErr != nil || len(listed) != 1 || listed[0].SessionID != forgotten.SessionID {
		t.Fatalf("forgotten list=%+v err=%v", listed, listErr)
	}
	if err = s.DeleteForgottenSession(ctx, forgotten.Source, forgotten.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetForgottenSession(ctx, forgotten.Source, forgotten.SessionID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("forgotten candidate after consume err=%v", err)
	}
}
