// Package persistence stores Remote-owned metadata and request idempotency.
package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var ErrRequestConflict = errors.New("request id was used with a different operation")
var ErrRequestInProgress = errors.New("request is in progress or its outcome is unknown")
var ErrEventCommitOutcomeUnknown = errors.New("event transaction commit outcome is unknown")

type Store struct{ db *sql.DB }

type CodexRecord struct {
	CodexID, ThreadID, SessionSource, CWD, Title, Origin, Status, ActiveTurnID, RuntimeVersion string
	CreatedAtUnixMS, ImportedAtUnixMS, LastActivityAtUnixMS                                    int64
	CurrentViewJSON                                                                            []byte
}

type DedupState int

const (
	DedupStarted DedupState = iota
	DedupCompleted
	DedupInProgress
)

type DedupResult struct {
	State        DedupState
	ResponseJSON []byte
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS codexes (
 codex_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, session_source TEXT NOT NULL DEFAULT 'unknown', cwd TEXT NOT NULL,
 title TEXT NOT NULL, origin TEXT NOT NULL, status TEXT NOT NULL,
 active_turn_id TEXT NOT NULL DEFAULT '', runtime_version TEXT NOT NULL DEFAULT '',
 created_at_ms INTEGER NOT NULL, imported_at_ms INTEGER NOT NULL DEFAULT 0,
 last_activity_at_ms INTEGER NOT NULL, current_view_json BLOB,
 UNIQUE(session_source, thread_id)
);
CREATE INDEX IF NOT EXISTS codexes_activity ON codexes(last_activity_at_ms DESC, codex_id);
CREATE TABLE IF NOT EXISTS request_dedup (
 request_id TEXT PRIMARY KEY, operation TEXT NOT NULL, fingerprint BLOB NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('IN_PROGRESS','COMPLETED')),
 response_json BLOB, created_at_ms INTEGER NOT NULL, completed_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS event_heads (
 codex_id TEXT PRIMARY KEY, head_seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS resolved_pending (
 codex_id TEXT NOT NULL, pending_id TEXT NOT NULL, kind TEXT NOT NULL,
 resolved_json BLOB NOT NULL, resolved_at_ms INTEGER NOT NULL,
 PRIMARY KEY(codex_id,pending_id)
);`)
	if err != nil {
		return err
	}
	return s.ensureSessionSourceIdentity(ctx)
}

// ensureSessionSourceIdentity upgrades the original V1 schema, whose global
// UNIQUE(thread_id) incorrectly collapsed sessions from different sources.
func (s *Store) ensureSessionSourceIdentity(ctx context.Context) error {
	var hasSource bool
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(codexes)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		hasSource = hasSource || name == "session_source"
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !hasSource {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE codexes ADD COLUMN session_source TEXT NOT NULL DEFAULT 'unknown'`); err != nil {
			return err
		}
	}

	var schema string
	if err := s.db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='codexes'`).Scan(&schema); err != nil {
		return err
	}
	if !strings.Contains(strings.ToLower(strings.Join(strings.Fields(schema), " ")), "thread_id text not null unique") {
		_, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS codexes_session_identity ON codexes(session_source,thread_id)`)
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `ALTER TABLE codexes RENAME TO codexes_legacy`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE codexes (
 codex_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, session_source TEXT NOT NULL DEFAULT 'unknown', cwd TEXT NOT NULL,
 title TEXT NOT NULL, origin TEXT NOT NULL, status TEXT NOT NULL,
 active_turn_id TEXT NOT NULL DEFAULT '', runtime_version TEXT NOT NULL DEFAULT '',
 created_at_ms INTEGER NOT NULL, imported_at_ms INTEGER NOT NULL DEFAULT 0,
 last_activity_at_ms INTEGER NOT NULL, current_view_json BLOB,
 UNIQUE(session_source,thread_id)
)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO codexes(codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json)
 SELECT codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json FROM codexes_legacy`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DROP TABLE codexes_legacy`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE INDEX codexes_activity ON codexes(last_activity_at_ms DESC,codex_id)`); err != nil {
		return err
	}
	return tx.Commit()
}

// BeginRequest reserves a side-effect before execution. An existing
// IN_PROGRESS entry is never executed again; its outcome must be reconciled.
func (s *Store) BeginRequest(ctx context.Context, requestID, operation string, fingerprint []byte) (DedupResult, error) {
	if requestID == "" || operation == "" || len(fingerprint) == 0 {
		return DedupResult{}, errors.New("request id, operation and fingerprint are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DedupResult{}, err
	}
	defer tx.Rollback()
	var oldOp, status string
	var oldFP, response []byte
	err = tx.QueryRowContext(ctx, `SELECT operation,fingerprint,status,response_json FROM request_dedup WHERE request_id=?`, requestID).Scan(&oldOp, &oldFP, &status, &response)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO request_dedup(request_id,operation,fingerprint,status,created_at_ms) VALUES(?,?,?,'IN_PROGRESS',?)`, requestID, operation, fingerprint, time.Now().UnixMilli())
		if err != nil {
			return DedupResult{}, err
		}
		if err = tx.Commit(); err != nil {
			return DedupResult{}, err
		}
		return DedupResult{State: DedupStarted}, nil
	}
	if err != nil {
		return DedupResult{}, err
	}
	if oldOp != operation || !equalBytes(oldFP, fingerprint) {
		return DedupResult{}, ErrRequestConflict
	}
	if status == "COMPLETED" {
		return DedupResult{State: DedupCompleted, ResponseJSON: append([]byte(nil), response...)}, nil
	}
	return DedupResult{State: DedupInProgress}, ErrRequestInProgress
}

func (s *Store) CompleteRequest(ctx context.Context, requestID string, responseJSON []byte) error {
	if len(responseJSON) == 0 {
		return errors.New("completed response is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE request_dedup SET status='COMPLETED',response_json=?,completed_at_ms=? WHERE request_id=? AND status='IN_PROGRESS'`, responseJSON, time.Now().UnixMilli(), requestID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("request %q is not in progress", requestID)
	}
	return nil
}

func (s *Store) UpsertCodex(ctx context.Context, r CodexRecord) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO codexes(codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json)
	 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(codex_id) DO UPDATE SET thread_id=excluded.thread_id,session_source=excluded.session_source,cwd=excluded.cwd,title=excluded.title,origin=excluded.origin,status=excluded.status,active_turn_id=excluded.active_turn_id,runtime_version=excluded.runtime_version,imported_at_ms=excluded.imported_at_ms,last_activity_at_ms=excluded.last_activity_at_ms,current_view_json=excluded.current_view_json`,
		r.CodexID, r.ThreadID, normalizeSessionSource(r.SessionSource), r.CWD, r.Title, r.Origin, r.Status, r.ActiveTurnID, r.RuntimeVersion, r.CreatedAtUnixMS, r.ImportedAtUnixMS, r.LastActivityAtUnixMS, nullableBytes(r.CurrentViewJSON))
	return err
}

func (s *Store) GetCodex(ctx context.Context, id string) (CodexRecord, error) {
	return s.getCodex(ctx, `SELECT codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json FROM codexes WHERE codex_id=?`, id)
}

func (s *Store) GetCodexByThread(ctx context.Context, threadID string) (CodexRecord, error) {
	return s.getCodex(ctx, `SELECT codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json FROM codexes WHERE thread_id=? ORDER BY last_activity_at_ms DESC LIMIT 1`, threadID)
}

func (s *Store) GetCodexBySession(ctx context.Context, source, sessionID string) (CodexRecord, error) {
	return s.getCodex2(ctx, `SELECT codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json FROM codexes WHERE session_source=? AND thread_id=?`, normalizeSessionSource(source), sessionID)
}

func (s *Store) ListCodexes(ctx context.Context, limit int, offset int) ([]CodexRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT codex_id,thread_id,session_source,cwd,title,origin,status,active_turn_id,runtime_version,created_at_ms,imported_at_ms,last_activity_at_ms,current_view_json FROM codexes ORDER BY last_activity_at_ms DESC,codex_id LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CodexRecord
	for rows.Next() {
		var r CodexRecord
		if err := rows.Scan(&r.CodexID, &r.ThreadID, &r.SessionSource, &r.CWD, &r.Title, &r.Origin, &r.Status, &r.ActiveTurnID, &r.RuntimeVersion, &r.CreatedAtUnixMS, &r.ImportedAtUnixMS, &r.LastActivityAtUnixMS, &r.CurrentViewJSON); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) getCodex(ctx context.Context, query, arg string) (CodexRecord, error) {
	var r CodexRecord
	err := s.db.QueryRowContext(ctx, query, arg).Scan(&r.CodexID, &r.ThreadID, &r.SessionSource, &r.CWD, &r.Title, &r.Origin, &r.Status, &r.ActiveTurnID, &r.RuntimeVersion, &r.CreatedAtUnixMS, &r.ImportedAtUnixMS, &r.LastActivityAtUnixMS, &r.CurrentViewJSON)
	return r, err
}

func (s *Store) getCodex2(ctx context.Context, query, arg1, arg2 string) (CodexRecord, error) {
	var r CodexRecord
	err := s.db.QueryRowContext(ctx, query, arg1, arg2).Scan(&r.CodexID, &r.ThreadID, &r.SessionSource, &r.CWD, &r.Title, &r.Origin, &r.Status, &r.ActiveTurnID, &r.RuntimeVersion, &r.CreatedAtUnixMS, &r.ImportedAtUnixMS, &r.LastActivityAtUnixMS, &r.CurrentViewJSON)
	return r, err
}

func (s *Store) SetCurrentView(ctx context.Context, codexID string, viewJSON []byte) error {
	res, err := s.db.ExecContext(ctx, `UPDATE codexes SET current_view_json=?,last_activity_at_ms=? WHERE codex_id=?`, viewJSON, time.Now().UnixMilli(), codexID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) CurrentView(ctx context.Context, codexID string) ([]byte, error) {
	var b []byte
	err := s.db.QueryRowContext(ctx, `SELECT current_view_json FROM codexes WHERE codex_id=?`, codexID).Scan(&b)
	return b, err
}

// LoadEventState reads the durable event boundary and CurrentView in one
// SQLite statement, so a newly created Activity Store cannot observe values
// from different transactions.
func (s *Store) LoadEventState(ctx context.Context, codexID string) (uint64, []byte, error) {
	var head uint64
	var view []byte
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE((SELECT head_seq FROM event_heads WHERE codex_id=c.codex_id),0),c.current_view_json FROM codexes c WHERE c.codex_id=?`, codexID).Scan(&head, &view)
	return head, append([]byte(nil), view...), err
}

// CommitEvent allocates the next sequence and writes the matching CurrentView
// in one transaction. buildView executes after sequence allocation but before
// the view UPDATE, which also provides a deterministic rollback seam for tests.
func (s *Store) CommitEvent(ctx context.Context, codexID string, buildView func(uint64) ([]byte, error)) (uint64, error) {
	if codexID == "" || buildView == nil {
		return 0, errors.New("codex id and event view builder are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var exists int
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM codexes WHERE codex_id=?`, codexID).Scan(&exists); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO event_heads(codex_id,head_seq) VALUES(?,0) ON CONFLICT(codex_id) DO NOTHING`, codexID); err != nil {
		return 0, err
	}
	var seq uint64
	if err = tx.QueryRowContext(ctx, `UPDATE event_heads SET head_seq=head_seq+1 WHERE codex_id=? RETURNING head_seq`, codexID).Scan(&seq); err != nil {
		return 0, err
	}
	viewJSON, err := buildView(seq)
	if err != nil {
		return 0, err
	}
	if len(viewJSON) == 0 {
		return 0, errors.New("event CurrentView is required")
	}
	res, err := tx.ExecContext(ctx, `UPDATE codexes SET current_view_json=?,last_activity_at_ms=? WHERE codex_id=?`, viewJSON, time.Now().UnixMilli(), codexID)
	if err != nil {
		return 0, err
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if updated != 1 {
		return 0, sql.ErrNoRows
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrEventCommitOutcomeUnknown, err)
	}
	return seq, nil
}

// NextEventSequence is retained only for legacy callers and migration tests.
// Deprecated: event publication must use CommitEvent so the sequence and
// CurrentView share one transaction.
func (s *Store) NextEventSequence(ctx context.Context, codexID string) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO event_heads(codex_id,head_seq) VALUES(?,0) ON CONFLICT(codex_id) DO NOTHING`, codexID); err != nil {
		return 0, err
	}
	var seq uint64
	if err = tx.QueryRowContext(ctx, `UPDATE event_heads SET head_seq=head_seq+1 WHERE codex_id=? RETURNING head_seq`, codexID).Scan(&seq); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *Store) EventHead(ctx context.Context, codexID string) (uint64, error) {
	var seq uint64
	err := s.db.QueryRowContext(ctx, `SELECT head_seq FROM event_heads WHERE codex_id=?`, codexID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *Store) SaveResolvedPending(ctx context.Context, codexID, pendingID, kind string, resolvedJSON []byte) error {
	if codexID == "" || pendingID == "" || kind == "" || len(resolvedJSON) == 0 {
		return errors.New("resolved pending identity, kind and state are required")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO resolved_pending(codex_id,pending_id,kind,resolved_json,resolved_at_ms) VALUES(?,?,?,?,?) ON CONFLICT(codex_id,pending_id) DO UPDATE SET kind=excluded.kind,resolved_json=excluded.resolved_json,resolved_at_ms=excluded.resolved_at_ms`, codexID, pendingID, kind, resolvedJSON, time.Now().UnixMilli())
	return err
}
func (s *Store) ResolvedPending(ctx context.Context, codexID, pendingID string) (kind string, resolvedJSON []byte, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT kind,resolved_json FROM resolved_pending WHERE codex_id=? AND pending_id=?`, codexID, pendingID).Scan(&kind, &resolvedJSON)
	return
}

func equalBytes(a, b []byte) bool {
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
func nullableBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func normalizeSessionSource(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return "unknown"
	}
	return source
}
