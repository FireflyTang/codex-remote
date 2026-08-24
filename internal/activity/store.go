// Package activity assigns Remote event sequences and provides restart-aware
// replay plus current-view reset semantics.
package activity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Persistence interface {
	LoadEventState(context.Context, string) (uint64, []byte, error)
	CommitEvent(context.Context, string, func(uint64) ([]byte, error)) (uint64, error)
}

type Auditor interface {
	RecordCanonical(context.Context, *remotev1.Event, *remotev1.Provenance, string) error
}

var ErrCodexNotFound = errors.New("codex not found")

type Store struct {
	persist  Persistence
	audit    Auditor
	capacity int
	mu       sync.Mutex
	codex    map[string]*stream
	auditErr atomic.Value // string; audit is diagnostic and never gates events
}

type stream struct {
	head         uint64
	events       []*remotev1.Event
	view         *remotev1.CurrentView
	replayUnsafe bool
	// safeReplayFloor is the head exposed by a RESET in this Store run. Even
	// when pre-run replay is unsafe, cursors at or beyond this boundary may
	// resume from the continuous in-memory suffix.
	safeReplayFloor *uint64
	needsReload     bool
	nextWatcher     uint64
	watchers        map[uint64]*watcher
}

type watcher struct {
	events chan *remotev1.Event
	slow   chan struct{}
}

type Watch struct {
	Response *remotev1.WatchCodexResponse
	Replay   []*remotev1.Event
	Events   <-chan *remotev1.Event
	Slow     <-chan struct{}
	Cancel   func()
}

func NewStore(p Persistence, a Auditor, replayCapacity int) *Store {
	if replayCapacity <= 0 {
		replayCapacity = 1024
	}
	return &Store{persist: p, audit: a, capacity: replayCapacity, codex: make(map[string]*stream)}
}

func (s *Store) ensure(ctx context.Context, id string) (*stream, error) {
	if st := s.codex[id]; st != nil {
		if st.needsReload {
			if err := s.loadDurable(ctx, id, st); err != nil {
				return nil, err
			}
			st.needsReload = false
			st.replayUnsafe = true
		}
		return st, nil
	}
	head, raw, err := s.persist.LoadEventState(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrCodexNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	st := &stream{head: head, watchers: make(map[uint64]*watcher)}
	if len(raw) > 0 {
		v := new(remotev1.CurrentView)
		if protojson.Unmarshal(raw, v) == nil {
			st.view = v
			st.replayUnsafe = v.HeadEventSeq != head
		} else {
			st.replayUnsafe = true
		}
	} else if head != 0 {
		st.replayUnsafe = true
	}
	s.codex[id] = st
	return st, nil
}

// Publish persists the new CurrentView before exposing the canonical event.
// The JSONL audit remains diagnostic evidence; SQLite is the restart snapshot.
func (s *Store) Publish(ctx context.Context, event *remotev1.Event, view *remotev1.CurrentView, provenance *remotev1.Provenance, parentRecordID string) (*remotev1.Event, error) {
	if event == nil || event.CodexId == "" {
		return nil, errors.New("event with codex id is required")
	}
	if view == nil {
		return nil, errors.New("event CurrentView is required")
	}
	// Serialize allocation, persistence and publication so per-Codex sequence
	// order cannot be inverted by concurrent adapter events.
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.ensure(ctx, event.CodexId)
	if err != nil {
		return nil, err
	}
	var committedView *remotev1.CurrentView
	seq, err := s.persist.CommitEvent(ctx, event.CodexId, func(seq uint64) ([]byte, error) {
		v := proto.Clone(view).(*remotev1.CurrentView)
		v.HeadEventSeq = seq
		raw, err := protojson.Marshal(v)
		if err != nil {
			return nil, err
		}
		committedView = v
		return raw, nil
	})
	if err != nil {
		// Even when SQLite rolled back cleanly, conservatively invalidate replay
		// after a failed commit attempt. This also covers an indeterminate Commit
		// outcome without ever exposing an internal sequence gap as RESUMED.
		s.reloadUnsafe(ctx, event.CodexId, st)
		return nil, err
	}
	ev := proto.Clone(event).(*remotev1.Event)
	ev.EventSeq = seq
	if s.audit != nil {
		if err = s.audit.RecordCanonical(ctx, ev, provenance, parentRecordID); err != nil {
			// Diagnostic evidence must expose degradation, but it must never make
			// a persisted canonical business event disappear from replay/live.
			s.auditErr.Store(err.Error())
		}
	}
	st.head = seq
	st.view = committedView
	st.events = append(st.events, ev)
	if len(st.events) > s.capacity {
		st.events = append([]*remotev1.Event(nil), st.events[len(st.events)-s.capacity:]...)
	}
	for id, watcher := range st.watchers {
		select {
		case watcher.events <- proto.Clone(ev).(*remotev1.Event):
		default:
			close(watcher.slow)
			delete(st.watchers, id)
		}
	}
	return ev, nil
}

func (s *Store) AuditDegraded() (bool, string) {
	v, ok := s.auditErr.Load().(string)
	return ok && v != "", v
}

func (s *Store) reloadUnsafe(ctx context.Context, codexID string, st *stream) {
	st.events = nil
	st.replayUnsafe = true
	st.safeReplayFloor = nil
	if err := s.loadDurable(ctx, codexID, st); err != nil {
		st.needsReload = true
	} else {
		st.needsReload = false
	}
}

// ReloadDurable discards the cached replay suffix and reloads the canonical
// CurrentView from persistence. State owners use this after rolling back a
// failed event publication so RESET cannot expose the pre-rollback cache.
func (s *Store) ReloadDurable(ctx context.Context, codexID string) error {
	if codexID == "" {
		return errors.New("codex id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.codex[codexID]
	if st == nil {
		st = &stream{watchers: make(map[uint64]*watcher)}
		s.codex[codexID] = st
	}
	st.events = nil
	st.replayUnsafe = true
	st.safeReplayFloor = nil
	if err := s.loadDurable(ctx, codexID, st); err != nil {
		st.needsReload = true
		return err
	}
	st.needsReload = false
	return nil
}

func (s *Store) loadDurable(ctx context.Context, codexID string, st *stream) error {
	head, raw, err := s.persist.LoadEventState(ctx, codexID)
	if err != nil {
		return err
	}
	st.head = head
	st.view = nil
	if len(raw) > 0 {
		v := new(remotev1.CurrentView)
		if protojson.Unmarshal(raw, v) == nil {
			st.view = v
		}
	}
	return nil
}

// Watch atomically captures the replay/reset boundary and registers live
// delivery. The caller must send Response, then Replay, then read Events.
func (s *Store) Watch(ctx context.Context, codexID string, after *uint64, queue int) (*Watch, error) {
	if codexID == "" {
		return nil, errors.New("codex id is required")
	}
	if queue <= 0 {
		queue = 128
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.ensure(ctx, codexID)
	if err != nil {
		return nil, err
	}
	return s.watchLocked(codexID, st, after, queue), nil
}

func (s *Store) watchLocked(codexID string, st *stream, after *uint64, queue int) *Watch {
	st.nextWatcher++
	id := st.nextWatcher
	w := &watcher{events: make(chan *remotev1.Event, queue), slow: make(chan struct{})}
	st.watchers[id] = w
	cancelOnce := sync.Once{}
	cancel := func() {
		cancelOnce.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if existing, ok := st.watchers[id]; ok {
				delete(st.watchers, id)
				close(existing.events)
			}
		})
	}
	resp := &remotev1.WatchCodexResponse{CodexId: codexID, HeadEventSeq: st.head}
	var replay []*remotev1.Event
	if after == nil {
		resp.Mode = remotev1.WatchMode_WATCH_MODE_RESET
		resp.ResetReason = remotev1.WatchResetReason_WATCH_RESET_REASON_INITIAL_WATCH
		resp.ResetView = cloneView(st.view, st.head)
	} else if st.replayUnsafe && (st.safeReplayFloor == nil || *after < *st.safeReplayFloor) {
		resp.Mode = remotev1.WatchMode_WATCH_MODE_RESET
		resp.ResetReason = remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE
		resp.ResetView = cloneView(st.view, st.head)
	} else if *after > st.head {
		resp.Mode = remotev1.WatchMode_WATCH_MODE_RESET
		resp.ResetReason = remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_INVALID
		resp.ResetView = cloneView(st.view, st.head)
	} else {
		start := *after + 1
		available := continuousReplayAvailable(st.events, start, st.head)
		if !available {
			resp.Mode = remotev1.WatchMode_WATCH_MODE_RESET
			resp.ResetReason = remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE
			resp.ResetView = cloneView(st.view, st.head)
		} else {
			resp.Mode = remotev1.WatchMode_WATCH_MODE_RESUMED
			resp.ReplayFromEventSeq = start
			for _, ev := range st.events {
				if ev.EventSeq >= start && ev.EventSeq <= st.head {
					replay = append(replay, proto.Clone(ev).(*remotev1.Event))
				}
			}
		}
	}
	if resp.Mode == remotev1.WatchMode_WATCH_MODE_RESET {
		boundary := resp.HeadEventSeq
		st.safeReplayFloor = &boundary
	}
	return &Watch{Response: resp, Replay: replay, Events: w.events, Slow: w.slow, Cancel: cancel}
}

func continuousReplayAvailable(events []*remotev1.Event, start, head uint64) bool {
	if start > head {
		return true
	}
	expected := start
	for _, event := range events {
		if event.EventSeq < start {
			continue
		}
		if event.EventSeq != expected {
			return false
		}
		expected++
		if expected > head {
			return true
		}
	}
	return false
}

// ForceReset registers a live watch at the current boundary while explicitly
// reporting why replay was not attempted (for example, a Host run mismatch).
func (s *Store) ForceReset(ctx context.Context, codexID string, reason remotev1.WatchResetReason, queue int) (*Watch, error) {
	if codexID == "" {
		return nil, errors.New("codex id is required")
	}
	if queue <= 0 {
		queue = 128
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.ensure(ctx, codexID)
	if err != nil {
		return nil, err
	}
	w := s.watchLocked(codexID, st, nil, queue)
	w.Response.ResetReason = reason
	return w, nil
}

func (s *Store) CurrentView(ctx context.Context, codexID string) (*remotev1.CurrentView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.ensure(ctx, codexID)
	if err != nil {
		return nil, err
	}
	if st.view == nil {
		return nil, fmt.Errorf("current view for %q unavailable", codexID)
	}
	return proto.Clone(st.view).(*remotev1.CurrentView), nil
}

func cloneView(v *remotev1.CurrentView, head uint64) *remotev1.CurrentView {
	if v == nil {
		return &remotev1.CurrentView{HeadEventSeq: head}
	}
	out := proto.Clone(v).(*remotev1.CurrentView)
	out.HeadEventSeq = head
	return out
}
