package activity

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"google.golang.org/protobuf/encoding/protojson"
)

type failingAuditor struct{ err error }

func (f failingAuditor) RecordCanonical(context.Context, *remotev1.Event, *remotev1.Provenance, string) error {
	return f.err
}

type failAfterBuildPersistence struct {
	*persistence.Store
	fail bool
	err  error
}

type failCommitAndFirstReloadPersistence struct {
	*persistence.Store
	commitErr  error
	failReload bool
}

func (p *failCommitAndFirstReloadPersistence) CommitEvent(context.Context, string, func(uint64) ([]byte, error)) (uint64, error) {
	p.failReload = true
	return 0, p.commitErr
}

func (p *failCommitAndFirstReloadPersistence) LoadEventState(ctx context.Context, codexID string) (uint64, []byte, error) {
	if p.failReload {
		p.failReload = false
		return 0, nil, errors.New("injected reload failure")
	}
	return p.Store.LoadEventState(ctx, codexID)
}

func (p *failAfterBuildPersistence) CommitEvent(ctx context.Context, codexID string, build func(uint64) ([]byte, error)) (uint64, error) {
	return p.Store.CommitEvent(ctx, codexID, func(seq uint64) ([]byte, error) {
		raw, err := build(seq)
		if err != nil {
			return nil, err
		}
		if p.fail {
			return nil, p.err
		}
		return raw, nil
	})
}

func newPersistence(t *testing.T) *persistence.Store {
	t.Helper()
	p, err := persistence.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if err = p.UpsertCodex(context.Background(), persistence.CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Title: "x", Origin: "remote", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWatchReplayWithinRunAndResetAfterRestart(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	s := NewStore(p, nil, 4)
	for i := 0; i < 3; i++ {
		_, err := s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c"}}, nil, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	after := uint64(1)
	w, err := s.Watch(ctx, "c", &after, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if w.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED || len(w.Replay) != 2 {
		t.Fatalf("response=%+v replay=%d", w.Response, len(w.Replay))
	}
	restarted := NewStore(p, nil, 4)
	w2, err := restarted.ForceReset(ctx, "c", remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Cancel()
	if w2.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || w2.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED {
		t.Fatalf("restart response %+v", w2.Response)
	}
}

func TestPersistedHeadCanResumeAfterExplicitRunReset(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	first := NewStore(p, nil, 4)
	if _, err := first.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c"}}, nil, ""); err != nil {
		t.Fatal(err)
	}
	head := uint64(1)
	restarted := NewStore(p, nil, 4)
	reset, err := restarted.ForceReset(ctx, "c", remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED, 2)
	if err != nil {
		t.Fatal(err)
	}
	reset.Cancel()
	w, err := restarted.Watch(ctx, "c", &head, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if w.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED {
		t.Fatalf("same-run response %+v", w.Response)
	}
}

func TestUnsafeRestoreCanReplayOnlyFromExplicitRunBoundary(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	// Simulate a legacy/uncertain restore: durable sequence allocation advanced,
	// but the persisted CurrentView still describes the previous head.
	if err := p.SetCurrentView(ctx, "c", []byte(`{"headEventSeq":"0","codex":{"codexId":"c"}}`)); err != nil {
		t.Fatal(err)
	}
	if seq, err := p.NextEventSequence(ctx, "c"); err != nil || seq != 1 {
		t.Fatalf("legacy partial write seq=%d err=%v", seq, err)
	}

	s := NewStore(p, nil, 8)
	head := uint64(1)
	unsafe, err := s.Watch(ctx, "c", &head, 2)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || unsafe.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE {
		t.Fatalf("pre-boundary Watch response=%+v", unsafe.Response)
	}
	unsafe.Cancel()
	resumedAfterUnavailable, err := s.Watch(ctx, "c", &head, 2)
	if err != nil {
		t.Fatal(err)
	}
	if resumedAfterUnavailable.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED || resumedAfterUnavailable.Response.ReplayFromEventSeq != 2 {
		t.Fatalf("post-unavailable-reset Watch response=%+v", resumedAfterUnavailable.Response)
	}
	resumedAfterUnavailable.Cancel()

	reset, err := s.ForceReset(ctx, "c", remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED, 2)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Response.HeadEventSeq != head {
		t.Fatalf("reset head=%d, want %d", reset.Response.HeadEventSeq, head)
	}
	reset.Cancel()

	atBoundary, err := s.Watch(ctx, "c", &head, 2)
	if err != nil {
		t.Fatal(err)
	}
	if atBoundary.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED || atBoundary.Response.ReplayFromEventSeq != 2 || len(atBoundary.Replay) != 0 {
		t.Fatalf("boundary Watch response=%+v replay=%+v", atBoundary.Response, atBoundary.Replay)
	}
	atBoundary.Cancel()

	for i := 0; i < 2; i++ {
		if _, err = s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c"}}, nil, ""); err != nil {
			t.Fatal(err)
		}
	}

	fromBoundary, err := s.Watch(ctx, "c", &head, 2)
	if err != nil {
		t.Fatal(err)
	}
	if fromBoundary.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED || len(fromBoundary.Replay) != 2 || fromBoundary.Replay[0].EventSeq != 2 || fromBoundary.Replay[1].EventSeq != 3 {
		t.Fatalf("suffix Watch response=%+v replay=%+v", fromBoundary.Response, fromBoundary.Replay)
	}
	fromBoundary.Cancel()

	exactHead := uint64(3)
	exact, err := s.Watch(ctx, "c", &exactHead, 2)
	if err != nil {
		t.Fatal(err)
	}
	if exact.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED || exact.Response.ReplayFromEventSeq != 4 || len(exact.Replay) != 0 {
		t.Fatalf("exact-head Watch response=%+v replay=%+v", exact.Response, exact.Replay)
	}
	exact.Cancel()

	older := uint64(0)
	old, err := s.Watch(ctx, "c", &older, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Cancel()
	if old.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || old.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE {
		t.Fatalf("older Watch response=%+v", old.Response)
	}
}

func TestUnsafeRestoreInitialResetEstablishesReplayBoundary(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	if err := p.SetCurrentView(ctx, "c", []byte(`{"headEventSeq":"0","codex":{"codexId":"c"}}`)); err != nil {
		t.Fatal(err)
	}
	if seq, err := p.NextEventSequence(ctx, "c"); err != nil || seq != 1 {
		t.Fatalf("legacy partial write seq=%d err=%v", seq, err)
	}

	s := NewStore(p, nil, 4)
	initial, err := s.Watch(ctx, "c", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || initial.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_INITIAL_WATCH || initial.Response.HeadEventSeq != 1 {
		t.Fatalf("initial Watch response=%+v", initial.Response)
	}
	initial.Cancel()

	head := uint64(1)
	exact, err := s.Watch(ctx, "c", &head, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer exact.Cancel()
	if exact.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED || exact.Response.ReplayFromEventSeq != 2 || len(exact.Replay) != 0 {
		t.Fatalf("post-initial-reset Watch response=%+v replay=%+v", exact.Response, exact.Replay)
	}
}

func TestInitialResetAndLive(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	s := NewStore(p, nil, 4)
	w, err := s.Watch(ctx, "c", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if w.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET {
		t.Fatal(w.Response)
	}
	_, err = s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events:
		if ev.EventSeq != 1 {
			t.Fatal(ev)
		}
	default:
		t.Fatal("missing live event")
	}
}

func TestWatchOverflowSignalsSlowConsumer(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	s := NewStore(p, nil, 4)
	w, err := s.Watch(ctx, "c", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	for i := 0; i < 2; i++ {
		if _, err = s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{}, nil, ""); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-w.Slow:
	case <-time.After(time.Second):
		t.Fatal("missing slow-consumer signal")
	}
}

func TestWatchRejectsUnknownCodex(t *testing.T) {
	p := newPersistence(t)
	s := NewStore(p, nil, 4)
	if _, err := s.Watch(context.Background(), "missing", nil, 1); !errors.Is(err, ErrCodexNotFound) {
		t.Fatalf("Watch error = %v, want ErrCodexNotFound", err)
	}
}

func TestAuditFailureDoesNotSwallowBusinessEvent(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	wantErr := errors.New("audit disk unavailable")
	s := NewStore(p, failingAuditor{err: wantErr}, 4)
	w, err := s.Watch(ctx, "c", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	published, err := s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{}, nil, "")
	if err != nil || published == nil || published.EventSeq != 1 {
		t.Fatalf("Publish = %+v, %v", published, err)
	}
	select {
	case got := <-w.Events:
		if got.EventSeq != published.EventSeq {
			t.Fatalf("live event seq = %d, want %d", got.EventSeq, published.EventSeq)
		}
	default:
		t.Fatal("audit failure swallowed live event")
	}
	if degraded, message := s.AuditDegraded(); !degraded || message != wantErr.Error() {
		t.Fatalf("AuditDegraded = %v, %q", degraded, message)
	}
	head, raw, err := p.LoadEventState(ctx, "c")
	if err != nil || head != 1 {
		t.Fatalf("durable state after audit failure head=%d raw=%s err=%v", head, raw, err)
	}
	persisted := new(remotev1.CurrentView)
	if err = protojson.Unmarshal(raw, persisted); err != nil || persisted.HeadEventSeq != 1 {
		t.Fatalf("persisted view after audit failure=%+v err=%v", persisted, err)
	}
}

func TestFailedAtomicPublishRollsBackAndForcesWatchReset(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	p, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.UpsertCodex(ctx, persistence.CodexRecord{CodexID: "c", ThreadID: "t", CWD: "/tmp", Status: "idle", CreatedAtUnixMS: 1, LastActivityAtUnixMS: 1}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected CurrentView write failure")
	faults := &failAfterBuildPersistence{Store: p, err: injected}
	s := NewStore(faults, nil, 8)
	live, err := s.Watch(ctx, "c", nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c"}}, nil, "")
	if err != nil || first.EventSeq != 1 {
		t.Fatalf("first Publish=%+v err=%v", first, err)
	}
	<-live.Events
	boundary, err := s.ForceReset(ctx, "c", remotev1.WatchResetReason_WATCH_RESET_REASON_HOST_RESTARTED, 2)
	if err != nil {
		t.Fatal(err)
	}
	boundary.Cancel()
	faults.fail = true
	if got, publishErr := s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c", Title: "must rollback"}}, nil, ""); got != nil || !errors.Is(publishErr, injected) {
		t.Fatalf("failed Publish=%+v err=%v", got, publishErr)
	}
	select {
	case event := <-live.Events:
		t.Fatalf("failed Publish leaked live event %+v", event)
	default:
	}
	head, raw, err := p.LoadEventState(ctx, "c")
	if err != nil || head != 1 {
		t.Fatalf("durable state head=%d raw=%s err=%v", head, raw, err)
	}
	persisted := new(remotev1.CurrentView)
	if err = protojson.Unmarshal(raw, persisted); err != nil || persisted.HeadEventSeq != 1 || persisted.GetCodex().GetTitle() != "" {
		t.Fatalf("persisted rollback view=%+v err=%v", persisted, err)
	}
	after := uint64(1)
	reset, err := s.Watch(ctx, "c", &after, 2)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || reset.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE {
		t.Fatalf("failed-commit Watch response=%+v", reset.Response)
	}
	live.Cancel()
	reset.Cancel()
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	reopened := NewStore(p, nil, 8)
	after = 0
	w, err := reopened.Watch(ctx, "c", &after, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if w.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || w.Response.HeadEventSeq != 1 || w.Response.ResetView.GetHeadEventSeq() != 1 {
		t.Fatalf("reopened Watch response=%+v", w.Response)
	}
}

func TestLegacyHeadViewMismatchResetsOlderCursor(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	if err := p.SetCurrentView(ctx, "c", []byte(`{"headEventSeq":"0","codex":{"codexId":"c"}}`)); err != nil {
		t.Fatal(err)
	}
	if seq, err := p.NextEventSequence(ctx, "c"); err != nil || seq != 1 {
		t.Fatalf("legacy partial write seq=%d err=%v", seq, err)
	}
	s := NewStore(p, nil, 4)
	older := uint64(0)
	w, err := s.Watch(ctx, "c", &older, 1)
	if err != nil {
		t.Fatal(err)
	}
	if w.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || w.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE || w.Response.HeadEventSeq != 1 {
		t.Fatalf("cursor=%d response=%+v", older, w.Response)
	}
	w.Cancel()
}

func TestContinuousReplayRejectsInternalGap(t *testing.T) {
	events := []*remotev1.Event{{EventSeq: 1}, {EventSeq: 3}}
	if continuousReplayAvailable(events, 1, 3) {
		t.Fatal("gapped replay reported available")
	}
}

func TestWatchReloadsDurableBoundaryAfterIndeterminatePublishFailure(t *testing.T) {
	ctx := context.Background()
	p := newPersistence(t)
	faults := &failCommitAndFirstReloadPersistence{Store: p, commitErr: persistence.ErrEventCommitOutcomeUnknown}
	s := NewStore(faults, nil, 4)
	// Prime the cached stream before the injected commit/reload failures.
	initial, err := s.Watch(ctx, "c", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	initial.Cancel()
	if _, err = s.Publish(ctx, &remotev1.Event{CodexId: "c"}, &remotev1.CurrentView{Codex: &remotev1.Codex{CodexId: "c"}}, nil, ""); !errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
		t.Fatalf("Publish error=%v", err)
	}
	after := uint64(0)
	w, err := s.Watch(ctx, "c", &after, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Cancel()
	if w.Response.Mode != remotev1.WatchMode_WATCH_MODE_RESET || w.Response.ResetReason != remotev1.WatchResetReason_WATCH_RESET_REASON_CURSOR_UNAVAILABLE {
		t.Fatalf("Watch response=%+v", w.Response)
	}
}
