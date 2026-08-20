// Package audit writes append-only, human-readable ProtoJSON evidence.
package audit

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kylin1993/codex-remote/internal/tailnet"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Dir          string
	ProcessRunID string
	MaxFileBytes int64
	MaxRawBytes  int
}

type Recorder struct {
	cfg         Config
	mu          sync.Mutex
	seq         uint64
	files       map[string]*journal
	degraded    atomic.Bool
	lastErr     atomic.Value // string
	corrMu      sync.Mutex
	requestWire map[string]string
	appByTurn   map[string]string
	disabled    error
}

// NewDegraded keeps diagnostic failure visible while allowing the Host's
// business path to start. Every record attempt returns the original cause and
// Degraded reports it; no evidence is falsely claimed to have been written.
func NewDegraded(processRunID string, cause error) *Recorder {
	if cause == nil {
		cause = errors.New("audit recorder unavailable")
	}
	r := &Recorder{cfg: Config{ProcessRunID: processRunID}, files: make(map[string]*journal), requestWire: make(map[string]string), appByTurn: make(map[string]string), disabled: cause}
	r.degraded.Store(true)
	r.lastErr.Store(cause.Error())
	return r
}

type journal struct {
	file *os.File
	path string
	size int64
	part int
}

func New(cfg Config) (*Recorder, error) {
	if cfg.Dir == "" {
		return nil, errors.New("audit directory is required")
	}
	if cfg.ProcessRunID == "" {
		cfg.ProcessRunID = newID("host_run")
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = 16 << 20
	}
	if cfg.MaxRawBytes <= 0 {
		cfg.MaxRawBytes = 1 << 20
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	r := &Recorder{cfg: cfg, files: make(map[string]*journal), requestWire: make(map[string]string), appByTurn: make(map[string]string)}
	manifest := map[string]any{"auditFormatVersion": 1, "side": "host", "processRunId": cfg.ProcessRunID, "startedAt": time.Now().UTC().Format(time.RFC3339Nano), "format": "ProtoJSON JSONL", "integrity": "diagnostic; no hash chain"}
	b, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(cfg.Dir, "manifest.json"), append(b, '\n'), 0o600); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Recorder) ProcessRunID() string { return r.cfg.ProcessRunID }
func (r *Recorder) Degraded() (bool, string) {
	v, _ := r.lastErr.Load().(string)
	return r.degraded.Load(), v
}

func (r *Recorder) Record(_ context.Context, rec *remotev1.AuditRecord) error {
	if r.disabled != nil {
		return r.markFailed(r.disabled)
	}
	if rec == nil {
		return errors.New("nil audit record")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	rec.FormatVersion = 1
	rec.LocalSeq = r.seq
	rec.ProcessRunId = r.cfg.ProcessRunID
	rec.Side = remotev1.AuditSide_AUDIT_SIDE_HOST
	if rec.RecordId == "" {
		rec.RecordId = newID("audit")
	}
	if rec.ObservedAtUnixMs == 0 {
		rec.ObservedAtUnixMs = time.Now().UnixMilli()
	}
	if rec.RawText != "" {
		raw := []byte(rec.RawText)
		sum := sha256.Sum256(raw)
		rec.RawSha256 = hex.EncodeToString(sum[:])
		rec.RawSizeBytes = uint64(len(raw))
		if len(raw) > r.cfg.MaxRawBytes {
			rec.RawText = string(raw[:r.cfg.MaxRawBytes])
			rec.RawTruncated = true
		}
	}
	b, err := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(rec)
	if err != nil {
		return r.markFailed(err)
	}
	b = append(b, '\n')
	key := journalKey(rec.Kind)
	j, err := r.ensureJournal(key, int64(len(b)))
	if err != nil {
		return r.markFailed(err)
	}
	n, err := j.file.Write(b)
	j.size += int64(n)
	if err != nil || n != len(b) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return r.markFailed(err)
	}
	return nil
}

func (r *Recorder) RecordTailnet(ctx context.Context, e tailnet.AuditEvent) error {
	outcome := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
	if e.Outcome == "failed" {
		outcome = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
	}
	return r.Record(ctx, &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_HOST_ACTION, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: outcome, Component: "tailnet", Operation: e.Operation, Message: e.Message, Metadata: SafeMetadata(e.Metadata)})
}

// RecordWire preserves both exact text and the decoded Frame when available.
func (r *Recorder) RecordWire(ctx context.Context, inbound bool, connectionID, clientID, clientRunID string, raw []byte, frame *remotev1.Frame, outcome remotev1.AuditOutcome) (string, error) {
	dir := remotev1.AuditDirection_AUDIT_DIRECTION_OUTBOUND
	if inbound {
		dir = remotev1.AuditDirection_AUDIT_DIRECTION_INBOUND
	}
	rec := &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_CS_WIRE_FRAME, Direction: dir, Outcome: outcome, Component: "gateway", Operation: "cs_wire", ConnectionId: connectionID, ClientId: clientID, ClientRunId: clientRunID, RawText: string(raw), Frame: frame}
	populateFrameIDs(rec, frame)
	if err := r.Record(ctx, rec); err != nil {
		return "", err
	}
	if inbound && frame != nil && frame.GetRequest() != nil && rec.RequestId != "" {
		r.corrMu.Lock()
		r.requestWire[rec.RequestId] = rec.RecordId
		r.corrMu.Unlock()
	}
	return rec.RecordId, nil
}

func (r *Recorder) RecordAppServerWire(ctx context.Context, inbound bool, raw []byte, requestID, threadID, turnID string) error {
	dir := remotev1.AuditDirection_AUDIT_DIRECTION_OUTBOUND
	if inbound {
		dir = remotev1.AuditDirection_AUDIT_DIRECTION_INBOUND
	}
	parsedRequest, parsedThread, parsedTurn := appServerIDs(raw)
	if requestID == "" {
		requestID = parsedRequest
	}
	if threadID == "" {
		threadID = parsedThread
	}
	if turnID == "" {
		turnID = parsedTurn
	}
	rec := &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_APP_SERVER_WIRE, Direction: dir, Outcome: remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, Component: "app_server", Operation: "raw_wire", RawText: string(raw), RequestId: requestID, ThreadId: threadID, TurnId: turnID}
	if err := r.Record(ctx, rec); err != nil {
		return err
	}
	if turnID != "" {
		r.corrMu.Lock()
		r.appByTurn[turnID] = rec.RecordId
		r.corrMu.Unlock()
	}
	return nil
}

func (r *Recorder) RecordCanonical(ctx context.Context, event *remotev1.Event, provenance *remotev1.Provenance, parentRecordID string) error {
	if event == nil {
		return errors.New("nil canonical event")
	}
	p := &remotev1.Provenance{}
	if provenance != nil {
		p = proto.Clone(provenance).(*remotev1.Provenance)
	}
	turnID, itemID := eventIDs(event)
	r.corrMu.Lock()
	if id := r.requestWire[event.GetCausedByRequestId()]; id != "" {
		p.SourceRecordIds = appendUnique(p.SourceRecordIds, id)
	}
	if id := r.appByTurn[turnID]; id != "" {
		p.SourceRecordIds = appendUnique(p.SourceRecordIds, id)
	}
	r.corrMu.Unlock()
	if parentRecordID == "" && len(p.SourceRecordIds) > 0 {
		parentRecordID = p.SourceRecordIds[0]
	}
	activity := &remotev1.CanonicalActivity{ActivityId: newID("activity"), CodexId: event.GetCodexId(), TurnId: turnID, ItemId: itemID, OccurredAtUnixMs: event.GetOccurredAtUnixMs(), Event: event, Provenance: p}
	return r.Record(ctx, &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_CANONICAL_ACTIVITY, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, Component: "activity", Operation: "canonical_event", CodexId: event.GetCodexId(), EventSeq: event.GetEventSeq(), RequestId: event.GetCausedByRequestId(), TurnId: turnID, ItemId: itemID, CanonicalActivity: activity, Provenance: p, ParentRecordId: parentRecordID})
}

func (r *Recorder) RecordRuntime(ctx context.Context, operation string, raw []byte, failed bool) error {
	out := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
	if failed {
		out = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
	}
	return r.Record(ctx, &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_RUNTIME_LOG, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: out, Component: "runtime", Operation: operation, RawText: string(raw)})
}

func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, j := range r.files {
		if err := j.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	r.files = make(map[string]*journal)
	return errors.Join(errs...)
}

// Export writes a portable diagnostic archive. It contains evidence files and
// a manifest, never tsnet state, auth keys, or SQLite.
func (r *Recorder) Export(ctx context.Context, dst string) error {
	if r.disabled != nil {
		return r.disabled
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, j := range r.files {
		_ = j.file.Sync()
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(dst)
		}
	}()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	err = filepath.WalkDir(r.cfg.Dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(r.cfg.Dir, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		h, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		h.Name = filepath.ToSlash(rel)
		if err = tw.WriteHeader(h); err != nil {
			return err
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, src)
		closeErr := src.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if err == nil {
		err = tw.Close()
	} else {
		_ = tw.Close()
	}
	if err == nil {
		err = gz.Close()
	} else {
		_ = gz.Close()
	}
	if err == nil {
		err = f.Close()
	}
	if err == nil {
		ok = true
	}
	return err
}

func (r *Recorder) ensureJournal(key string, incoming int64) (*journal, error) {
	j := r.files[key]
	if j != nil && j.size+incoming <= r.cfg.MaxFileBytes {
		return j, nil
	}
	part := 0
	if j != nil {
		part = j.part + 1
		if err := j.file.Close(); err != nil {
			return nil, err
		}
	}
	dir := filepath.Join(r.cfg.Dir, key)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	name := time.Now().UTC().Format("2006-01-02") + fmt.Sprintf("-%03d.jsonl", part)
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	j = &journal{file: f, path: path, part: part}
	if info != nil {
		j.size = info.Size()
	}
	r.files[key] = j
	return j, nil
}

func (r *Recorder) markFailed(err error) error {
	r.degraded.Store(true)
	r.lastErr.Store(err.Error())
	return err
}
func journalKey(k remotev1.AuditKind) string {
	switch k {
	case remotev1.AuditKind_AUDIT_KIND_CS_WIRE_FRAME:
		return "cs-wire"
	case remotev1.AuditKind_AUDIT_KIND_RPC:
		return "rpc"
	case remotev1.AuditKind_AUDIT_KIND_CANONICAL_ACTIVITY:
		return "activities"
	case remotev1.AuditKind_AUDIT_KIND_APP_SERVER_WIRE:
		return "app-server-wire"
	case remotev1.AuditKind_AUDIT_KIND_HOST_ACTION:
		return "host-actions"
	case remotev1.AuditKind_AUDIT_KIND_RUNTIME_LOG:
		return "runtime"
	default:
		return "host"
	}
}
func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
func populateFrameIDs(rec *remotev1.AuditRecord, f *remotev1.Frame) {
	if f == nil {
		return
	}
	if req := f.GetRequest(); req != nil {
		rec.RequestId = req.RequestId
	}
	if resp := f.GetResponse(); resp != nil {
		rec.RequestId = resp.RequestId
	}
	if ev := f.GetEvent(); ev != nil {
		rec.CodexId = ev.CodexId
		rec.EventSeq = ev.EventSeq
		rec.RequestId = ev.CausedByRequestId
	}
	if h := f.GetClientHello(); h != nil {
		rec.ClientId = h.ClientId
		rec.ClientRunId = h.ClientRunId
	}
}

func appServerIDs(raw []byte) (requestID, threadID, turnID string) {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return "", "", ""
	}
	if top, ok := value.(map[string]any); ok {
		requestID = scalarString(top["requestId"])
		if requestID == "" {
			requestID = scalarString(top["id"])
		}
	}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if threadID == "" {
				threadID = scalarString(x["threadId"])
			}
			if turnID == "" {
				turnID = scalarString(x["turnId"])
			}
			if threadID == "" {
				if thread, ok := x["thread"].(map[string]any); ok {
					threadID = scalarString(thread["id"])
				}
			}
			if turnID == "" {
				if turn, ok := x["turn"].(map[string]any); ok {
					turnID = scalarString(turn["id"])
				}
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(value)
	return requestID, threadID, turnID
}

func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return fmt.Sprintf("%v", x)
	default:
		return ""
	}
}

func eventIDs(event *remotev1.Event) (turnID, itemID string) {
	switch {
	case event.GetTurnUpdated() != nil:
		turnID = event.GetTurnUpdated().GetTurnId()
	case event.GetItemStarted() != nil && event.GetItemStarted().GetItem() != nil:
		turnID = event.GetItemStarted().GetItem().GetTurnId()
		itemID = event.GetItemStarted().GetItem().GetItemId()
	case event.GetItemDelta() != nil:
		turnID = event.GetItemDelta().GetTurnId()
		itemID = event.GetItemDelta().GetItemId()
	case event.GetItemUpdated() != nil && event.GetItemUpdated().GetItem() != nil:
		turnID = event.GetItemUpdated().GetItem().GetTurnId()
		itemID = event.GetItemUpdated().GetItem().GetItemId()
	case event.GetItemCompleted() != nil && event.GetItemCompleted().GetItem() != nil:
		turnID = event.GetItemCompleted().GetItem().GetTurnId()
		itemID = event.GetItemCompleted().GetItem().GetItemId()
	case event.GetPendingRequestUpdated() != nil:
		pending := event.GetPendingRequestUpdated().GetRequest()
		if pending != nil && pending.GetApproval() != nil {
			turnID = pending.GetApproval().GetTurnId()
			itemID = pending.GetApproval().GetItemId()
		} else if pending != nil && pending.GetUserInput() != nil {
			turnID = pending.GetUserInput().GetTurnId()
			itemID = pending.GetUserInput().GetItemId()
		}
	}
	return turnID, itemID
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// Prevent accidental inclusion of secrets in free-form metadata.
func SafeMetadata(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		lower := strings.ToLower(k)
		if strings.Contains(lower, "auth") || strings.Contains(lower, "secret") || strings.Contains(lower, "key") {
			continue
		}
		out[k] = v
	}
	return out
}
