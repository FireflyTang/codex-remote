package audit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRecorderWritesProtoJSONAndRotates(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Config{Dir: dir, ProcessRunID: "run", MaxFileBytes: 400, MaxRawBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	f := &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "req"}}}
	for i := 0; i < 3; i++ {
		if _, err := r.RecordWire(context.Background(), true, "conn", "client", "cr", []byte("abcdefgh"), f, remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "cs-wire"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected rotation, got %d files", len(entries))
	}
	b, err := os.ReadFile(filepath.Join(dir, "cs-wire", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	line := string(b)
	if !strings.Contains(line, `"requestId":"req"`) || !strings.Contains(line, `"rawTruncated":true`) {
		t.Fatalf("unexpected JSONL %s", line)
	}
}

func TestRecorderCorrelatesRequestRawAndCanonicalActivity(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Config{Dir: dir, ProcessRunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	requestFrame := &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "cs-request"}}}
	wireRecordID, err := r.RecordWire(context.Background(), true, "conn", "client", "client-run", []byte(`{"request":{"requestId":"cs-request"}}`), requestFrame, remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.RecordAppServerWire(context.Background(), true, []byte(`{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1"}}}`), "", "", ""); err != nil {
		t.Fatal(err)
	}
	event := &remotev1.Event{CodexId: "codex-1", EventSeq: 7, CausedByRequestId: "cs-request", Event: &remotev1.Event_ItemDelta{ItemDelta: &remotev1.ItemDelta{TurnId: "turn-1", ItemId: "item-1"}}}
	if err = r.RecordCanonical(context.Background(), event, &remotev1.Provenance{}, ""); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "activities", "*.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("activity files = %v, %v", files, err)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	rec := new(remotev1.AuditRecord)
	if err = protojson.Unmarshal([]byte(strings.TrimSpace(string(raw))), rec); err != nil {
		t.Fatal(err)
	}
	if rec.ParentRecordId != wireRecordID || rec.RequestId != "cs-request" || rec.TurnId != "turn-1" || rec.ItemId != "item-1" {
		t.Fatalf("correlation fields = %+v", rec)
	}
	if rec.Provenance == nil || len(rec.Provenance.SourceRecordIds) != 2 {
		t.Fatalf("source records = %+v", rec.Provenance)
	}
}

func TestRecorderFailureIsVisibleAsDegraded(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "rpc"), []byte("blocks journal directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = r.Record(context.Background(), &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_RPC})
	if err == nil {
		t.Fatal("Record unexpectedly succeeded")
	}
	if degraded, message := r.Degraded(); !degraded || message == "" {
		t.Fatalf("Degraded = %v, %q", degraded, message)
	}
}

func TestDegradedRecorderKeepsFailureVisibleWithoutStorage(t *testing.T) {
	wantErr := errors.New("audit directory unavailable")
	r := NewDegraded("run", wantErr)
	if err := r.Record(context.Background(), &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_HOST_ACTION}); !errors.Is(err, wantErr) {
		t.Fatalf("Record error = %v, want %v", err, wantErr)
	}
	if degraded, message := r.Degraded(); !degraded || message != wantErr.Error() {
		t.Fatalf("Degraded = %v, %q", degraded, message)
	}
	if r.ProcessRunID() != "run" {
		t.Fatalf("ProcessRunID = %q", r.ProcessRunID())
	}
}

func TestExportAndSafeMetadata(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Config{Dir: filepath.Join(dir, "audit")})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Record(context.Background(), &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_HOST_ACTION, Message: "ok"}); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "diag.tar.gz")
	if err := r.Export(context.Background(), dst); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(dst); err != nil || info.Size() == 0 {
		t.Fatalf("export err=%v", err)
	}
	m := SafeMetadata(map[string]string{"node": "n", "auth_key": "bad"})
	if len(m) != 1 || m["node"] != "n" {
		t.Fatalf("unsafe metadata %+v", m)
	}
}
