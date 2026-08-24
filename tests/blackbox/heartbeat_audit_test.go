package blackbox_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestHeartbeatPingPongKeepsConnectionUsable(t *testing.T) {
	requireScenario(t, "normal")
	c := dial(t)
	hello := c.hello(t)
	if hello.HeartbeatIntervalMs != 200 || hello.ConnectionTimeoutMs != 2000 {
		t.Fatalf("test heartbeat config not advertised: %+v", hello)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		frame := c.readNetworkFrame(t)
		if ping := frame.GetPing(); ping != nil {
			if ping.Nonce == 0 || ping.SentAtUnixMs == 0 {
				t.Fatalf("invalid Ping=%+v", ping)
			}
			c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Pong{Pong: &remotev1.Pong{Nonce: ping.Nonce, PingSentAtUnixMs: ping.SentAtUnixMs, PongSentAtUnixMs: time.Now().UnixMilli()}}})
			resp := c.request(t, &remotev1.Request{RequestId: "after-pong", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}})
			if resp.GetGetHost() == nil {
				t.Fatalf("connection unusable after Pong: %+v", resp)
			}
			return
		}
	}
	t.Fatal("Host did not send application Ping")
}

func TestHeartbeatTimeoutSendsProtocolClose(t *testing.T) {
	requireScenario(t, "normal")
	c := dial(t)
	c.hello(t)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		frame := c.readNetworkFrame(t)
		if closeFrame := frame.GetClose(); closeFrame != nil {
			if closeFrame.Code != remotev1.CloseCode_CLOSE_CODE_CONNECTION_TIMEOUT || !closeFrame.ReconnectAllowed {
				t.Fatalf("timeout Close=%+v", closeFrame)
			}
			return
		}
	}
	t.Fatal("Host did not send timeout Close")
}

func TestHostDiagnosticAuditContainsCorrelatedWireAndCanonicalEvidence(t *testing.T) {
	requireScenario(t, "normal")
	auditDir := os.Getenv("CODEX_REMOTE_BLACKBOX_AUDIT_DIR")
	if auditDir == "" {
		t.Skip("CODEX_REMOTE_BLACKBOX_AUDIT_DIR is unset")
	}
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	startAndWaitCompletion(t, c, codexID)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		records := readAuditRecords(t, auditDir)
		if auditRecordsCorrelate(records, codexID) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("audit missing minimally correlated wire/app/canonical evidence for %s", codexID)
}

func TestAuditWriteFailureDoesNotBlockBusiness(t *testing.T) {
	requireScenario(t, "audit-failure")
	auditDir := os.Getenv("CODEX_REMOTE_BLACKBOX_AUDIT_DIR")
	journalDir := filepath.Join(auditDir, "cs-wire")
	initial := dial(t)
	initial.hello(t)
	initial.request(t, request("audit-open-journal", &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}))
	_ = initial.conn.CloseNow()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(journalDir); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("audit journal was not opened: %s", journalDir)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := os.Chmod(journalDir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(journalDir, 0o700) })
	largeInvalid := []byte(strings.Repeat("x", 1<<20))
	for i := 0; i < 20; i++ {
		c := dial(t)
		c.writeRaw(t, 1, largeInvalid)
		c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetClose() != nil })
		_ = c.conn.CloseNow()
	}

	c := dial(t)
	c.hello(t)
	response := c.request(t, request("after-audit-failure", &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}))
	if response.GetGetHost() == nil {
		t.Fatalf("audit failure blocked business response: %+v", response)
	}
	hostLog := os.Getenv("CODEX_REMOTE_BLACKBOX_HOST_LOG")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(hostLog)
		if strings.Contains(string(raw), "diagnostic audit degraded") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("audit failure was not surfaced in Host log %s", hostLog)
}

func readAuditRecords(t *testing.T, auditDir string) []*remotev1.AuditRecord {
	t.Helper()
	var records []*remotev1.AuditRecord
	_ = filepath.WalkDir(auditDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			return nil
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64<<10), 2<<20)
		for scanner.Scan() {
			record := new(remotev1.AuditRecord)
			if (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(scanner.Bytes(), record) == nil {
				records = append(records, record)
			}
		}
		return nil
	})
	return records
}

func auditRecordsCorrelate(records []*remotev1.AuditRecord, codexID string) bool {
	byID := make(map[string]*remotev1.AuditRecord, len(records))
	for _, record := range records {
		if record.RecordId == "" || record.LocalSeq == 0 || record.ProcessRunId == "" || record.ObservedAtUnixMs == 0 {
			continue
		}
		byID[record.RecordId] = record
	}
	var csLinked, appLinked bool
	for _, record := range records {
		if record.Kind != remotev1.AuditKind_AUDIT_KIND_CANONICAL_ACTIVITY || record.CodexId != codexID || record.EventSeq == 0 || record.CanonicalActivity == nil || record.ParentRecordId == "" || record.Provenance == nil {
			continue
		}
		if _, ok := byID[record.ParentRecordId]; !ok {
			continue
		}
		for _, sourceID := range record.Provenance.SourceRecordIds {
			source := byID[sourceID]
			if source == nil {
				continue
			}
			switch source.Kind {
			case remotev1.AuditKind_AUDIT_KIND_CS_WIRE_FRAME:
				csLinked = csLinked || source.ConnectionId != "" && source.ClientId != "" && source.ClientRunId != "" && source.RequestId != ""
			case remotev1.AuditKind_AUDIT_KIND_APP_SERVER_WIRE:
				appLinked = appLinked || source.TurnId != "" || source.ThreadId != ""
			}
		}
	}
	return csLinked && appLinked
}

func startAndWaitCompletion(t *testing.T, c *wireClient, codexID string) uint64 {
	t.Helper()
	id := "audit-start-" + time.Now().Format("150405.000000000")
	c.request(t, request(id, &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "audit"}}}}}}))
	for {
		ev := c.readUntil(t, func(f *remotev1.Frame) bool { return f.GetEvent() != nil }).GetEvent()
		if turn := ev.GetTurnUpdated(); turn != nil && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			return ev.EventSeq
		}
	}
}
