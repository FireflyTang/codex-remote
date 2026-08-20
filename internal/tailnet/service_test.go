package tailnet

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"testing"
)

type auditCapture struct{ events []AuditEvent }

func (a *auditCapture) RecordTailnet(_ context.Context, e AuditEvent) error {
	a.events = append(a.events, e)
	return nil
}

func TestConfigAndDefaultHostname(t *testing.T) {
	if _, err := New(Config{}, nil); err == nil {
		t.Fatal("expected required hostname")
	}
	h := DefaultHostname()
	if h == "" || strings.ContainsAny(h, " _/") {
		t.Fatalf("bad hostname %q", h)
	}
	s, err := New(Config{Hostname: "demo", StateDir: filepath.Join(t.TempDir(), "state"), Logger: log.Default()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	st := s.Status()
	if st.State != StateStopped || st.ListenAddr != ":80" {
		t.Fatalf("unexpected status %+v", st)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadyRequiresStartAndAuthURLIsNotAudited(t *testing.T) {
	capture := new(auditCapture)
	var shown string
	s, err := New(Config{Hostname: "demo", StateDir: t.TempDir(), OnAuthURL: func(v string) { shown = v }, Logger: log.Default()}, capture)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.WaitReady(context.Background()); err == nil {
		t.Fatal("expected not started")
	}
	s.setAuthURL(context.Background(), "https://login.tailscale.com/a-secret")
	if shown == "" {
		t.Fatal("auth callback not invoked")
	}
	if len(capture.events) != 1 || strings.Contains(capture.events[0].Message, "a-secret") {
		t.Fatalf("URL leaked to audit %+v", capture.events)
	}
	if !errors.Is(s.Close(), nil) {
		t.Fatal("close")
	}
}
