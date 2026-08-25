package tailnet

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tailscale.com/net/tshttpproxy"
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
	if len(capture.events) != 2 || capture.events[1].Operation != "tailnet.auth_required" || strings.Contains(capture.events[1].Message, "a-secret") {
		t.Fatalf("URL leaked to audit %+v", capture.events)
	}
	if !errors.Is(s.Close(), nil) {
		t.Fatal("close")
	}
}

func TestEmbeddedTailscaleProxyPolicyIsDirectWithoutMutatingEnvironment(t *testing.T) {
	wantEnvironment := map[string]string{
		"http_proxy":  "http://127.0.0.1:7897",
		"https_proxy": "http://127.0.0.1:7897",
		"all_proxy":   "socks5://127.0.0.1:7897",
	}
	for key, value := range wantEnvironment {
		t.Setenv(key, value)
	}

	capture := new(auditCapture)
	if _, err := New(Config{Hostname: "demo", StateDir: t.TempDir(), Logger: log.Default()}, capture); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, "https://derp.example:57991/derp", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := tshttpproxy.ProxyFromEnvironment(req)
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL != nil {
		t.Fatalf("embedded Tailscale unexpectedly selected proxy %s", proxyURL)
	}
	for key, want := range wantEnvironment {
		if got := os.Getenv(key); got != want {
			t.Fatalf("%s changed: got %q want %q", key, got, want)
		}
	}
	if len(capture.events) != 1 || capture.events[0].Operation != "tailnet.proxy_policy" || capture.events[0].Metadata["policy"] != "direct" {
		t.Fatalf("missing direct proxy policy audit: %+v", capture.events)
	}
}
