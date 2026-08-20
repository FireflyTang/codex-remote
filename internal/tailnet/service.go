// Package tailnet owns the embedded Tailscale node used by the Host.
package tailnet

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

type State string

const (
	StateStopped      State = "stopped"
	StateStarting     State = "starting"
	StateAuthRequired State = "auth_required"
	StateConnecting   State = "connecting"
	StateReady        State = "ready"
	StateDegraded     State = "degraded"
	StateStopping     State = "stopping"
)

type Config struct {
	Hostname string
	StateDir string
	// ListenAddr is a tsnet address, normally ":80". It never binds a host NIC.
	ListenAddr string
	AuthKey    string
	Verbose    bool
	Logger     *log.Logger
	OnAuthURL  func(string)
}

type AuditEvent struct {
	Operation string
	Outcome   string
	Message   string
	Metadata  map[string]string
}

type AuditSink interface {
	RecordTailnet(context.Context, AuditEvent) error
}

type Status struct {
	State      State
	Hostname   string
	ListenAddr string
	AuthURL    string
	TailnetIPs []netip.Addr
	LastError  string
}

type PeerIdentity struct {
	NodeID      string
	NodeName    string
	UserID      string
	LoginName   string
	DisplayName string
	Tags        []string
	RemoteAddr  string
}

// Service is independent from the Codex runtime lifecycle. It never starts or
// discovers a system tailscaled and has no host-network fallback.
type Service struct {
	cfg   Config
	audit AuditSink
	srv   *tsnet.Server

	mu       sync.RWMutex
	state    State
	authURL  string
	lastErr  error
	status   *ipnstate.Status
	listener net.Listener
}

var authURLPattern = regexp.MustCompile(`https://login\.tailscale\.com/[^\s]+`)

func New(cfg Config, audit AuditSink) (*Service, error) {
	if strings.TrimSpace(cfg.Hostname) == "" {
		return nil, errors.New("tailnet hostname is required")
	}
	if strings.TrimSpace(cfg.StateDir) == "" {
		return nil, errors.New("tailnet state directory is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":80"
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "tailnet: ", log.LstdFlags)
	}
	return &Service{cfg: cfg, audit: audit, state: StateStopped}, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.state != StateStopped {
		s.mu.Unlock()
		return fmt.Errorf("tailnet cannot start from %s", s.state)
	}
	s.state = StateStarting
	s.mu.Unlock()

	if err := os.MkdirAll(s.cfg.StateDir, 0o700); err != nil {
		return s.fail(ctx, "tailnet.start", err)
	}
	if err := os.Chmod(s.cfg.StateDir, 0o700); err != nil {
		return s.fail(ctx, "tailnet.start", err)
	}

	server := &tsnet.Server{Hostname: s.cfg.Hostname, Dir: filepath.Clean(s.cfg.StateDir), AuthKey: s.cfg.AuthKey}
	server.Logf = func(format string, args ...any) {
		message := fmt.Sprintf(format, args...)
		if s.cfg.Verbose {
			s.cfg.Logger.Printf("%s", message)
		}
		if u := authURLPattern.FindString(message); u != "" {
			s.setAuthURL(ctx, u)
		}
	}
	s.mu.Lock()
	s.srv = server
	s.state = StateConnecting
	s.mu.Unlock()
	s.record(ctx, "tailnet.start", "started", "embedded tailnet node starting", nil)
	if err := server.Start(); err != nil {
		return s.fail(ctx, "tailnet.start", err)
	}
	return nil
}

// WaitReady waits for login/approval and network readiness. Start may return
// before authentication completes; Up supplies the interactive login URL via
// the server log callback and continues once the node is usable.
func (s *Service) WaitReady(ctx context.Context) error {
	s.mu.RLock()
	server := s.srv
	s.mu.RUnlock()
	if server == nil {
		return errors.New("tailnet is not started")
	}
	st, err := server.Up(ctx)
	if err != nil {
		return s.fail(ctx, "tailnet.wait_ready", err)
	}
	s.mu.Lock()
	s.status = st
	s.state = StateReady
	s.authURL = ""
	s.lastErr = nil
	s.mu.Unlock()
	s.record(ctx, "tailnet.auth_succeeded", "succeeded", "tailnet node authenticated", nil)
	s.record(ctx, "tailnet.ready", "succeeded", "tailnet node ready", nil)
	return nil
}

func (s *Service) Listen(ctx context.Context) (net.Listener, error) {
	s.mu.RLock()
	server, state := s.srv, s.state
	s.mu.RUnlock()
	if server == nil || state != StateReady {
		return nil, fmt.Errorf("tailnet is not ready: %s", state)
	}
	ln, err := server.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return nil, s.fail(ctx, "tailnet.listener_started", err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	s.record(ctx, "tailnet.listener_started", "succeeded", "plain TCP listener started inside tailnet", map[string]string{"address": s.cfg.ListenAddr})
	return &auditListener{Listener: ln, svc: s}, nil
}

func (s *Service) WhoIs(ctx context.Context, remoteAddr string) (PeerIdentity, error) {
	s.mu.RLock()
	server := s.srv
	s.mu.RUnlock()
	if server == nil {
		return PeerIdentity{}, errors.New("tailnet is not started")
	}
	lc, err := server.LocalClient()
	if err != nil {
		s.record(ctx, "tailnet.whois_failed", "failed", err.Error(), map[string]string{"remote_addr": remoteAddr})
		return PeerIdentity{}, err
	}
	who, err := lc.WhoIs(ctx, remoteAddr)
	if err != nil {
		s.record(ctx, "tailnet.whois_failed", "failed", err.Error(), map[string]string{"remote_addr": remoteAddr})
		return PeerIdentity{}, err
	}
	identity := PeerIdentity{RemoteAddr: remoteAddr}
	if who.Node != nil {
		identity.NodeID = fmt.Sprint(who.Node.ID)
		identity.NodeName = who.Node.Name
		identity.Tags = append([]string(nil), who.Node.Tags...)
	}
	if who.UserProfile != nil {
		identity.UserID = fmt.Sprint(who.UserProfile.ID)
		identity.LoginName = who.UserProfile.LoginName
		identity.DisplayName = who.UserProfile.DisplayName
	}
	s.record(ctx, "tailnet.whois_succeeded", "succeeded", "tailnet peer identified", map[string]string{"remote_addr": remoteAddr, "node_id": identity.NodeID, "user_id": identity.UserID})
	return identity, nil
}

func (s *Service) LocalClient() (*local.Client, error) {
	s.mu.RLock()
	server := s.srv
	s.mu.RUnlock()
	if server == nil {
		return nil, errors.New("tailnet is not started")
	}
	return server.LocalClient()
}

func (s *Service) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Status{State: s.state, Hostname: s.cfg.Hostname, ListenAddr: s.cfg.ListenAddr, AuthURL: s.authURL}
	if s.status != nil {
		out.TailnetIPs = append(out.TailnetIPs, s.status.TailscaleIPs...)
	}
	if s.lastErr != nil {
		out.LastError = s.lastErr.Error()
	}
	return out
}

func (s *Service) Close() error {
	s.mu.Lock()
	if s.state == StateStopped {
		s.mu.Unlock()
		return nil
	}
	s.state = StateStopping
	ln, server := s.listener, s.srv
	s.listener = nil
	s.mu.Unlock()
	var errs []error
	if ln != nil {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	if server != nil {
		if err := server.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.mu.Lock()
	s.state = StateStopped
	s.srv = nil
	s.status = nil
	s.mu.Unlock()
	s.record(context.Background(), "tailnet.stopped", "succeeded", "tailnet node stopped", nil)
	return errors.Join(errs...)
}

func (s *Service) setAuthURL(ctx context.Context, url string) {
	s.mu.Lock()
	changed := s.authURL != url
	s.authURL = url
	s.state = StateAuthRequired
	s.mu.Unlock()
	if !changed {
		return
	}
	// The URL is deliberately emitted to the interactive console only; audit
	// records state but never persist the credential-bearing URL.
	s.cfg.Logger.Printf("authentication required: %s", url)
	if s.cfg.OnAuthURL != nil {
		s.cfg.OnAuthURL(url)
	}
	s.record(ctx, "tailnet.auth_required", "started", "tailnet authentication required; see host console", nil)
}

func (s *Service) fail(ctx context.Context, operation string, err error) error {
	s.mu.Lock()
	s.state = StateDegraded
	s.lastErr = err
	s.mu.Unlock()
	s.record(ctx, operation, "failed", err.Error(), nil)
	return err
}

func (s *Service) record(ctx context.Context, operation, outcome, message string, metadata map[string]string) {
	if s.audit != nil {
		_ = s.audit.RecordTailnet(ctx, AuditEvent{Operation: operation, Outcome: outcome, Message: message, Metadata: metadata})
	}
}

type auditListener struct {
	net.Listener
	svc *Service
}

func (l *auditListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.svc.record(context.Background(), "tailnet.connection_accepted", "succeeded", "tailnet TCP connection accepted", map[string]string{"remote_addr": c.RemoteAddr().String()})
	return c, nil
}

// DefaultHostname provides a stable default as long as Server.Dir is reused.
func DefaultHostname() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "codex-remote"
	}
	host = strings.ToLower(host)
	host = regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(host, "-")
	host = strings.Trim(host, "-")
	if host == "" {
		return "codex-remote"
	}
	return "codex-remote-" + host
}

// ReadyContext is useful to bound interactive authentication in callers/tests.
func ReadyContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}
