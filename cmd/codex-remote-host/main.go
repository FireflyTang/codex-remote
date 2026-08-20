package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/audit"
	"github.com/kylin1993/codex-remote/internal/capability"
	"github.com/kylin1993/codex-remote/internal/codex"
	"github.com/kylin1993/codex-remote/internal/directory"
	"github.com/kylin1993/codex-remote/internal/gateway"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"github.com/kylin1993/codex-remote/internal/runtime"
	"github.com/kylin1993/codex-remote/internal/tailnet"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

const version = "0.1.0"

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, " ") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

type config struct {
	dataDir, auditDir, hostname, authKeyEnv, appExecutable, socketPath, devListen string
	appArgs                                                                       stringList
	heartbeat, timeout                                                            time.Duration
	sendQueue, watchQueue, maxFrame, replayCapacity, maxPage                      int
}

func main() {
	if err := run(); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func run() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	cfg := config{}
	flag.StringVar(&cfg.dataDir, "state-dir", filepath.Join(home, ".codex-remote"), "Host state directory")
	flag.StringVar(&cfg.auditDir, "audit-dir", "", "diagnostic audit directory (default <state-dir>/audit)")
	flag.StringVar(&cfg.hostname, "hostname", tailnet.DefaultHostname(), "stable embedded Tailnet node hostname")
	flag.StringVar(&cfg.authKeyEnv, "auth-key-env", "TS_AUTHKEY", "environment variable containing optional Tailscale auth key")
	flag.StringVar(&cfg.appExecutable, "app-server-executable", "codex", "Codex or deterministic fake app-server executable")
	flag.Var(&cfg.appArgs, "app-server-arg", "app-server argument; repeat for multiple arguments")
	flag.StringVar(&cfg.socketPath, "app-server-socket", "", "app-server Unix WebSocket path (default <state-dir>/run/app-server.sock)")
	flag.StringVar(&cfg.devListen, "dev-listen", "", "DEV/TEST ONLY host TCP address (for example 127.0.0.1:0); bypasses tsnet and is never an automatic fallback")
	flag.DurationVar(&cfg.heartbeat, "heartbeat", 15*time.Second, "application heartbeat interval")
	flag.DurationVar(&cfg.timeout, "connection-timeout", 45*time.Second, "connection inactivity timeout")
	flag.IntVar(&cfg.sendQueue, "send-queue", 256, "bounded connection send queue")
	flag.IntVar(&cfg.watchQueue, "watch-queue", 128, "bounded per-watch live queue")
	flag.IntVar(&cfg.maxFrame, "max-frame-bytes", 4<<20, "maximum ProtoJSON frame bytes")
	flag.IntVar(&cfg.replayCapacity, "replay-capacity", 1024, "same-run events retained per Codex")
	flag.IntVar(&cfg.maxPage, "max-page-size", 100, "maximum list/history page size")
	flag.Parse()
	if cfg.auditDir == "" {
		cfg.auditDir = filepath.Join(cfg.dataDir, "audit")
	}
	if cfg.socketPath == "" {
		cfg.socketPath = filepath.Join(cfg.dataDir, "run", "app-server.sock")
	}
	if err := os.MkdirAll(cfg.dataDir, 0o700); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hostID, err := stableID(filepath.Join(cfg.dataDir, "host-id"))
	if err != nil {
		return err
	}
	runID := ephemeralID()
	recorder, err := audit.New(audit.Config{Dir: cfg.auditDir, ProcessRunID: runID})
	if err != nil {
		log.Printf("diagnostic audit unavailable at startup: %v", err)
		recorder = audit.NewDegraded(runID, err)
	}
	reportAudit := func(err error) {
		if err != nil {
			log.Printf("diagnostic audit degraded: %v", err)
		}
	}
	defer func() { reportAudit(recorder.Close()) }()
	reportAudit(recorder.Record(ctx, &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_PROCESS_LIFECYCLE, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: remotev1.AuditOutcome_AUDIT_OUTCOME_STARTED, Component: "host", Operation: "host.start", Message: "Codex Remote Host starting"}))
	store, err := persistence.Open(filepath.Join(cfg.dataDir, "state.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	var runtimeManager *runtime.Manager
	wireObserver := func(direction adapter.WireDirection, raw []byte) {
		in := direction == adapter.WireIn
		reportAudit(recorder.RecordAppServerWire(context.Background(), in, raw, "", "", ""))
	}
	args := []string(cfg.appArgs)
	if len(args) == 0 {
		args = []string{"app-server"}
	}
	runtimeManager = runtime.New(runtime.Config{Executable: cfg.appExecutable, BaseArgs: args, SocketPath: cfg.socketPath, WireObserver: wireObserver, Stderr: func(line string) {
		reportAudit(recorder.RecordRuntime(context.Background(), "stderr", []byte(line), false))
	}})
	reportAudit(recorder.Record(ctx, hostAction("runtime.start", remotev1.AuditOutcome_AUDIT_OUTCOME_STARTED, "starting app-server runtime")))
	if err := runtimeManager.Start(ctx); err != nil {
		reportAudit(recorder.Record(ctx, hostAction("runtime.start", remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED, err.Error())))
		return fmt.Errorf("start app-server: %w", err)
	}
	reportAudit(recorder.Record(ctx, hostAction("runtime.start", remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, "app-server runtime ready")))
	defer func() {
		reportAudit(recorder.Record(context.Background(), hostAction("runtime.stop", remotev1.AuditOutcome_AUDIT_OUTCOME_STARTED, "stopping app-server runtime")))
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closeErr := runtimeManager.Close(closeCtx)
		outcome := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
		message := "app-server runtime stopped"
		if closeErr != nil {
			outcome = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
			message = closeErr.Error()
		}
		reportAudit(recorder.Record(context.Background(), hostAction("runtime.stop", outcome, message)))
	}()
	eventStore := activity.NewStore(store, recorder, cfg.replayCapacity)
	caps := capability.New(uint32(64), uint32(cfg.maxPage))
	manager := codex.NewManager(runtimeManager, store, eventStore, directory.Service{}, caps, hostID, version)
	manager.MaxPage = uint32(cfg.maxPage)
	manager.ContentBudget = cfg.maxFrame / 2
	manager.Degraded = recorder.Degraded
	if err := manager.Restore(ctx); err != nil {
		return fmt.Errorf("restore managed sessions: %w", err)
	}
	go manager.RunEvents(ctx)
	go func() {
		for st := range runtimeManager.States() {
			outcome := remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED
			if st.Status == runtime.StatusFailed {
				outcome = remotev1.AuditOutcome_AUDIT_OUTCOME_FAILED
			}
			message := ""
			if st.Err != nil {
				message = st.Err.Error()
			}
			reportAudit(recorder.Record(context.Background(), hostAction("runtime.state."+string(st.Status), outcome, message)))
			if st.Status == runtime.StatusReady {
				if err := manager.Restore(context.Background()); err != nil {
					log.Printf("runtime restore degraded: %v", err)
				}
			}
		}
	}()
	dispatcher := &gateway.Dispatcher{Backend: manager, Dedup: store}
	gwcfg := gateway.ServerConfig{MaxFrameBytes: int64(cfg.maxFrame), SendQueueSize: cfg.sendQueue, WatchQueueSize: cfg.watchQueue, HeartbeatInterval: cfg.heartbeat, ConnectionTimeout: cfg.timeout, MaxWatches: 64, HostID: hostID, HostRunID: runID, HostVersion: version, AuditError: reportAudit, Hello: func(ctx context.Context) (*remotev1.ServerHello, error) {
		h, err := manager.GetHost(ctx, &remotev1.GetHostRequest{})
		if err != nil {
			return nil, err
		}
		return &remotev1.ServerHello{HostStatus: h.Host.Status, HostVersion: version, Runtime: h.Host.Runtime, Capabilities: h.Capabilities}, nil
	}}
	var ln net.Listener
	var tail *tailnet.Service
	var identity gateway.IdentityProvider
	if cfg.devListen != "" {
		if !isLoopbackAddress(cfg.devListen) {
			return errors.New("--dev-listen is test-only and must use a loopback address")
		}
		log.Printf("WARNING: --dev-listen bypasses embedded tsnet for tests only")
		ln, err = net.Listen("tcp", cfg.devListen)
		if err != nil {
			return err
		}
		identity = devIdentity{}
		log.Printf("LISTEN_URL=ws://%s/connect", ln.Addr())
	} else {
		tail, err = tailnet.New(tailnet.Config{Hostname: cfg.hostname, StateDir: filepath.Join(cfg.dataDir, "tailscale"), AuthKey: os.Getenv(cfg.authKeyEnv), Logger: log.Default()}, recorder)
		if err != nil {
			return err
		}
		if err = tail.Start(ctx); err != nil {
			return err
		}
		defer tail.Close()
		if err = tail.WaitReady(ctx); err != nil {
			return err
		}
		ln, err = tail.Listen(ctx)
		if err != nil {
			return err
		}
		identity = tail
		log.Printf("LISTEN_URL=ws://%s/connect", cfg.hostname)
	}
	gw := gateway.NewServer(gwcfg, dispatcher, eventStore, identity, recorder)
	reportAudit(recorder.Record(ctx, hostAction("gateway.serve", remotev1.AuditOutcome_AUDIT_OUTCOME_STARTED, ln.Addr().String())))
	serveErr := make(chan error, 1)
	go func() { serveErr <- gw.Serve(ln) }()
	select {
	case err = <-serveErr:
		if err != nil {
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = gw.Shutdown(shutdownCtx)
	_ = ln.Close()
	reportAudit(recorder.Record(context.Background(), hostAction("gateway.serve", remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, "gateway stopped")))
	reportAudit(recorder.Record(context.Background(), &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_PROCESS_LIFECYCLE, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: remotev1.AuditOutcome_AUDIT_OUTCOME_SUCCEEDED, Component: "host", Operation: "host.stop", Message: "Codex Remote Host stopped"}))
	return nil
}

func hostAction(operation string, outcome remotev1.AuditOutcome, message string) *remotev1.AuditRecord {
	return &remotev1.AuditRecord{Kind: remotev1.AuditKind_AUDIT_KIND_HOST_ACTION, Direction: remotev1.AuditDirection_AUDIT_DIRECTION_INTERNAL, Outcome: outcome, Component: "host", Operation: operation, Message: message}
}

type devIdentity struct{}

func (devIdentity) WhoIs(_ context.Context, remote string) (tailnet.PeerIdentity, error) {
	return tailnet.PeerIdentity{NodeID: "dev-only", NodeName: "loopback", UserID: "dev-only", LoginName: "dev-only", RemoteAddr: remote}, nil
}
func isLoopbackAddress(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func stableID(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(b)) != "" {
		return strings.TrimSpace(string(b)), nil
	}
	id := "host_" + ephemeralID()
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", err
	}
	return id, nil
}
func ephemeralID() string { return fmt.Sprintf("%d", time.Now().UnixNano()) }
