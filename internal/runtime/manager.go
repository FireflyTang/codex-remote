package runtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/kylin1993/codex-remote/internal/adapter"
)

var ErrRestartDeferred = errors.New("restart scheduled until runtime is idle")
var ErrNotReady = errors.New("app-server is not ready")

type Status string

const (
	StatusStarting   Status = "starting"
	StatusReady      Status = "ready"
	StatusRestarting Status = "restarting"
	StatusStopped    Status = "stopped"
	StatusFailed     Status = "failed"
)

type State struct {
	Status       Status
	RestartCount uint64
	StartedAt    time.Time
	AppServer    adapter.InitializeResult
	Err          error
}
type Config struct {
	Executable         string
	BaseArgs           []string
	SocketPath         string
	StartTimeout       time.Duration
	ConnectInterval    time.Duration
	MaxRestarts        int
	Backoff            []time.Duration
	StopTimeout        time.Duration
	WireObserver       adapter.WireObserver
	AppServerReadLimit int64
	Stderr             func(string)
}

type Manager struct {
	cfg            Config
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	state          State
	current        *adapter.Adapter
	cmd            *exec.Cmd
	planned        bool
	plannedWatcher bool
	stopping       bool
	states         chan State
	events         chan adapter.Event
	wake           chan struct{}
	done           chan struct{}
	forwardWG      sync.WaitGroup
}

func New(cfg Config) *Manager {
	if cfg.Executable == "" {
		cfg.Executable = "codex"
	}
	if len(cfg.BaseArgs) == 0 {
		cfg.BaseArgs = []string{"app-server"}
	}
	if cfg.StartTimeout == 0 {
		cfg.StartTimeout = 10 * time.Second
	}
	if cfg.ConnectInterval == 0 {
		cfg.ConnectInterval = 25 * time.Millisecond
	}
	if cfg.MaxRestarts == 0 {
		cfg.MaxRestarts = 3
	}
	if len(cfg.Backoff) == 0 {
		cfg.Backoff = []time.Duration{50 * time.Millisecond, 200 * time.Millisecond, time.Second}
	}
	if cfg.StopTimeout == 0 {
		cfg.StopTimeout = 2 * time.Second
	}
	return &Manager{cfg: cfg, state: State{Status: StatusStopped}, states: make(chan State, 32), events: make(chan adapter.Event, 256), wake: make(chan struct{}, 1), done: make(chan struct{})}
}
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.ctx != nil {
		m.mu.Unlock()
		return errors.New("runtime already started")
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.mu.Unlock()
	if err := m.startInstance(ctx, StatusStarting); err != nil {
		m.setState(State{Status: StatusFailed, Err: err})
		m.cancel()
		close(m.done)
		close(m.events)
		close(m.states)
		return err
	}
	go m.supervise()
	return nil
}
func (m *Manager) Adapter() (*adapter.Adapter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state.Status != StatusReady || m.current == nil {
		return nil, ErrNotReady
	}
	return m.current, nil
}
func (m *Manager) State() State                 { m.mu.RLock(); defer m.mu.RUnlock(); return m.state }
func (m *Manager) States() <-chan State         { return m.states }
func (m *Manager) Events() <-chan adapter.Event { return m.events }
func (m *Manager) Done() <-chan struct{}        { return m.done }

func (m *Manager) RequestRestart() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopping {
		return errors.New("runtime stopping")
	}
	if m.current != nil && m.current.ActiveTurnCount() > 0 {
		m.planned = true
		if !m.plannedWatcher {
			m.plannedWatcher = true
			ad := m.current
			go func() {
				select {
				case <-ad.Idle():
					select {
					case m.wake <- struct{}{}:
					default:
					}
				case <-m.ctx.Done():
				}
			}()
		}
		return ErrRestartDeferred
	}
	m.planned = true
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.ctx == nil {
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	if m.cancel != nil {
		m.cancel()
	}
	cmd := m.cmd
	client := m.current
	m.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return ctx.Err()
	}
}

func (m *Manager) supervise() {
	defer close(m.done)
	defer close(m.states)
	defer func() { m.forwardWG.Wait(); close(m.events) }()
	for {
		m.mu.RLock()
		cmd := m.cmd
		ad := m.current
		ctx := m.ctx
		m.mu.RUnlock()
		if cmd == nil {
			return
		}
		exited := make(chan error, 1)
		go func(c *exec.Cmd) { exited <- c.Wait() }(cmd)
		var exitErr error
		planned := false
	connectionLoop:
		for {
			select {
			case exitErr = <-exited:
				m.setState(State{Status: StatusRestarting, RestartCount: m.State().RestartCount, Err: exitErr})
				break connectionLoop
			case <-ctx.Done():
				_ = cmd.Process.Signal(syscall.SIGTERM)
				waitForProcess(cmd, exited, m.cfg.StopTimeout)
				m.setState(State{Status: StatusStopped})
				return
			case <-m.wake:
				planned = true
				m.setState(State{Status: StatusRestarting, RestartCount: m.State().RestartCount})
				_ = cmd.Process.Signal(syscall.SIGTERM)
				waitForProcess(cmd, exited, m.cfg.StopTimeout)
				break connectionLoop
			case <-ad.Done():
				select {
				case <-ctx.Done():
					_ = cmd.Process.Signal(syscall.SIGTERM)
					waitForProcess(cmd, exited, m.cfg.StopTimeout)
					m.setState(State{Status: StatusStopped})
					return
				default:
				}
				previous := m.State()
				previous.Status = StatusRestarting
				previous.Err = adapter.ErrDisconnected
				m.setState(previous)
				m.mu.Lock()
				m.current = nil
				m.mu.Unlock()
				var reconnected bool
				for attempt := 0; attempt <= m.cfg.MaxRestarts; attempt++ {
					if attempt > 0 {
						delay := m.cfg.Backoff[min(attempt-1, len(m.cfg.Backoff)-1)]
						select {
						case <-time.After(delay):
						case exitErr = <-exited:
							break connectionLoop
						case <-ctx.Done():
							_ = cmd.Process.Signal(syscall.SIGTERM)
							waitForProcess(cmd, exited, m.cfg.StopTimeout)
							m.setState(State{Status: StatusStopped})
							return
						}
					}
					newAdapter, info, err := m.connectAdapter(ctx)
					if err != nil {
						exitErr = err
						continue
					}
					m.mu.Lock()
					m.current = newAdapter
					m.state = State{Status: StatusReady, RestartCount: m.state.RestartCount, StartedAt: previous.StartedAt, AppServer: info}
					state := m.state
					m.mu.Unlock()
					m.publishState(state)
					m.forwardWG.Add(1)
					go m.forwardEvents(newAdapter)
					ad = newAdapter
					reconnected = true
					break
				}
				if reconnected {
					continue connectionLoop
				}
				_ = cmd.Process.Signal(syscall.SIGTERM)
				waitForProcess(cmd, exited, m.cfg.StopTimeout)
				break connectionLoop
			}
		}
		_ = ad.Close()
		if exitErr == nil && !planned {
			exitErr = errors.New("app-server exited")
		}
		m.mu.Lock()
		m.current = nil
		m.cmd = nil
		m.mu.Unlock()
		for attempt := 0; ; attempt++ {
			if !planned || attempt > 0 {
				delay := m.cfg.Backoff[min(attempt, len(m.cfg.Backoff)-1)]
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					m.setState(State{Status: StatusStopped})
					return
				}
			}
			err := m.startInstance(ctx, StatusRestarting)
			if err == nil {
				break
			}
			exitErr = err
			if attempt >= m.cfg.MaxRestarts {
				m.setState(State{Status: StatusFailed, RestartCount: m.State().RestartCount, Err: err})
				return
			}
		}
		m.mu.Lock()
		m.planned = false
		m.plannedWatcher = false
		m.mu.Unlock()
	}
}

func waitForProcess(cmd *exec.Cmd, exited <-chan error, timeout time.Duration) {
	select {
	case <-exited:
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-exited
	}
}

func (m *Manager) startInstance(parent context.Context, status Status) error {
	m.mu.RLock()
	restartCount := m.state.RestartCount
	m.mu.RUnlock()
	if status == StatusRestarting {
		restartCount++
	}
	m.setState(State{Status: status, RestartCount: restartCount})
	if m.cfg.SocketPath == "" {
		return errors.New("socket path is required")
	}
	if err := os.MkdirAll(filepath.Dir(m.cfg.SocketPath), 0o755); err != nil {
		return err
	}
	if err := removeStaleSocket(m.cfg.SocketPath); err != nil {
		return err
	}
	args := append([]string(nil), m.cfg.BaseArgs...)
	args = append(args, "--listen", "unix://"+m.cfg.SocketPath)
	cmd := exec.Command(m.cfg.Executable, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go m.captureStderr(stderr)
	ad, info, err := m.connectAdapter(parent)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	m.mu.Lock()
	m.cmd = cmd
	m.current = ad
	started := time.Now()
	m.state = State{Status: StatusReady, RestartCount: restartCount, StartedAt: started, AppServer: info}
	state := m.state
	m.mu.Unlock()
	m.publishState(state)
	m.forwardWG.Add(1)
	go m.forwardEvents(ad)
	return nil
}

func (m *Manager) connectAdapter(parent context.Context) (*adapter.Adapter, adapter.InitializeResult, error) {
	startCtx, cancel := context.WithTimeout(parent, m.cfg.StartTimeout)
	defer cancel()
	var client *adapter.Client
	var err error
	for {
		client, err = adapter.DialWithOptions(startCtx, m.cfg.SocketPath, m.cfg.WireObserver, adapter.DialOptions{ReadLimit: m.cfg.AppServerReadLimit})
		if err == nil {
			break
		}
		select {
		case <-startCtx.Done():
			return nil, adapter.InitializeResult{}, fmt.Errorf("wait for app-server socket: %w", startCtx.Err())
		case <-time.After(m.cfg.ConnectInterval):
		}
	}
	ad, info, err := adapter.Initialize(startCtx, client)
	if err != nil {
		_ = client.Close()
		return nil, info, fmt.Errorf("initialize app-server: %w", err)
	}
	return ad, info, nil
}
func (m *Manager) forwardEvents(ad *adapter.Adapter) {
	defer m.forwardWG.Done()
	for e := range ad.Events() {
		select {
		case m.events <- e:
		case <-m.ctx.Done():
			return
		}
	}
}
func (m *Manager) captureStderr(r io.Reader) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		if m.cfg.Stderr != nil {
			m.cfg.Stderr(s.Text())
		}
	}
}
func (m *Manager) setState(s State) { m.mu.Lock(); m.state = s; m.mu.Unlock(); m.publishState(s) }
func (m *Manager) publishState(s State) {
	select {
	case m.states <- s:
	default:
	}
}
func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("socket path exists and is not a socket: %s", path)
	}
	c, err := netDialUnix(path, 50*time.Millisecond)
	if err == nil {
		_ = c.Close()
		return fmt.Errorf("socket already has a listener: %s", path)
	}
	return os.Remove(path)
}

var netDialUnix = func(path string, timeout time.Duration) (io.Closer, error) {
	return (&netDialer{timeout: timeout}).dial(path)
}

type netDialer struct{ timeout time.Duration }

func (d *netDialer) dial(path string) (io.Closer, error) { return netDial(path, d.timeout) }
