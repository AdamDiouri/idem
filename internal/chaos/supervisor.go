package chaos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

const (
	restartDelay = 500 * time.Millisecond
	gracefulWait = 10 * time.Second
	stopAllWait  = 5 * time.Second
)

// ProcSpec describes a child process.
type ProcSpec struct {
	Name    string
	Path    string
	Args    []string
	Env     []string // added to the parent's environment
	LogPath string   // stdout and stderr are appended here
}

// Supervisor runs child processes and restarts any that exit unexpectedly,
// like an orchestrator restarting a crashed pod.
type Supervisor struct {
	logger *slog.Logger

	mu      sync.Mutex
	procs   []*Proc
	stopped bool
}

// NewSupervisor returns an empty supervisor.
func NewSupervisor(logger *slog.Logger) *Supervisor {
	return &Supervisor{logger: logger}
}

// Proc is one supervised child. Its PID changes when it is restarted.
type Proc struct {
	sup  *Supervisor
	spec ProcSpec

	mu         sync.Mutex
	cmd        *exec.Cmd
	exited     chan struct{} // closed when the current process exits
	deliberate bool          // the current exit is intended: do not restart
	paused     bool
	restarts   int
}

// Start launches spec under supervision.
func (s *Supervisor) Start(spec ProcSpec) (*Proc, error) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil, errors.New("supervisor is stopped")
	}
	p := &Proc{sup: s, spec: spec}
	s.procs = append(s.procs, p)
	s.mu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.launchLocked(); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Supervisor) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// launchLocked starts a fresh process for p. Callers hold p.mu.
func (p *Proc) launchLocked() error {
	logFile, err := os.OpenFile(p.spec.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644) //nolint:gosec // harness log file
	if err != nil {
		return fmt.Errorf("open log for %s: %w", p.spec.Name, err)
	}
	cmd := exec.Command(p.spec.Path, p.spec.Args...) //nolint:gosec // harness-controlled binaries
	cmd.Env = append(os.Environ(), p.spec.Env...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	// Own process group, so signals reach the child and anything it spawns.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start %s: %w", p.spec.Name, err)
	}

	exited := make(chan struct{})
	p.cmd, p.exited, p.deliberate, p.paused = cmd, exited, false, false
	go p.monitor(cmd, exited, logFile)
	return nil
}

// monitor waits for one process; if its exit was not deliberate, it restarts it.
func (p *Proc) monitor(cmd *exec.Cmd, exited chan struct{}, logFile *os.File) {
	err := cmd.Wait()
	_ = logFile.Close()
	close(exited)

	p.mu.Lock()
	restart := !p.deliberate && p.cmd == cmd && !p.sup.isStopped()
	p.mu.Unlock()
	if !restart {
		return
	}
	p.sup.logger.Warn("child exited; restarting", "proc", p.spec.Name, "pid", cmd.Process.Pid, "error", err)
	time.Sleep(restartDelay)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != cmd || p.sup.isStopped() { // replaced or stopped meanwhile
		return
	}
	p.restarts++
	if err := p.launchLocked(); err != nil {
		p.sup.logger.Error("restart failed", "proc", p.spec.Name, "error", err)
	}
}

// Name returns the process name.
func (p *Proc) Name() string { return p.spec.Name }

// PID returns the current process ID.
func (p *Proc) PID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cmd.Process.Pid
}

// Restarts counts restarts after unexpected exits (crashes and kills).
func (p *Proc) Restarts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.restarts
}

func (p *Proc) signal(sig syscall.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.signalLocked(sig)
}

func (p *Proc) signalLocked(sig syscall.Signal) error {
	if err := syscall.Kill(-p.cmd.Process.Pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("signal %s to %s: %w", sig, p.spec.Name, err)
	}
	return nil
}

// Kill sends SIGKILL; the supervisor restarts the process (a crash).
func (p *Proc) Kill() error { return p.signal(syscall.SIGKILL) }

// Pause freezes the process (SIGSTOP), like a long GC pause or a frozen VM.
func (p *Proc) Pause() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	return p.signalLocked(syscall.SIGSTOP)
}

// Resume unfreezes the process (SIGCONT).
func (p *Proc) Resume() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	return p.signalLocked(syscall.SIGCONT)
}

// Restart stops the process gracefully (SIGTERM, then SIGKILL after 10s) and
// starts it again. It is deliberate, so it does not count as a restart.
func (p *Proc) Restart(ctx context.Context) error {
	if err := p.stop(ctx, gracefulWait); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sup.isStopped() {
		return errors.New("supervisor is stopped")
	}
	return p.launchLocked()
}

// Stop ends the process gracefully for good: no automatic restart.
func (p *Proc) Stop(ctx context.Context) error { return p.stop(ctx, gracefulWait) }

// stop ends the current process for good (no automatic restart).
func (p *Proc) stop(ctx context.Context, wait time.Duration) error {
	p.mu.Lock()
	p.deliberate = true
	exited := p.exited
	if p.paused {
		_ = p.signalLocked(syscall.SIGCONT) // a stopped process cannot handle SIGTERM
		p.paused = false
	}
	err := p.signalLocked(syscall.SIGTERM)
	p.mu.Unlock()
	if err != nil {
		return err
	}

	select {
	case <-exited:
		return nil
	case <-time.After(wait):
	case <-ctx.Done():
	}
	if err := p.signal(syscall.SIGKILL); err != nil {
		return err
	}
	<-exited
	return nil
}

// StopAll stops every child (resuming paused ones first) and disables
// restarts. It is safe to call more than once.
func (s *Supervisor) StopAll(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	procs := append([]*Proc(nil), s.procs...)
	s.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(procs))
	for i, p := range procs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.stop(ctx, stopAllWait)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
