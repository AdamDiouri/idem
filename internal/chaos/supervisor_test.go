package chaos

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/testutil"
)

var dsn string

// TestMain lets the test binary double as a long-running child process;
// otherwise it provides a Postgres for the client tests.
func TestMain(m *testing.M) {
	switch os.Getenv("CHAOS_HELPER") {
	case "sleep":
		for {
			fmt.Println("heartbeat")
			time.Sleep(50 * time.Millisecond)
		}
	case "exit137": // what a failpoint "exit" does
		time.Sleep(100 * time.Millisecond)
		os.Exit(137)
	}
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

func helperSpec(t *testing.T, name string) ProcSpec {
	t.Helper()
	return ProcSpec{
		Name:    name,
		Path:    os.Args[0],
		Env:     []string{"CHAOS_HELPER=sleep"},
		LogPath: filepath.Join(t.TempDir(), name+".log"),
	}
}

func newSupervisor(t *testing.T) *Supervisor {
	t.Helper()
	s := NewSupervisor(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = s.StopAll(context.Background()) })
	return s
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func logSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

func TestSupervisor_RestartsCrashedChild(t *testing.T) {
	s := newSupervisor(t)
	p, err := s.Start(helperSpec(t, "w0"))
	require.NoError(t, err)
	first := p.PID()
	require.True(t, alive(first))

	require.NoError(t, p.Kill())
	require.Eventually(t, func() bool { return p.PID() != first && alive(p.PID()) }, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, 1, p.Restarts())
	require.Equal(t, "w0", p.Name())
}

func TestSupervisor_PauseStopsHeartbeats(t *testing.T) {
	s := newSupervisor(t)
	spec := helperSpec(t, "w0")
	p, err := s.Start(spec)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return logSize(t, spec.LogPath) > 0 }, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, p.Pause())
	time.Sleep(100 * time.Millisecond) // let any in-flight write land
	frozen := logSize(t, spec.LogPath)
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, frozen, logSize(t, spec.LogPath), "a paused process must not run")

	require.NoError(t, p.Resume())
	require.Eventually(t, func() bool { return logSize(t, spec.LogPath) > frozen }, 2*time.Second, 20*time.Millisecond)
}

func TestSupervisor_RestartIsGraceful(t *testing.T) {
	s := newSupervisor(t)
	p, err := s.Start(helperSpec(t, "api"))
	require.NoError(t, err)
	first := p.PID()

	require.NoError(t, p.Restart(context.Background()))
	require.NotEqual(t, first, p.PID())
	require.True(t, alive(p.PID()))
	require.False(t, alive(first))
	require.Equal(t, 0, p.Restarts(), "a deliberate restart is not a crash")
}

func TestSupervisor_StopAllResumesAndKills(t *testing.T) {
	s := NewSupervisor(slog.New(slog.NewTextHandler(io.Discard, nil)))
	var pids []int
	var procs []*Proc
	for i := range 3 {
		p, err := s.Start(helperSpec(t, fmt.Sprintf("w%d", i)))
		require.NoError(t, err)
		procs = append(procs, p)
		pids = append(pids, p.PID())
	}
	require.NoError(t, procs[1].Pause())

	require.NoError(t, s.StopAll(context.Background()))
	for _, pid := range pids {
		require.False(t, alive(pid), "pid %d still alive", pid)
	}
	time.Sleep(700 * time.Millisecond) // longer than the restart delay
	for _, p := range procs {
		require.False(t, alive(p.PID()), "%s was restarted after StopAll", p.Name())
	}
	require.NoError(t, s.StopAll(context.Background()), "StopAll is idempotent")
}

// Review finding I1: stopping a process during the restart window must not let
// the monitor bring it back (it would still carry its failpoints).
func TestSupervisor_StopDuringRestartWindowDoesNotRelaunch(t *testing.T) {
	s := newSupervisor(t)
	p, err := s.Start(helperSpec(t, "w0"))
	require.NoError(t, err)
	first := p.PID()
	require.NoError(t, p.Kill())
	time.Sleep(100 * time.Millisecond) // inside the 500ms restart delay

	require.NoError(t, p.Stop(context.Background()))
	time.Sleep(restartDelay + 300*time.Millisecond)
	require.False(t, alive(first))
	require.False(t, alive(p.PID()), "stopped process was relaunched")
}

// Review finding I1: an event aimed at a process that is not running must
// fail, not be reported as executed.
func TestSupervisor_SignalDuringRestartWindowErrors(t *testing.T) {
	s := newSupervisor(t)
	p, err := s.Start(helperSpec(t, "w0"))
	require.NoError(t, err)
	require.NoError(t, p.Kill())
	time.Sleep(100 * time.Millisecond)

	require.ErrorIs(t, p.Kill(), ErrNotRunning)
	require.ErrorIs(t, p.Pause(), ErrNotRunning)
	require.Eventually(t, func() bool { return p.Kill() == nil }, 3*time.Second, 50*time.Millisecond, "running again after the restart")
}

// Review finding I4: failpoint exits (status 137) are told apart from kills.
func TestSupervisor_CountsFailpointExitsSeparately(t *testing.T) {
	s := newSupervisor(t)
	spec := helperSpec(t, "w0")
	spec.Env = []string{"CHAOS_HELPER=exit137"}
	p, err := s.Start(spec)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return p.FailpointExits() >= 2 }, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, p.Restarts(), p.FailpointExits()+p.Kills())
	require.Zero(t, p.Kills())

	k, err := s.Start(helperSpec(t, "w1"))
	require.NoError(t, err)
	require.NoError(t, k.Kill())
	require.Eventually(t, func() bool { return k.Kills() == 1 }, 3*time.Second, 50*time.Millisecond)
	require.Zero(t, k.FailpointExits())
}

// Slice 2.3b (F26, rolling deploy): restart_all restarts every process at once,
// gracefully, while traffic runs.
func TestApply_RestartAll(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"worker-0", "worker-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.workers = append(procs.workers, p)
	}
	api, err := s.Start(helperSpec(t, "api-0"))
	require.NoError(t, err)
	procs.apis = []*Proc{api}
	sweeper, err := s.Start(helperSpec(t, "sweeper-0"))
	require.NoError(t, err)
	procs.sweepers = []*Proc{sweeper}

	all := append(append(append([]*Proc{}, procs.workers...), procs.apis...), procs.sweepers...)
	before := map[*Proc]int{}
	for _, p := range all {
		before[p] = p.PID()
	}
	relay, err := s.Start(helperSpec(t, "relay-0"))
	require.NoError(t, err)
	procs.relays = []*Proc{relay}

	all = append(all, procs.relays...)
	before[relay] = relay.PID()
	require.NoError(t, apply(context.Background(), &Event{Kind: RestartAll}, procs, nil, "", "", nil))
	for _, p := range all {
		require.NotEqual(t, before[p], p.PID(), p.Name())
		require.True(t, alive(p.PID()), p.Name())
		require.Equal(t, 0, p.Restarts(), "a deliberate restart is not a crash")
	}
}

// Slice 2.5 (F33 at process level): every relay dies at once; the supervisor
// brings them back and the outbox, durable, is delivered then.
func TestApply_KillAllRelays(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"relay-0", "relay-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.relays = append(procs.relays, p)
	}
	before := []int{procs.relays[0].PID(), procs.relays[1].PID()}
	require.NoError(t, apply(context.Background(), &Event{Kind: KillAllRelays}, procs, nil, "", "", nil))
	for i, p := range procs.relays {
		require.Eventually(t, func() bool { return p.Restarts() == 1 && p.PID() != before[i] && alive(p.PID()) },
			5*time.Second, 20*time.Millisecond, p.Name())
	}
}

func TestApply_KillRelay(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"relay-0", "relay-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.relays = append(procs.relays, p)
	}
	require.NoError(t, apply(context.Background(), &Event{Kind: KillRelay, Target: 1}, procs, nil, "", "", nil))
	require.Eventually(t, func() bool { return procs.relays[1].Restarts() == 1 }, 5*time.Second, 20*time.Millisecond)
	require.Zero(t, procs.relays[0].Restarts())
}

// Slice 2.5b (the 2.5 flake): an event whose target worker is mid-restart
// goes to another running worker, so a seed cannot lose a whole event kind to
// bad timing; it is skipped only when no worker runs.
func TestApply_KillRetargetsWhenTargetDown(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"worker-0", "worker-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.workers = append(procs.workers, p)
	}
	require.NoError(t, procs.workers[0].Stop(context.Background()))
	require.NoError(t, apply(context.Background(), &Event{Kind: KillWorker, Target: 0}, procs, nil, "", "", nil))
	require.Eventually(t, func() bool { return procs.workers[1].Restarts() == 1 }, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, procs.workers[1].Stop(context.Background()))
	require.ErrorIs(t, apply(context.Background(), &Event{Kind: KillWorker, Target: 0}, procs, nil, "", "", nil), ErrNotRunning)
}

func TestApply_PauseRetargetsWhenTargetDown(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"worker-0", "worker-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.workers = append(procs.workers, p)
	}
	require.NoError(t, procs.workers[0].Stop(context.Background()))
	var pauses sync.WaitGroup
	require.NoError(t, apply(context.Background(), &Event{Kind: PauseWorker, Target: 0, Duration: 200 * time.Millisecond}, procs, nil, "", "", &pauses))
	before := logSize(t, procs.workers[1].spec.LogPath)
	time.Sleep(120 * time.Millisecond)
	require.Equal(t, before, logSize(t, procs.workers[1].spec.LogPath), "worker-1 is the one frozen")
	pauses.Wait()
}

// The same for relays: they crash at their failpoints too, so kill_relay must
// not be lost to a relay mid-restart.
func TestApply_KillRelayRetargetsWhenTargetDown(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"relay-0", "relay-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.relays = append(procs.relays, p)
	}
	require.NoError(t, procs.relays[1].Stop(context.Background()))
	require.NoError(t, apply(context.Background(), &Event{Kind: KillRelay, Target: 1}, procs, nil, "", "", nil))
	require.Eventually(t, func() bool { return procs.relays[0].Restarts() == 1 }, 5*time.Second, 20*time.Millisecond)
}

// Final review, minor 2: no process to target means the event is skipped, not
// recorded as done (and a pause never resumes a nil process).
func TestApply_NoProcessesIsSkipped(t *testing.T) {
	var pauses sync.WaitGroup
	for _, kind := range []EventKind{KillWorker, PauseWorker, KillRelay} {
		ev := Event{Kind: kind, Duration: time.Millisecond}
		require.ErrorIs(t, apply(context.Background(), &ev, procSet{}, nil, "", "", &pauses), ErrNotRunning, kind)
	}
}

// Final review, minor 3: a retargeted event records the process it hit and
// the one it was planned for.
func TestApply_RetargetIsRecorded(t *testing.T) {
	s := newSupervisor(t)
	var procs procSet
	for _, name := range []string{"worker-0", "worker-1"} {
		p, err := s.Start(helperSpec(t, name))
		require.NoError(t, err)
		procs.workers = append(procs.workers, p)
	}
	require.NoError(t, procs.workers[0].Stop(context.Background()))
	ev := Event{Kind: KillWorker, Target: 0}
	require.NoError(t, apply(context.Background(), &ev, procs, nil, "", "", nil))
	require.Equal(t, 1, ev.Target)
	require.NotNil(t, ev.PlannedTarget)
	require.Equal(t, 0, *ev.PlannedTarget)
}
