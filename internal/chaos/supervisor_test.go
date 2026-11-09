package chaos

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
