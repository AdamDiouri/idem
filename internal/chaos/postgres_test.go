package chaos

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func ping(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	require.NoError(t, conn.Ping(ctx))
}

func TestPostgres_StartRestartRemove(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	ctx := context.Background()
	port, err := FreePort()
	require.NoError(t, err)

	pg, err := StartPostgres(ctx, "idem-chaos-test", port)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Remove(context.Background()) })
	ping(t, pg.DSN())

	require.NoError(t, pg.Restart(ctx))
	ping(t, pg.DSN())

	require.NoError(t, pg.Remove(ctx))
	require.NoError(t, pg.Remove(ctx), "Remove is idempotent")
	out, _ := exec.Command("docker", "ps", "-a", "-q", "--filter", "name=^idem-chaos-test$").Output()
	require.Empty(t, string(out))
}

func containerExists(name string) bool {
	out, _ := exec.Command("docker", "ps", "-a", "-q", "--filter", "name=^"+name+"$").Output()
	return len(out) > 0
}

// Review finding I3: removing the container must also remove its data volume.
func TestPostgres_RemoveDeletesVolume(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	ctx := context.Background()
	port, err := FreePort()
	require.NoError(t, err)
	pg, err := StartPostgres(ctx, "idem-chaos-test-vol", port)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Remove(context.Background()) })

	out, err := exec.Command("docker", "inspect", "-f", "{{range .Mounts}}{{.Name}} {{end}}", pg.Name).Output()
	require.NoError(t, err)
	volumes := strings.Fields(string(out))
	require.NotEmpty(t, volumes, "postgres:18 declares a data volume")

	require.NoError(t, pg.Remove(ctx))
	for _, v := range volumes {
		require.Error(t, exec.Command("docker", "volume", "inspect", v).Run(), "volume %s leaked", v)
	}
}

// Review finding I2: cancelling during `docker run` must not leak a container
// (the daemon may finish creating it after the CLI is killed).
func TestPostgres_CancelledStartLeavesNothing(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	const name = "idem-chaos-test-cancel"
	port, err := FreePort()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err = StartPostgres(ctx, name, port)
	require.Error(t, err)
	time.Sleep(2 * time.Second) // give a late daemon-side create time to show up
	require.False(t, containerExists(name), "container leaked after a cancelled start")
}

func TestContainerName_UniquePerRun(t *testing.T) {
	a, b := containerName(1), containerName(1)
	require.NotEqual(t, a, b, "two runs with the same seed must not share a container")
	require.True(t, strings.HasPrefix(a, "idem-chaos-1-"))
}
