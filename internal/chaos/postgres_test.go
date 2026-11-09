package chaos

import (
	"context"
	"os/exec"
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
