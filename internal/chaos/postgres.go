package chaos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const readyTimeout = 60 * time.Second

// Postgres is a throwaway Postgres container owned by the harness.
type Postgres struct {
	Name string
	Port int
}

// StartPostgres runs postgres:18 as container `name`, published on
// 127.0.0.1:port, and waits until it accepts connections. On any failure,
// including cancellation, the container is removed before returning.
func StartPostgres(ctx context.Context, name string, port int) (*Postgres, error) {
	pg := &Postgres{Name: name, Port: port}
	if out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--name", name, //nolint:gosec // harness-controlled args
		"-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"-e", "POSTGRES_USER=idem", "-e", "POSTGRES_PASSWORD=idem", "-e", "POSTGRES_DB=idem",
		"postgres:18").CombinedOutput(); err != nil {
		// Killing the CLI does not stop the daemon from creating the container.
		pg.removeEventually()
		return nil, fmt.Errorf("docker run: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if err := pg.waitReady(ctx); err != nil {
		pg.removeEventually()
		return nil, err
	}
	return pg, nil
}

// containerName returns a container name unique to this run, so two campaigns
// with the same seed never remove each other's database.
func containerName(seed int64) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("idem-chaos-%d-%s", seed, hex.EncodeToString(b))
}

// removeEventually removes the container even if the daemon is still creating
// it: it retries until the container has been absent twice in a row (5s max).
func (pg *Postgres) removeEventually() {
	ctx := context.Background()
	absent := 0
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && absent < 2; {
		_ = pg.Remove(ctx)
		time.Sleep(500 * time.Millisecond)
		out, err := exec.CommandContext(ctx, "docker", "ps", "-a", "-q", "--filter", "name=^"+pg.Name+"$").Output() //nolint:gosec // harness-controlled args
		if err == nil && len(strings.TrimSpace(string(out))) == 0 {
			absent++
		} else {
			absent = 0
		}
	}
}

// DSN is the connection string for the container.
func (pg *Postgres) DSN() string {
	return fmt.Sprintf("postgres://idem:idem@127.0.0.1:%d/idem?sslmode=disable", pg.Port)
}

// Restart restarts the container (a database outage) and waits until it is ready.
func (pg *Postgres) Restart(ctx context.Context) error {
	if out, err := exec.CommandContext(ctx, "docker", "restart", pg.Name).CombinedOutput(); err != nil { //nolint:gosec // harness-controlled args
		return fmt.Errorf("docker restart: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return pg.waitReady(ctx)
}

// Remove deletes the container and its anonymous data volume. Removing a
// missing container is not an error.
func (pg *Postgres) Remove(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", "-v", pg.Name).CombinedOutput() //nolint:gosec // harness-controlled args
	if err != nil && !strings.Contains(string(out), "No such container") {
		return fmt.Errorf("docker rm: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// waitReady polls over TCP from the host. (pg_isready inside the container can
// succeed during the image's init phase, before TCP connections are accepted.)
func (pg *Postgres) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(readyTimeout)
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := pgx.Connect(attemptCtx, pg.DSN())
		if err == nil {
			err = conn.Ping(attemptCtx)
			_ = conn.Close(attemptCtx)
		}
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("postgres %s not ready: %w", pg.Name, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// FreePort returns a TCP port on 127.0.0.1 that was free a moment ago.
func FreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
