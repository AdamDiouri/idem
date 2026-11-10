package store_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// clockColumns are the columns whose values decide who may run what, when:
// leases, schedules and callback deadlines (design F24).
var clockColumns = regexp.MustCompile(`\b(lease_expires_at|run_after|callback_deadline|next_attempt_at)\b`)

// sqlcParam matches sqlc.arg(name) and sqlc.narg(name).
var sqlcParam = regexp.MustCompile(`sqlc\.n?arg\((\w+)\)`)

// positionalParam matches $1, $2, ...: its type is not visible in the query,
// so a query touching a clock column must use named parameters instead.
var positionalParam = regexp.MustCompile(`\$\d+`)

// timeLike names a parameter that would carry a wall-clock instant from Go.
// Durations (*_ms) are fine: they are added to the database's now().
var timeLike = regexp.MustCompile(`(^now$|_at$|^at$|time|until|deadline|instant)`)

// goClockedQueries returns, for every query in dir touching a clock column,
// the parameters that look like a time from the application's clock, and any
// positional parameter (whose type cannot be judged).
func goClockedQueries(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	var bad []string
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // repository files
		require.NoError(t, err)
		for _, q := range strings.Split(string(b), "-- name: ")[1:] {
			name, _, _ := strings.Cut(q, " ")
			if !clockColumns.MatchString(q) {
				continue
			}
			for _, m := range sqlcParam.FindAllStringSubmatch(q, -1) {
				if timeLike.MatchString(m[1]) {
					bad = append(bad, filepath.Base(f)+": "+name+" uses "+m[0])
				}
			}
			for _, m := range positionalParam.FindAllString(q, -1) {
				bad = append(bad, filepath.Base(f)+": "+name+" uses "+m)
			}
		}
	}
	return bad
}

// F24 (clock skew between workers): leases and schedules are set and compared
// with the database's now() only, never with a worker's clock. A static guard:
// no query touching those columns takes a time parameter from Go.
func TestLeaseQueriesUseDatabaseClock(t *testing.T) {
	require.Empty(t, goClockedQueries(t, "queries"))
}

func TestLeaseQueriesUseDatabaseClock_DetectsAGoClock(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.sql"), []byte(`-- name: ClaimWithGoClock :exec
UPDATE steps SET lease_expires_at = sqlc.arg(expires_at) WHERE id = sqlc.arg(id);

-- name: PositionalClock :exec
UPDATE steps SET run_after = $1 WHERE id = $2;

-- name: Fine :exec
UPDATE steps SET run_after = now() + (sqlc.arg(retry_ms)::bigint * interval '1 millisecond') WHERE id = sqlc.arg(id);
`), 0o600))
	require.Equal(t, []string{
		"bad.sql: ClaimWithGoClock uses sqlc.arg(expires_at)",
		"bad.sql: PositionalClock uses $1", // a positional parameter's type is unknown: refused
		"bad.sql: PositionalClock uses $2",
	}, goClockedQueries(t, dir))
}
