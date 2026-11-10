package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// Final review, minor 1: errors the database raises under contention are
// transient, not a bug in the hook: retrying them is right, counting them
// could park a healthy step after three unlucky deadlocks.
func TestCountableHookError(t *testing.T) {
	hook := func(err error) error { return fmt.Errorf("complete: %w", &HookError{Err: err}) }
	for code, want := range map[string]bool{
		"40P01": false, // deadlock_detected
		"40001": false, // serialization_failure
		"55P03": false, // lock_not_available
		"57014": false, // query_canceled (statement_timeout)
		"23505": true,  // unique_violation: the hook's own bug
		"23514": true,  // check_violation (e.g. an unbalanced posting)
	} {
		require.Equal(t, want, countableHookError(hook(&pgconn.PgError{Code: code})), code)
	}
	require.True(t, countableHookError(hook(errors.New("bug"))))
	require.False(t, countableHookError(hook(context.DeadlineExceeded)))
	require.False(t, countableHookError(errors.New("not a hook error")))
}
