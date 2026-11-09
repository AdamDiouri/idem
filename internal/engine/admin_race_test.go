package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
)

const raceRounds = 10

// race runs every action at once (behind one start barrier) and returns their
// errors in order.
func race(actions ...func() error) []error {
	errs := make([]error, len(actions))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, act := range actions {
		wg.Go(func() {
			<-start
			errs[i] = act()
		})
	}
	close(start)
	wg.Wait()
	return errs
}

// requireOneWinner asserts exactly one action succeeded and every other one
// was refused cleanly (not parked any more, or stale), and returns the winner.
func requireOneWinner(t *testing.T, names []string, errs []error) string {
	t.Helper()
	winner := ""
	for i, err := range errs {
		switch {
		case err == nil:
			require.Empty(t, winner, "two actions won: %s and %s", winner, names[i])
			winner = names[i]
		case errors.Is(err, engine.ErrNotActionable), errors.Is(err, engine.ErrStale):
		default:
			t.Fatalf("%s failed with an unexpected error: %v", names[i], err)
		}
	}
	require.NotEmpty(t, winner, "no action won: %v", errs)
	return winner
}

// Deferred minor 7: operators acting on the same listing at the same moment.
// Exactly one action may win, and the losers must leave no trace: no audit
// row, no extra step, no ledger posting.
func TestAdmin_ConcurrentActionsExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	a, b := h.admin(), engine.NewAdmin(h.pool, h.allSteps(), "tester-2")
	wins := map[string]int{}

	for round := range raceRounds {
		wf := parkPayoutAfterDoubt(t, h)
		p := h.parkedStep(t, wf)
		names := []string{"retry-step", "confirm-step", "compensate", "compensate (2nd operator)", "new-generation"}
		errs := race(
			func() error { return a.RetryStep(ctx, p.StepID, p.Version, "r") },
			func() error { return b.ConfirmStep(ctx, p.StepID, p.Version, fmt.Sprintf("po_race_%d", round), "r") },
			func() error { return a.Compensate(ctx, wf, p.Version, "r", true) },
			func() error { return b.Compensate(ctx, wf, p.Version, "r", true) },
			func() error { _, err := a.NewGeneration(ctx, p.StepID, p.Version, "r", true); return err },
		)
		winner := requireOneWinner(t, names, errs)
		wins[winner]++

		require.Equal(t, 1, h.auditRows(t, wf), "round %d (%s won)", round, winner)
		refunds := h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf)
		payouts := h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf)
		posted := h.count(t, `SELECT count(*) FROM ledger_transactions WHERE workflow_id = $1 AND kind = 'payout'`, wf)
		require.Equal(t, b2i(winner == "compensate" || winner == "compensate (2nd operator)"), refunds, "round %d (%s won)", round, winner)
		require.Equal(t, 1+b2i(winner == "new-generation"), payouts, "round %d (%s won)", round, winner)
		require.Equal(t, b2i(winner == "confirm-step"), posted, "round %d (%s won)", round, winner)

		h.drain(t, h.workerWith("w1", nil, h.allSteps()...)) // settle before the next round's faults
	}
	t.Logf("winners over %d rounds: %v", raceRounds, wins)
}

// Deferred minor 7: operators racing each other while workers run. Whichever
// action wins, workers pick up its result and the workflow settles with every
// invariant intact (no money moved before parking, so all three are valid).
func TestAdmin_ConcurrentActionsWithRunningWorkers(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	a, b := h.admin(), engine.NewAdmin(h.pool, h.allSteps(), "tester-2")
	wins := map[string]int{}

	for round := range raceRounds {
		wf := parkCharge(t, h, fakeprovider.ModeRateLimit) // attempts_exhausted_no_effect
		p := h.parkedStep(t, wf)
		cancel1, done1 := runInBackground(t, h.workerWith(fmt.Sprintf("w1-%d", round), nil, h.allSteps()...).Run)
		cancel2, done2 := runInBackground(t, h.workerWith(fmt.Sprintf("w2-%d", round), nil, h.allSteps()...).Run)

		names := []string{"retry-step", "fail", "fail (2nd operator)", "new-generation"}
		errs := race(
			func() error { return a.RetryStep(ctx, p.StepID, p.Version, "r") },
			func() error { return a.Fail(ctx, wf, p.Version, "r", false) },
			func() error { return b.Fail(ctx, wf, p.Version, "r", false) },
			func() error { _, err := b.NewGeneration(ctx, p.StepID, p.Version, "r", true); return err },
		)
		winner := requireOneWinner(t, names, errs)
		wins[winner]++
		want := "SUCCEEDED"
		if winner == "fail" || winner == "fail (2nd operator)" {
			want = "FAILED"
		}
		require.Eventually(t, func() bool { return h.workflowStatus(t, wf) == want },
			10*time.Second, 20*time.Millisecond, "round %d (%s won)", round, winner)
		cancel1()
		cancel2()
		require.NoError(t, <-done1)
		require.NoError(t, <-done2)
		require.Equal(t, 1, h.auditRows(t, wf), "round %d (%s won)", round, winner)
	}
	h.assertNoViolations(t)
	t.Logf("winners over %d rounds: %v", raceRounds, wins)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
