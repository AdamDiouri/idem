package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/provider"
)

// pollingStub is an async step: its send is accepted, its poll reports paid,
// and its success hook behaves like stubStep's.
type pollingStub struct{ *stubStep }

func (p pollingStub) Execute(context.Context, engine.ClaimedStep) provider.Outcome {
	return provider.Outcome{Class: provider.Accepted, ProviderRef: "po_stub", HTTPStatus: 202}
}

func (p pollingStub) Poll(context.Context, engine.ClaimedStep) provider.Outcome {
	return provider.Outcome{Class: provider.Success, ProviderRef: "po_stub", HTTPStatus: 200}
}

var errHookBug = errors.New("hook bug: cannot post")

func (h harness) hookFailures(t *testing.T, wf uuid.UUID) (status, reason string, failures int) {
	t.Helper()
	require.NoError(t, h.pool.QueryRow(context.Background(), `
		SELECT status, COALESCE(dead_reason, ''), hook_failures FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf).
		Scan(&status, &reason, &failures))
	return status, reason, failures
}

// Review R11: a poll whose success hook always fails used to loop (lease
// expiry, back to AWAITING_CALLBACK, poll again) without ever parking. It now
// parks after HookMaxFailures with dead_reason hook_failed.
func TestPollHookAlwaysFails_ParksAfterMax(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := pollingStub{&stubStep{successErr: errHookBug, successErrFor: 1 << 30}}
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.HookMaxFailures = 3 }, step)

	for i := 0; i < 20 && h.workflowStatus(t, wf) != "NEEDS_ATTENTION"; i++ {
		_, _ = w.RunOnce(context.Background())
		if status, _, _ := h.hookFailures(t, wf); status == "RUNNING" {
			time.Sleep(pastLease) // the old path: wait for the lease to expire
			h.reclaim(t)
		}
	}
	status, reason, failures := h.hookFailures(t, wf)
	require.Equal(t, "DEAD", status)
	require.Equal(t, "hook_failed", reason)
	require.Equal(t, 3, failures)
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	require.True(t, engine.MoneyMayHaveMoved(reason), "the provider reported success: money moved")
}

// A send whose hook fails once: the provider acted, so the step is IN_DOUBT at
// once (no lease wait), and the retry replays the same key and posts once.
func TestSendHookFails_ThenSucceeds(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := &stubStep{successErr: errHookBug, successErrFor: 1}
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.HookMaxFailures = 3 }, step)

	_, err := w.RunOnce(context.Background())
	require.ErrorIs(t, err, errHookBug)
	status, _, failures := h.hookFailures(t, wf)
	require.Equal(t, "IN_DOUBT", status)
	require.Equal(t, 1, failures)

	require.Equal(t, "succeeded", h.runOnce(t, w).Outcome)
	status, _, failures = h.hookFailures(t, wf)
	require.Equal(t, "SUCCEEDED", status)
	require.Equal(t, 1, failures)
}

// Review focus 4: a worker that lost its lease cannot record a hook failure.
func TestRecordHookFailure_Fenced(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	s := mustClaim(t, h.pool, "w1", leaseTTL)
	_, err := h.pool.Exec(context.Background(), `UPDATE steps SET lease_epoch = lease_epoch + 1 WHERE id = $1`, s.ID)
	require.NoError(t, err)

	dead, err := engine.RecordHookFailure(context.Background(), h.pool, s, engine.Result{Kind: engine.ResultSucceeded}, errHookBug, 3, 0)
	require.ErrorIs(t, err, engine.ErrFenced)
	require.False(t, dead)
	status, _, failures := h.hookFailures(t, wf)
	require.Equal(t, "RUNNING", status)
	require.Equal(t, 0, failures)
}
