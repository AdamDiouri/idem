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

// sendDoubt reports whether the step's send history says money may have moved
// (the one definition, step_send_doubt).
func (h harness) sendDoubt(t *testing.T, wf uuid.UUID) bool {
	t.Helper()
	return scalar[bool](t, h.pool, `SELECT step_send_doubt(id) FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
}

// Final review, Important 1: a send the provider refused moved nothing. If its
// failure hook keeps failing, neither the retries nor the parking may turn
// that into doubt (which the pivot rule would later act on).
func TestRefusedSendHookFails_NeverBecomesDoubt(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := &stubStep{
		failureErr: errHookBug,
		execute: func(context.Context, engine.ClaimedStep) provider.Outcome {
			return provider.Outcome{Class: provider.Permanent, HTTPStatus: 422, Err: errors.New("account closed")}
		},
	}
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.HookMaxFailures = 2 }, step)

	_, err := w.RunOnce(context.Background())
	require.ErrorIs(t, err, errHookBug)
	status, _, failures := h.hookFailures(t, wf)
	require.Equal(t, "PENDING", status, "below the limit: retried, not in doubt")
	require.Equal(t, 1, failures)
	require.False(t, h.sendDoubt(t, wf))

	_, err = w.RunOnce(context.Background())
	require.ErrorIs(t, err, errHookBug)
	status, reason, failures := h.hookFailures(t, wf)
	require.Equal(t, "DEAD", status)
	require.Equal(t, engine.DeadHookFailedNoEffect, reason)
	require.Equal(t, 2, failures)
	require.False(t, h.sendDoubt(t, wf), "at the limit too: the refusal moved nothing")
	require.False(t, engine.MoneyMayHaveMoved(reason))

	// The operator fixes the bug and retries: the refusal is replayed and the
	// failure hook now runs (no pivot-rule parking, no confirmation needed).
	step.mu.Lock()
	step.failureErr = nil
	step.mu.Unlock()
	stepID := scalar[uuid.UUID](t, h.pool, `SELECT id FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
	require.NoError(t, h.admin().RetryStep(context.Background(), stepID, h.version(t, wf), "hook fixed"))
	require.Equal(t, "PENDING", h.chargeStatus(t, wf))
	require.Equal(t, "failed_permanent", h.runOnce(t, w).Outcome)
	require.Equal(t, 3, step.failureCalls)
}
