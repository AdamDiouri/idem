package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/provider"
)

// completeTimeout bounds recording a step's result. It is separate from the
// step's own context so that an outcome is still recorded after a shutdown
// cancelled the provider call (durable before acknowledged).
const completeTimeout = 10 * time.Second

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	ID           string
	Steps        []Step
	LeaseTTL     time.Duration
	PollInterval time.Duration
	Backoff      Backoff
	// InFlightDelay is the retry delay after a 409 (the provider is still
	// processing the same key). Zero means Backoff.Base.
	InFlightDelay time.Duration
	// KeyRetention is how long the provider remembers idempotency keys;
	// KeyRetentionMargin is the safety margin before it. Zero disables lookups.
	KeyRetention       time.Duration
	KeyRetentionMargin time.Duration
	// ShutdownGrace is how long the in-flight step may keep running after Run's
	// context is cancelled.
	ShutdownGrace time.Duration
	Logger        *slog.Logger
}

// RunResult describes one RunOnce call.
type RunResult struct {
	Claimed bool
	StepID  uuid.UUID
	Outcome string // succeeded | failed_permanent | in_doubt | retry_no_effect | dead | fenced
}

var outcomeNames = map[ResultKind]string{
	ResultSucceeded:       "succeeded",
	ResultFailedPermanent: "failed_permanent",
	ResultInDoubt:         "in_doubt",
	ResultRetryNoEffect:   "retry_no_effect",
	ResultDead:            "dead",
}

// Worker claims and executes steps.
type Worker struct {
	pool   *pgxpool.Pool
	opts   WorkerOptions
	steps  map[string]Step
	names  []string
	logger *slog.Logger
}

// NewWorker returns a worker that handles opts.Steps.
func NewWorker(pool *pgxpool.Pool, opts WorkerOptions) *Worker {
	if opts.InFlightDelay == 0 {
		opts.InFlightDelay = opts.Backoff.Base
	}
	w := &Worker{pool: pool, opts: opts, steps: map[string]Step{}, logger: opts.Logger}
	if w.logger == nil {
		w.logger = slog.Default()
	}
	for _, s := range opts.Steps {
		w.steps[s.Name()] = s
		w.names = append(w.names, s.Name())
	}
	return w
}

// RunOnce claims one step, executes it and records the result. A lost lease is
// not an error: the result is discarded and Outcome is "fenced".
func (w *Worker) RunOnce(ctx context.Context) (RunResult, error) {
	s, ok, err := Claim(ctx, w.pool, w.opts.ID, w.names, w.opts.LeaseTTL)
	if err != nil || !ok {
		return RunResult{}, err
	}
	log := w.logger.With(
		"workflow_id", s.WorkflowID, "step_id", s.ID, "step", s.Name,
		"attempt", s.Attempt, "lease_epoch", s.LeaseEpoch, "worker_id", w.opts.ID)
	log.Debug("step claimed", "prior_status", s.PriorStatus)
	failpoint.Hit(FPAfterClaim)

	step := w.steps[s.Name]
	var r Result
	var out provider.Outcome
	failpoint.Hit(FPBeforeProviderCall)
	if w.nearKeyRetention(s) {
		// Re-sending could reach a provider that has forgotten the key and
		// charge twice. Ask what happened instead (design §9.4).
		log.Warn("idempotency key near provider retention; resolving by lookup", "key_age", s.KeyAge.String())
		var found bool
		out, found = w.lookup(ctx, step, s)
		if !found && out.Class == provider.Permanent {
			r = Result{Kind: ResultDead, DeadReason: DeadKeyExpiredUnknown, HTTPStatus: out.HTTPStatus, Error: errString(out.Err)}
		}
	} else {
		out = w.execute(ctx, step, s)
	}
	failpoint.Hit(FPAfterProviderCall)

	if r.Kind == "" {
		r = decide(s, out, w.opts.Backoff, w.opts.InFlightDelay, step.Pivot())
	}

	completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancel()
	err = Complete(completeCtx, w.pool, s, r, step)
	if errors.Is(err, ErrFenced) {
		log.Warn("lease lost before completion; result discarded", "provider_class", out.Class.String())
		return RunResult{Claimed: true, StepID: s.ID, Outcome: "fenced"}, nil
	}
	if err != nil {
		log.Error("complete step; it will be recovered when its lease expires", "error", err)
		return RunResult{Claimed: true, StepID: s.ID}, err
	}

	outcome := outcomeNames[r.Kind]
	switch r.Kind {
	case ResultDead:
		log.Error("step is DEAD; workflow needs attention",
			"dead_reason", r.DeadReason, "money_may_have_moved", MoneyMayHaveMoved(r.DeadReason),
			"http_status", r.HTTPStatus, "error", r.Error)
	case ResultInDoubt, ResultRetryNoEffect:
		log.Warn("step will be retried", "outcome", outcome, "retry_in", r.RetryDelay.String(),
			"http_status", r.HTTPStatus, "error", r.Error)
	default:
		log.Info("step completed", "outcome", outcome, "http_status", r.HTTPStatus, "error", r.Error)
	}
	return RunResult{Claimed: true, StepID: s.ID, Outcome: outcome}, nil
}

// execute runs the step, turning an unexpected panic into an ambiguous outcome:
// we cannot know how far the step got. Simulated crashes are re-panicked.
func (w *Worker) execute(ctx context.Context, step Step, s ClaimedStep) (out provider.Outcome) {
	defer func() {
		if r := recover(); r != nil {
			if failpoint.IsCrash(r) {
				panic(r)
			}
			out = provider.Outcome{Class: provider.Ambiguous, Err: fmt.Errorf("panic: %v", r)}
		}
	}()
	return step.Execute(ctx, s)
}

// nearKeyRetention reports whether an in-doubt step's key is old enough that
// the provider may soon forget it (or already has).
func (w *Worker) nearKeyRetention(s ClaimedStep) bool {
	return w.opts.KeyRetention > 0 &&
		s.PriorStatus == string(ResultInDoubt) &&
		s.KeyAge >= w.opts.KeyRetention-w.opts.KeyRetentionMargin
}

// lookup is execute's counterpart for Step.Lookup.
func (w *Worker) lookup(ctx context.Context, step Step, s ClaimedStep) (out provider.Outcome, found bool) {
	defer func() {
		if r := recover(); r != nil {
			if failpoint.IsCrash(r) {
				panic(r)
			}
			out, found = provider.Outcome{Class: provider.Ambiguous, Err: fmt.Errorf("panic: %v", r)}, false
		}
	}()
	return step.Lookup(ctx, s)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Run processes steps until ctx is cancelled. Cancellation stops new claims;
// the in-flight step keeps running for up to ShutdownGrace (design §11.2), and
// its outcome is still recorded. Errors are logged and the loop continues.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker started", "worker_id", w.opts.ID, "steps", w.names)

	stepCtx, cancelSteps := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSteps()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(w.opts.ShutdownGrace, cancelSteps) })
	defer stop()

	for ctx.Err() == nil {
		res, err := w.RunOnce(stepCtx)
		if err != nil && !res.Claimed {
			w.logger.Error("claim step", "worker_id", w.opts.ID, "error", err)
		}
		if err != nil || !res.Claimed {
			select {
			case <-ctx.Done():
			case <-time.After(w.opts.PollInterval):
			}
		}
	}
	w.logger.Info("worker stopped", "worker_id", w.opts.ID)
	return nil
}

// RunSweeper reclaims expired leases every interval until ctx is cancelled,
// and logs every workflow that has not progressed for stuckAfter (§11.1).
func RunSweeper(ctx context.Context, pool *pgxpool.Pool, interval, stuckAfter time.Duration, logger *slog.Logger) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		n, err := ReclaimExpiredLeases(ctx, pool)
		switch {
		case err != nil && ctx.Err() == nil:
			logger.Error("sweep", "error", err)
		case n > 0:
			logger.Warn("reclaimed expired leases; steps are now IN_DOUBT", "count", n)
		}

		stuck, err := FindStuckWorkflows(ctx, pool, stuckAfter)
		if err != nil && ctx.Err() == nil {
			logger.Error("find stuck workflows", "error", err)
		}
		for _, wf := range stuck {
			logger.Warn("workflow stuck", "workflow_id", wf.ID.String(), "status", wf.Status, "age", wf.Age.String())
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
