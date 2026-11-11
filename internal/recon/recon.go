package recon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// Reporter is the provider's balance-transactions report; satisfied by
// *provider.Client.
type Reporter interface {
	ListBalanceTransactions(ctx context.Context, from, to time.Time) ([]provider.BalanceTransaction, error)
}

// Options configures a run.
type Options struct {
	// Margin widens the provider report on both sides of the window: a payout
	// can settle at the provider before idem posts it, on either side of a
	// window's edge.
	Margin time.Duration
	Logger *slog.Logger
}

// Result is what a run found.
type Result struct {
	RunID         uuid.UUID
	Records       int // provider records read (window plus margin)
	Workflows     int // workflows compared
	InFlight      int // skipped: not final yet
	Discrepancies int
	Parked        int // discrepancies on parked workflows
}

// Run reconciles the window [from, to). The workflows compared are those with
// a provider record or a ledger posting created in it, each compared on
// everything known about it; workflows not final yet are skipped. The run and
// its discrepancies are written in one transaction; a run that fails is
// recorded FAILED with its error and reports nothing. Nothing else changes (D15).
func Run(ctx context.Context, pool *pgxpool.Pool, provider Reporter, from, to time.Time, opts Options) (Result, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	res := Result{RunID: uuid.Must(uuid.NewV7())}
	q := db.New(pool)
	if err := q.InsertReconRun(ctx, db.InsertReconRunParams{ID: res.RunID, WindowFrom: ts(from), WindowTo: ts(to)}); err != nil {
		return res, fmt.Errorf("start reconciliation run: %w", err)
	}
	found, err := reconcile(ctx, pool, provider, from, to, opts.Margin, res.RunID)
	if err != nil {
		msg := err.Error()
		if ferr := q.FinishReconRun(ctx, db.FinishReconRunParams{ID: res.RunID, Status: "FAILED", Error: &msg}); ferr != nil {
			err = errors.Join(err, fmt.Errorf("record the failed run: %w", ferr))
		}
		logger.Error("reconciliation failed", "run_id", res.RunID, "error", err)
		return res, err
	}
	found.RunID = res.RunID
	logger.Info("reconciliation done", "run_id", found.RunID, "records", found.Records, "workflows", found.Workflows,
		"in_flight", found.InFlight, "discrepancies", found.Discrepancies, "parked", found.Parked)
	return found, nil
}

func reconcile(ctx context.Context, pool *pgxpool.Pool, reporter Reporter, from, to time.Time, margin time.Duration, runID uuid.UUID) (Result, error) {
	var res Result
	records, err := reporter.ListBalanceTransactions(ctx, from.Add(-margin), to.Add(margin))
	if err != nil {
		return res, err
	}
	res.Records = len(records)

	q := db.New(pool)
	byWorkflow := map[string][]provider.BalanceTransaction{}
	anchored := map[string]bool{}
	for _, r := range records {
		byWorkflow[r.WorkflowID] = append(byWorkflow[r.WorkflowID], r)
		if !r.Created.Before(from) && r.Created.Before(to) {
			anchored[r.WorkflowID] = true
		}
	}
	posted, err := q.WorkflowsPostedBetween(ctx, db.WorkflowsPostedBetweenParams{WindowFrom: ts(from), WindowTo: ts(to)})
	if err != nil {
		return res, fmt.Errorf("workflows posted in the window: %w", err)
	}
	for _, id := range posted {
		anchored[id.String()] = true
	}

	var ids []uuid.UUID
	var unknownIDs []string // the provider names a workflow idem cannot even parse
	for id := range anchored {
		parsed, err := uuid.Parse(id)
		if err != nil {
			unknownIDs = append(unknownIDs, id)
			continue
		}
		ids = append(ids, parsed)
	}
	workflows, err := load(ctx, q, ids)
	if err != nil {
		return res, err
	}
	inFlight, err := q.ReconInFlight(ctx, ids)
	if err != nil {
		return res, fmt.Errorf("in-flight workflows: %w", err)
	}

	var found []Discrepancy
	for _, id := range ids {
		if slices.Contains(inFlight, id) {
			res.InFlight++
			continue
		}
		w := workflows[id] // nil: a workflow idem does not know
		if w != nil {
			res.Workflows++
		}
		found = append(found, Classify(w, byWorkflow[id.String()])...)
	}
	for _, id := range unknownIDs {
		found = append(found, Classify(nil, byWorkflow[id])...)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := db.New(tx)
	for _, d := range found {
		refs := d.ProviderRefs
		if refs == nil {
			refs = []string{}
		}
		if err := qtx.InsertReconDiscrepancy(ctx, db.InsertReconDiscrepancyParams{
			ID: uuid.Must(uuid.NewV7()), RunID: runID, WorkflowID: d.WorkflowID, Kind: d.Kind, EffectType: d.EffectType,
			ProviderRefs: refs, ProviderAmount: d.ProviderAmount, LedgerAmount: d.LedgerAmount, Parked: d.Parked, Detail: d.Detail,
		}); err != nil {
			return res, fmt.Errorf("record discrepancy: %w", err)
		}
		if d.Parked {
			res.Parked++
		}
	}
	res.Discrepancies = len(found)
	if err := qtx.FinishReconRun(ctx, db.FinishReconRunParams{ID: runID, Status: "SUCCEEDED", Records: int32(res.Records), //nolint:gosec // counts
		Workflows: int32(res.Workflows), InFlight: int32(res.InFlight), Discrepancies: int32(res.Discrepancies)}); err != nil { //nolint:gosec // counts
		return res, fmt.Errorf("finish run: %w", err)
	}
	return res, tx.Commit(ctx)
}

// load reads what idem knows about each workflow.
func load(ctx context.Context, q *db.Queries, ids []uuid.UUID) (map[uuid.UUID]*Workflow, error) {
	out := map[uuid.UUID]*Workflow{}
	statuses, err := q.ReconWorkflowStatuses(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("workflow statuses: %w", err)
	}
	for _, s := range statuses {
		out[s.ID] = &Workflow{ID: s.ID, Status: s.Status, StepStatus: map[string]string{}, Postings: map[string][]int64{}}
	}
	steps, err := q.ReconLatestStepStatuses(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("step statuses: %w", err)
	}
	for _, s := range steps {
		if w := out[s.WorkflowID]; w != nil {
			w.StepStatus[s.Name] = s.Status
		}
	}
	postings, err := q.ReconPostings(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("postings: %w", err)
	}
	for _, p := range postings {
		if w := out[p.WorkflowID]; w != nil {
			w.Postings[p.Kind] = append(w.Postings[p.Kind], p.AmountCents)
		}
	}
	return out, nil
}

func ts(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
