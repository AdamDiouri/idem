package recon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	// Margin is how far before the window the provider's report is read: a
	// posting in the window may follow its movement by up to this long (a
	// payout settles before the webhook or poll lets idem post it).
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

// Run reconciles the window [from, to): each provider movement created in it
// must have its posting, and each posting created in it its movement (looked
// for up to Margin before the window). idem's state is read first, in one
// read-only repeatable-read snapshot held while the provider's report is
// fetched: a posting in the snapshot already has its movement at the
// provider, and a movement made later belongs to a step the snapshot shows in
// flight. The run and its discrepancies are written in one transaction; a run
// that fails is recorded FAILED with its error and reports nothing. Nothing
// else changes (D15): the snapshot is read-only.
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
	found, err := reconcile(ctx, pool, provider, Window{From: from, To: to}, opts.Margin, res.RunID)
	if err != nil {
		msg := err.Error()
		// Record the failure even if ctx was cancelled (SIGTERM mid-run).
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if ferr := q.FinishReconRun(finishCtx, db.FinishReconRunParams{ID: res.RunID, Status: "FAILED", Error: &msg}); ferr != nil {
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

func reconcile(ctx context.Context, pool *pgxpool.Pool, reporter Reporter, win Window, margin time.Duration, runID uuid.UUID) (Result, error) {
	var res Result
	snap, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return res, fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { _ = snap.Rollback(ctx) }()
	sq := db.New(snap)
	// The first statement fixes the snapshot, before the report is read.
	posted, err := sq.WorkflowsPostedBetween(ctx, db.WorkflowsPostedBetweenParams{WindowFrom: ts(win.From), WindowTo: ts(win.To)})
	if err != nil {
		return res, fmt.Errorf("workflows posted in the window: %w", err)
	}

	records, err := reporter.ListBalanceTransactions(ctx, win.From.Add(-margin), win.To)
	if err != nil {
		return res, err
	}
	res.Records = len(records)

	byWorkflow := map[string][]provider.BalanceTransaction{}
	anchored := map[string]bool{}
	for _, r := range records {
		key := r.WorkflowID
		if id, err := uuid.Parse(r.WorkflowID); err == nil {
			key = id.String() // one key per workflow, whatever form the metadata used
		}
		byWorkflow[key] = append(byWorkflow[key], r)
		if win.contains(r.Created) {
			anchored[key] = true
		}
	}
	for _, id := range posted {
		anchored[id.String()] = true
	}
	var ids []uuid.UUID
	var unparsable []string
	for key := range anchored {
		if id, err := uuid.Parse(key); err == nil {
			ids = append(ids, id)
		} else {
			unparsable = append(unparsable, key)
		}
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
	workflows, err := load(ctx, sq, ids)
	if err != nil {
		return res, err
	}
	if err := snap.Commit(ctx); err != nil {
		return res, err
	}

	var found []Discrepancy
	for _, id := range ids {
		w := workflows[id] // nil: a workflow idem does not know
		if w != nil {
			res.Workflows++
		}
		ds, inFlight := Classify(w, byWorkflow[id.String()], win)
		found, res.InFlight = append(found, ds...), res.InFlight+inFlight
	}
	for _, key := range unparsable {
		ds, _ := Classify(nil, byWorkflow[key], win)
		found = append(found, ds...)
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
			ItemKey: d.ItemKey, ItemAt: ts(d.ItemAt),
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

// load reads, in the snapshot, what idem knows about each workflow.
func load(ctx context.Context, q *db.Queries, ids []uuid.UUID) (map[uuid.UUID]*Workflow, error) {
	out := map[uuid.UUID]*Workflow{}
	statuses, err := q.ReconWorkflowStatuses(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("workflow statuses: %w", err)
	}
	for _, s := range statuses {
		out[s.ID] = &Workflow{ID: s.ID, Status: s.Status}
	}
	steps, err := q.ReconSteps(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("steps: %w", err)
	}
	for _, s := range steps {
		if w := out[s.WorkflowID]; w != nil {
			w.Steps = append(w.Steps, Step{ID: s.ID, Name: s.Name, Status: s.Status, ProviderRef: s.ProviderRef})
		}
	}
	postings, err := q.ReconPostings(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("postings: %w", err)
	}
	for _, p := range postings {
		if w := out[p.WorkflowID]; w != nil {
			w.Postings = append(w.Postings, Posting{StepID: p.StepID, Kind: p.Kind, Amount: p.AmountCents, Currency: p.Currency, At: p.CreatedAt.Time})
		}
	}
	return out, nil
}

func ts(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
