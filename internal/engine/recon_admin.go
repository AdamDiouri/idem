package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// ReconRun is a successful reconciliation run (slice 2.6).
type ReconRun struct {
	ID            uuid.UUID `json:"run_id"`
	WindowFrom    time.Time `json:"window_from"`
	WindowTo      time.Time `json:"window_to"`
	FinishedAt    time.Time `json:"finished_at"`
	Records       int32     `json:"records"`
	Workflows     int32     `json:"workflows"`
	InFlight      int32     `json:"in_flight"`
	Discrepancies int32     `json:"discrepancies"`
}

// ReconDiscrepancy is one difference the run found.
type ReconDiscrepancy struct {
	WorkflowID     *uuid.UUID `json:"workflow_id,omitempty"`
	Kind           string     `json:"kind"`
	EffectType     string     `json:"effect_type"`
	ProviderRefs   []string   `json:"provider_refs"`
	ProviderAmount *int64     `json:"provider_amount,omitempty"`
	LedgerAmount   *int64     `json:"ledger_amount,omitempty"`
	Parked         bool       `json:"parked"`
	Detail         string     `json:"detail"`
}

// ListReconciliation returns a successful run and its discrepancies: runID,
// or the latest one when nil (ErrNotActionable if there is none).
func (a *Admin) ListReconciliation(ctx context.Context, runID *uuid.UUID) (ReconRun, []ReconDiscrepancy, error) {
	q := db.New(a.pool)
	var row db.LatestReconRunRow
	var err error
	if runID == nil {
		row, err = q.LatestReconRun(ctx)
	} else {
		var r db.ReconRunByIDRow
		r, err = q.ReconRunByID(ctx, *runID)
		row = db.LatestReconRunRow(r)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconRun{}, nil, fmt.Errorf("%w: no successful reconciliation run", ErrNotActionable)
	}
	if err != nil {
		return ReconRun{}, nil, fmt.Errorf("reconciliation run: %w", err)
	}
	run := ReconRun{ID: row.ID, WindowFrom: row.WindowFrom.Time, WindowTo: row.WindowTo.Time, FinishedAt: row.FinishedAt.Time,
		Records: row.Records, Workflows: row.Workflows, InFlight: row.InFlight, Discrepancies: row.Discrepancies}
	rows, err := q.ReconDiscrepanciesOfRun(ctx, row.ID)
	if err != nil {
		return ReconRun{}, nil, fmt.Errorf("discrepancies: %w", err)
	}
	out := make([]ReconDiscrepancy, 0, len(rows))
	for _, r := range rows {
		out = append(out, ReconDiscrepancy{WorkflowID: r.WorkflowID, Kind: r.Kind, EffectType: r.EffectType, ProviderRefs: r.ProviderRefs,
			ProviderAmount: r.ProviderAmount, LedgerAmount: r.LedgerAmount, Parked: r.Parked, Detail: r.Detail})
	}
	return run, out, nil
}
