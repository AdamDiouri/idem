package chaos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/store"
)

// Config configures a chaos campaign.
type Config struct {
	Seed          int64         `json:"seed"`
	Duration      time.Duration `json:"duration"`       // how long traffic and faults run
	SettleTimeout time.Duration `json:"settle_timeout"` // how long the system may take to settle afterwards
	Workflows     int           `json:"workflows"`
	Workers       int           `json:"workers"`
	Sweepers      int           `json:"sweepers"`
	BinDir        string        `json:"bin_dir"`
	RunDir        string        `json:"run_dir"`
	Failpoints    string        `json:"failpoints"` // armed in workers during the campaign
}

// DefaultConfig returns the campaign defaults.
func DefaultConfig() Config {
	return Config{
		Duration:      90 * time.Second,
		SettleTimeout: 120 * time.Second,
		Workflows:     1000,
		Workers:       4,
		Sweepers:      2,
		BinDir:        "bin",
		Failpoints:    "after_claim=exit@0.01,after_provider_call=exit@0.02,before_commit=exit@0.02",
	}
}

const (
	apiToken = "chaos-token-0123456789"
	leaseTTL = 4 * time.Second
)

// webhookSecret signs the chaos provider's webhooks to the chaos API.
const webhookSecret = "whsec_chaos_0123456789" //nolint:gosec // throwaway secret for a local chaos run, like apiToken

// providerEnv compresses the fake provider's timings like the engine's: payouts
// settle in 300ms, and a timeout fault hangs 3s (with a 1s client timeout)
// instead of 30s, which no real provider holds a key for. Settled payouts are
// announced by signed webhook to the API (slice 2.2); polling stays the net.
func providerEnv(port, apiPort int) []string {
	return []string{
		fmt.Sprintf("FAKEPROVIDER_ADDR=127.0.0.1:%d", port), "FAKEPROVIDER_PAYOUT_SETTLE=300ms", "FAKEPROVIDER_HANG=3s",
		fmt.Sprintf("FAKEPROVIDER_WEBHOOK_URL=http://127.0.0.1:%d/webhooks/provider", apiPort),
		"FAKEPROVIDER_WEBHOOK_SECRET=" + webhookSecret,
	}
}

// apiEnv configures the chaos API, which receives the provider's webhooks.
func apiEnv(dsn string, port int) []string {
	return []string{"IDEM_DATABASE_URL=" + dsn, "IDEM_API_TOKENS=chaos=" + apiToken,
		fmt.Sprintf("IDEM_HTTP_ADDR=127.0.0.1:%d", port), "IDEM_PROVIDER_WEBHOOK_SECRETS=" + webhookSecret}
}

// workerEnv tunes the engine for chaos: short leases and retries so a short
// campaign exercises many recoveries. It satisfies the config rules
// (timeout + 2s < lease, grace >= timeout + 2s).
func workerEnv(dsn, providerURL string) []string {
	return []string{
		"IDEM_DATABASE_URL=" + dsn,
		"IDEM_PROVIDER_URL=" + providerURL,
		"IDEM_LEASE_TTL=" + leaseTTL.String(),
		"IDEM_PROVIDER_TIMEOUT=1s",
		"IDEM_SHUTDOWN_GRACE=3s",
		"IDEM_RETRY_BASE=100ms",
		"IDEM_RETRY_CAP=2s",
		"IDEM_SWEEP_INTERVAL=500ms",
		"IDEM_POLL_INTERVAL=50ms",
		"IDEM_STUCK_AFTER=30s",
		"IDEM_METRICS_ADDR=off", // several processes per host; nothing scrapes them
		// Payouts settle in 300ms (fake provider), far inside 30 polls: a healthy
		// run never parks a payout as callback_unresolved.
		"IDEM_CALLBACK_DEADLINE=500ms",
		"IDEM_CALLBACK_MAX_POLLS=30",
		"IDEM_CALLBACK_MAX_WAIT=30s",
		"IDEM_CALLBACK_POLL_CAP=1s",
	}
}

// Run executes one campaign: start the system, drive traffic while executing
// the fault plan, let the system settle, and check every invariant. It always
// cleans up its processes and container, and writes the report to cfg.RunDir.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) (rep Report, err error) {
	rep = Report{Seed: cfg.Seed, Config: cfg, Restarts: map[string]int{}}
	idem, provider := filepath.Join(cfg.BinDir, "idem"), filepath.Join(cfg.BinDir, "fakeprovider")
	for _, bin := range []string{idem, provider} {
		if _, err := os.Stat(bin); err != nil {
			return rep, fmt.Errorf("missing %s (run `make build`): %w", bin, err)
		}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return rep, errors.New("docker is required")
	}
	if err := os.MkdirAll(cfg.RunDir, 0o755); err != nil { //nolint:gosec // run directory
		return rep, err
	}
	logPath := func(name string) string { return filepath.Join(cfg.RunDir, name+".log") }

	// --- Infrastructure -----------------------------------------------------
	pgPort, err := FreePort()
	if err != nil {
		return rep, err
	}
	logger.Info("starting postgres", "port", pgPort)
	pg, err := StartPostgres(ctx, containerName(cfg.Seed), pgPort)
	if err != nil {
		return rep, err
	}
	defer func() {
		if rmErr := pg.Remove(context.Background()); rmErr != nil {
			logger.Error("remove postgres container", "error", rmErr)
		}
	}()

	migrate := exec.CommandContext(ctx, idem, "migrate") //nolint:gosec // harness-controlled binary
	migrate.Env = append(os.Environ(), "IDEM_DATABASE_URL="+pg.DSN())
	if out, err := migrate.CombinedOutput(); err != nil {
		return rep, fmt.Errorf("migrate: %w: %s", err, out)
	}

	pool, err := store.Open(ctx, pg.DSN())
	if err != nil {
		return rep, err
	}
	defer pool.Close()

	sup := NewSupervisor(logger)
	defer func() {
		if stopErr := sup.StopAll(context.Background()); stopErr != nil {
			logger.Error("stop processes", "error", stopErr)
		}
	}()

	providerPort, err := FreePort()
	if err != nil {
		return rep, err
	}
	apiPort, err := FreePort()
	if err != nil {
		return rep, err
	}
	providerURL := fmt.Sprintf("http://127.0.0.1:%d", providerPort)
	if _, err := sup.Start(ProcSpec{Name: "fakeprovider", Path: provider, LogPath: logPath("fakeprovider"),
		Env: providerEnv(providerPort, apiPort)}); err != nil {
		return rep, err
	}
	if err := waitHTTP(ctx, providerURL+"/_effects"); err != nil {
		return rep, fmt.Errorf("fake provider: %w", err)
	}

	apiURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	apiProc, err := sup.Start(ProcSpec{Name: "api", Path: idem, Args: []string{"api"}, LogPath: logPath("api"),
		Env: apiEnv(pg.DSN(), apiPort)})
	if err != nil {
		return rep, err
	}
	if err := waitHTTP(ctx, apiURL+"/readyz"); err != nil {
		return rep, fmt.Errorf("api: %w", err)
	}

	env := workerEnv(pg.DSN(), providerURL)
	workers := make([]*Proc, cfg.Workers)
	for i := range workers {
		if workers[i], err = sup.Start(ProcSpec{Name: fmt.Sprintf("worker-%d", i), Path: idem, Args: []string{"worker"},
			LogPath: logPath(fmt.Sprintf("worker-%d", i)),
			Env:     append(env, fmt.Sprintf("IDEM_WORKER_ID=w%d", i), "IDEM_FAILPOINTS="+cfg.Failpoints)}); err != nil {
			return rep, err
		}
	}
	for i := range cfg.Sweepers {
		if _, err := sup.Start(ProcSpec{Name: fmt.Sprintf("sweeper-%d", i), Path: idem, Args: []string{"sweeper"},
			LogPath: logPath(fmt.Sprintf("sweeper-%d", i)), Env: env}); err != nil {
			return rep, err
		}
	}

	// --- Campaign: traffic and faults --------------------------------------
	logger.Info("campaign started", "seed", cfg.Seed, "duration", cfg.Duration, "workflows", cfg.Workflows)
	clientCtx, cancelClients := context.WithTimeout(ctx, cfg.Duration+cfg.SettleTimeout)
	defer cancelClients()
	clientsDone := make(chan ClientResult, 1)
	var outstanding atomic.Int64
	go func() {
		clientsDone <- RunClients(clientCtx, ClientConfig{
			BaseURL: apiURL, Token: apiToken, Workflows: cfg.Workflows, Seed: cfg.Seed,
			// Open-loop arrivals over the whole campaign, so faults meet work in flight.
			Spread:      cfg.Duration,
			Outstanding: &outstanding,
		})
	}()

	plan := NewPlan(PlanConfig{Seed: cfg.Seed, Duration: cfg.Duration, Workers: cfg.Workers, LeaseTTL: leaseTTL})
	var pauses sync.WaitGroup
	start := time.Now()
	for _, ev := range plan {
		select {
		case <-ctx.Done():
			return rep, ctx.Err()
		case <-time.After(time.Until(start.Add(ev.At))):
		}
		inFlight := unsettled(ctx, pool) + int(outstanding.Load())
		err := apply(ctx, ev, workers, apiProc, pg, providerURL, &pauses)
		ex := ExecutedEvent{Event: ev, WallTime: time.Now(), InFlight: inFlight}
		switch {
		case errors.Is(err, ErrNotRunning):
			ex.Skipped = err.Error() // target already down (restarting): harmless
			logger.Info("event skipped", "kind", ev.Kind, "reason", err)
		case err != nil:
			ex.Err = err.Error()
			logger.Warn("event failed", "kind", ev.Kind, "error", err)
		default:
			logger.Info("event", "kind", ev.Kind, "target", ev.Target, "in_flight", inFlight)
		}
		rep.Events = append(rep.Events, ex)
	}
	select {
	case <-ctx.Done():
		return rep, ctx.Err()
	case <-time.After(time.Until(start.Add(cfg.Duration))):
	}

	// --- Settle --------------------------------------------------------------
	logger.Info("settling: faults off, workers restarted without failpoints")
	settleStart := time.Now()
	if err := fakeprovider.PutFaults(ctx, providerURL, fakeprovider.Faults{}); err != nil {
		return rep, fmt.Errorf("clear faults: %w", err)
	}
	pauses.Wait() // every pause resumes itself
	for i, w := range workers {
		if err := w.Stop(ctx); err != nil {
			return rep, err
		}
		rep.Restarts[w.Name()] = w.Restarts()
		rep.FailpointExits += w.FailpointExits()
		if _, err := sup.Start(ProcSpec{Name: fmt.Sprintf("worker-%d-settle", i), Path: idem, Args: []string{"worker"},
			LogPath: logPath(fmt.Sprintf("worker-%d", i)), Env: append(env, fmt.Sprintf("IDEM_WORKER_ID=w%d-settle", i))}); err != nil {
			return rep, err
		}
	}
	rep.Restarts[apiProc.Name()] = apiProc.Restarts()

	rep.Clients = <-clientsDone

	deadline := settleStart.Add(cfg.SettleTimeout)
	for {
		if rep.Liveness, err = invariants.CheckLiveness(ctx, pool); err != nil {
			return rep, err
		}
		if len(rep.Liveness) == 0 || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return rep, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	rep.SettleTime = time.Since(settleStart)

	// --- Verdict ---------------------------------------------------------------
	effects, err := fakeprovider.FetchEffects(ctx, providerURL)
	if err != nil {
		return rep, err
	}
	if rep.Violations, err = invariants.Check(ctx, pool, effects); err != nil {
		return rep, err
	}
	if rep.NeedsAttention, err = invariants.NeedsAttention(ctx, pool); err != nil {
		return rep, err
	}
	if rep.FaultHits, err = fakeprovider.FetchFaultHits(ctx, providerURL); err != nil {
		return rep, err
	}
	if rep.Parked, err = parked(ctx, pool); err != nil {
		return rep, err
	}
	if rep.AckMismatches, err = ackMismatches(ctx, pool, rep.Clients.Accepted); err != nil {
		return rep, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM steps s
		WHERE s.status = 'SUCCEEDED' AND EXISTS (
			SELECT 1 FROM step_attempts a
			WHERE a.step_id = s.id AND a.kind = 'send' AND a.outcome IN ('IN_DOUBT', 'ABANDONED'))`).Scan(&rep.InDoubtResolved); err != nil {
		return rep, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM step_attempts WHERE kind = 'poll'),
		       (SELECT count(DISTINCT step_id) FROM step_attempts WHERE kind = 'poll')`).
		Scan(&rep.CallbackPolls, &rep.PayoutsWaited); err != nil {
		return rep, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status = 'PROCESSED' AND note = 'applied'),
		       count(*) FILTER (WHERE status = 'PROCESSED' AND note = 'duplicate'),
		       count(*) FILTER (WHERE attempts > 1)
		FROM webhook_inbox`).Scan(&rep.Webhooks.Applied, &rep.Webhooks.Duplicates, &rep.Webhooks.Deferred); err != nil {
		return rep, err
	}
	rep.FencedCommits = countLogLines(cfg.RunDir, "worker-", "lease lost before completion")
	rep.Outcomes = map[string]int{}
	rows, err := pool.Query(ctx, `SELECT status, count(*) FROM workflows GROUP BY status`)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return rep, err
		}
		rep.Outcomes[status] = n
	}
	rows.Close()

	rep.Evaluate()
	if err := rep.Write(cfg.RunDir); err != nil {
		return rep, err
	}
	return rep, nil
}

// countLogLines counts lines containing needle in RunDir files named prefix*.
func countLogLines(dir, prefix, needle string) int {
	paths, _ := filepath.Glob(filepath.Join(dir, prefix+"*.log"))
	n := 0
	for _, p := range paths {
		data, err := os.ReadFile(p) //nolint:gosec // harness-owned log files
		if err != nil {
			continue
		}
		n += strings.Count(string(data), needle)
	}
	return n
}

// unsettled counts workflows not yet terminal or parked (0 if the database is
// unreachable, e.g. during a restart: then the event is not counted as effective).
func unsettled(ctx context.Context, pool *pgxpool.Pool) int {
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(qctx, `SELECT count(*) FROM workflows WHERE status IN ('PENDING','RUNNING','COMPENSATING')`).Scan(&n); err != nil {
		return 0
	}
	return n
}

// parked lists workflows needing attention with the step and reason that parked them.
func parked(ctx context.Context, pool *pgxpool.Pool) ([]Parked, error) {
	rows, err := pool.Query(ctx, `
		SELECT w.id::text, COALESCE(s.name, ''), COALESCE(s.dead_reason, s.status, 'unknown')
		FROM workflows w
		LEFT JOIN steps s ON s.workflow_id = w.id AND s.status IN ('DEAD', 'FAILED_PERMANENT')
		WHERE w.status = 'NEEDS_ATTENTION' ORDER BY w.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Parked
	for rows.Next() {
		var p Parked
		if err := rows.Scan(&p.WorkflowID, &p.Step, &p.Reason); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ackMismatches checks every workflow a client was told about against the
// database: the key must exist and map to the same workflow, and the database
// must hold no workflow the clients were not told about.
func ackMismatches(ctx context.Context, pool *pgxpool.Pool, accepted map[string]string) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT key, workflow_id::text FROM api_idempotency_keys WHERE client_id = 'chaos' AND workflow_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	db := map[string]string{}
	for rows.Next() {
		var key, wf string
		if err := rows.Scan(&key, &wf); err != nil {
			return nil, err
		}
		db[key] = wf
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var out []string
	for key, wf := range accepted {
		switch got, ok := db[key]; {
		case !ok:
			out = append(out, fmt.Sprintf("%s: client was told %s, database has no such key", key, wf))
		case got != wf:
			out = append(out, fmt.Sprintf("%s: client was told %s, database has %s", key, wf, got))
		}
	}
	for key, wf := range db {
		if _, ok := accepted[key]; !ok {
			out = append(out, fmt.Sprintf("%s: database has %s, no client was told", key, wf))
		}
	}
	slices.Sort(out)
	return out, nil
}

func apply(ctx context.Context, ev Event, workers []*Proc, api *Proc, pg *Postgres, providerURL string, pauses *sync.WaitGroup) error {
	switch ev.Kind {
	case KillWorker:
		return workers[ev.Target].Kill()
	case PauseWorker:
		w := workers[ev.Target]
		if err := w.Pause(); err != nil {
			return err
		}
		pauses.Add(1)
		go func() {
			defer pauses.Done()
			time.Sleep(ev.Duration)
			_ = w.Resume()
		}()
		return nil
	case RestartAPI:
		return api.Restart(ctx)
	case RestartPostgres:
		return pg.Restart(ctx)
	case SetProviderFaults:
		return fakeprovider.PutFaults(ctx, providerURL, ev.Faults)
	case ClearProviderFaults:
		return fakeprovider.PutFaults(ctx, providerURL, fakeprovider.Faults{})
	}
	return fmt.Errorf("unknown event kind %q", ev.Kind)
}

// waitHTTP polls url until it answers 200, for up to 30s.
func waitHTTP(ctx context.Context, url string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		lastErr := err
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("%s not ready: %w", url, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
