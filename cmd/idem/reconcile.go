package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/config"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/recon"
	"github.com/AdamDiouri/idem/internal/store"
)

// driftFound means a clean run found discrepancies on workflows that are not
// parked: the ledger and the provider disagree (exit code 3, so a cron can
// alert on it).
type driftFound struct{ n int }

func (d driftFound) Error() string {
	return fmt.Sprintf("%d discrepancies on workflows that are not parked: see `idem admin recon list`", d.n)
}

func reconcileCmd() *cobra.Command {
	var fromFlag, toFlag string
	var every time.Duration
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Compare the provider's record of money movements with the ledger; report every difference (changes nothing)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if (fromFlag == "") != (toFlag == "") {
				return usageError{errors.New("--from and --to go together (or neither: the last complete hour)")}
			}
			var from, to time.Time
			if fromFlag != "" {
				var err1, err2 error
				from, err1 = time.Parse(time.RFC3339, fromFlag)
				to, err2 = time.Parse(time.RFC3339, toFlag)
				if err := errors.Join(err1, err2); err != nil {
					return usageError{fmt.Errorf("--from and --to must be RFC 3339 times: %w", err)}
				}
				if !to.After(from) {
					return usageError{errors.New("--from must be before --to")}
				}
			}
			cfg, err := config.LoadRecon()
			if err != nil {
				return usageError{err}
			}
			level, _ := cfg.SlogLevel()
			logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			pool, err := store.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer pool.Close()
			client := provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout)

			once := func() error {
				f, t := from, to
				if fromFlag == "" { // the last complete hour, ending IDEM_RECON_LAG ago
					t = time.Now().Add(-cfg.Lag).Truncate(time.Hour)
					f = t.Add(-time.Hour)
				}
				res, err := recon.Run(ctx, pool, client, f, t, recon.Options{Margin: cfg.Margin, Logger: logger})
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "run %s: %d records, %d workflows (%d in flight), %d discrepancies (%d parked)\n",
					res.RunID, res.Records, res.Workflows, res.InFlight, res.Discrepancies, res.Parked)
				if drift := res.Discrepancies - res.Parked; drift > 0 {
					return driftFound{drift}
				}
				return nil
			}
			if every <= 0 {
				return once()
			}
			for {
				if err := once(); err != nil && ctx.Err() == nil {
					logger.Warn("reconciliation", "error", err) // keep looping: the alerts report it
				}
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(every):
				}
			}
		},
	}
	cmd.Flags().StringVar(&fromFlag, "from", "", "window start (RFC 3339); with --to")
	cmd.Flags().StringVar(&toFlag, "to", "", "window end, exclusive (RFC 3339); with --from")
	cmd.Flags().DurationVar(&every, "every", 0, "run again every DURATION until stopped (each run: the last complete hour)")
	return cmd
}
