package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/store"
)

// usageError marks a command-line mistake (exit code 2).
type usageError struct{ error }

// exitCode maps a command error to a process exit code: 0 ok, 2 usage, 1 other.
// Usage mistakes are recognised by type only (see markUsageErrors), never by
// error text: an operational error must not exit 2 because of its wording.
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ue usageError
	if errors.As(err, &ue) || errors.Is(err, engine.ErrConfirmationRequired) {
		return 2 // ErrConfirmationRequired: a missing flag, found once the step's history is known
	}
	var drift driftFound
	if errors.As(err, &drift) {
		return 3 // reconcile ran and found the ledger and the provider disagreeing
	}
	return 1
}

// markUsageErrors makes every command-line mistake cobra can detect a
// usageError: bad or unknown flags, missing required flags, wrong argument
// counts, and unknown subcommands (which cobra would otherwise answer with
// help and exit 0).
func markUsageErrors(root *cobra.Command) {
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageError{err} })
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		if err := cmd.ValidateRequiredFlags(); err != nil {
			return usageError{err}
		}
		return nil
	}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		switch {
		case c.HasSubCommands() && c.RunE == nil && c.Run == nil:
			c.Args = cobra.ArbitraryArgs
			c.RunE = func(cmd *cobra.Command, args []string) error {
				if len(args) > 0 {
					return usageError{fmt.Errorf("unknown command %q for %q", args[0], cmd.CommandPath())}
				}
				return cmd.Help()
			}
		case c.Args != nil:
			validate := c.Args
			c.Args = func(cmd *cobra.Command, args []string) error {
				if err := validate(cmd, args); err != nil {
					return usageError{err}
				}
				return nil
			}
		}
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
}

// runAdmin runs `idem admin <args>` and returns the exit code.
func runAdmin(args []string, stdout, stderr io.Writer) int {
	return runIdem(append([]string{"admin"}, args...), stdout, stderr)
}

func adminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Inspect and resolve workflows that need a human (audited)",
		Long: "Every change requires --reason and is recorded in admin_actions.\n" +
			"See docs/runbooks/needs-attention.md for which action fits which situation.",
	}
	cmd.AddCommand(adminListCmd(), adminRetryCmd(), adminNewGenerationCmd(), adminConfirmCmd(), adminCompensateCmd(), adminFailCmd(),
		adminConflictCmd(), adminOutboxCmd(), adminReconCmd())
	return cmd
}

// withAdmin opens the database and runs fn with an Admin. Only
// IDEM_DATABASE_URL is needed; the actor is IDEM_ADMIN_ACTOR or $USER.
func withAdmin(cmd *cobra.Command, fn func(ctx context.Context, a *engine.Admin) error) error {
	dsn := os.Getenv("IDEM_DATABASE_URL")
	if dsn == "" {
		return usageError{errors.New("IDEM_DATABASE_URL is required")}
	}
	actor := os.Getenv("IDEM_ADMIN_ACTOR")
	if actor == "" {
		actor = os.Getenv("USER")
	}
	if actor == "" {
		actor = "unknown"
	}
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	pool, err := store.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	// The steps provide hooks (ledger, saga); admin never calls the provider.
	steps := []engine.Step{engine.NewChargeStep(nil), engine.NewPayoutStep(nil), engine.NewRefundStep(nil)}
	return fn(ctx, engine.NewAdmin(pool, steps, actor))
}

func parseID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, usageError{fmt.Errorf("invalid id %q: %w", s, err)}
	}
	return id, nil
}

// addMutationFlags adds the flags every mutating command requires: why, and
// the workflow version the decision was made on (the VERSION column of
// `idem admin list`). A version that moved on means the facts changed.
func addMutationFlags(cmd *cobra.Command, reason *string, version *int64) {
	cmd.Flags().StringVar(reason, "reason", "", "why (recorded in the audit log)")
	_ = cmd.MarkFlagRequired("reason")
	cmd.Flags().Int64Var(version, "expect-version", 0,
		"the VERSION shown by `idem admin list`; refused if the workflow changed since")
	_ = cmd.MarkFlagRequired("expect-version")
}

func adminListCmd() *cobra.Command {
	var reason string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List workflows that need attention",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				parked, err := a.ListParked(ctx, reason)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				if asJSON {
					enc := json.NewEncoder(out)
					enc.SetIndent("", "  ")
					return enc.Encode(parked)
				}
				tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "WORKFLOW\tVERSION\tSTEP\tSTATUS\tREASON\tATTEMPTS\tPOLLS\tPARKED FOR\tLAST ERROR\tPROVIDER SAYS\tRECON")
				for _, p := range parked {
					why := p.DeadReason
					if why == "" {
						why = "refused"
					}
					_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n", p.WorkflowID, p.Version, p.Step, p.StepStatus, why,
						p.Attempts, p.Polls, p.ParkedFor.Round(time.Second), truncate(p.LastError, 60), p.Evidence, p.Reconciliation)
				}
				return tw.Flush()
			})
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "only this dead_reason")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func adminRetryCmd() *cobra.Command {
	var reason string
	var version int64
	cmd := &cobra.Command{
		Use:   "retry-step <step-id>",
		Short: "Retry a parked step with the SAME idempotency key (always safe)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.RetryStep(ctx, id, version, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "step %s handed back to workers (same idempotency key)\n", id)
				return nil
			})
		},
	}
	addMutationFlags(cmd, &reason, &version)
	return cmd
}

func adminNewGenerationCmd() *cobra.Command {
	var reason string
	var version int64
	var confirm bool
	cmd := &cobra.Command{
		Use:   "new-generation <step-id>",
		Short: "DANGEROUS: re-send a parked step under a NEW idempotency key",
		Long: "Only after verifying with the provider that the old key moved no money:\n" +
			"to the provider this is a brand-new payment.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			if !confirm {
				return usageError{errors.New("new-generation mints a new idempotency key; pass --confirm-no-effect after verifying the old key moved no money")}
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				newID, err := a.NewGeneration(ctx, id, version, reason, confirm)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "step %s superseded by new step %s (new idempotency key)\n", id, newID)
				return nil
			})
		},
	}
	addMutationFlags(cmd, &reason, &version)
	cmd.Flags().BoolVar(&confirm, "confirm-no-effect", false, "I verified the old key moved no money")
	return cmd
}

func adminConfirmCmd() *cobra.Command {
	var reason, ref string
	var version int64
	cmd := &cobra.Command{
		Use:   "confirm-step <step-id>",
		Short: "Record that a dead step's effect DID happen (posts the ledger, advances the saga)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.ConfirmStep(ctx, id, version, ref, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "step %s confirmed as succeeded (provider ref %s); ledger posted\n", id, ref)
				return nil
			})
		},
	}
	addMutationFlags(cmd, &reason, &version)
	cmd.Flags().StringVar(&ref, "provider-ref", "", "provider reference of the effect (e.g. po_…)")
	_ = cmd.MarkFlagRequired("provider-ref")
	return cmd
}

func adminCompensateCmd() *cobra.Command {
	var reason string
	var version int64
	var confirm bool
	cmd := &cobra.Command{
		Use:   "compensate <workflow-id>",
		Short: "Refund the customer of a parked payout (merchant verified NOT paid)",
		Long: "If the payout may have paid the merchant (an ambiguous attempt), --confirm-no-effect is required:\n" +
			"refunding a customer whose merchant was paid pays out twice.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.Compensate(ctx, id, version, reason, confirm); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "workflow %s is compensating: refund queued\n", id)
				return nil
			})
		},
	}
	addMutationFlags(cmd, &reason, &version)
	cmd.Flags().BoolVar(&confirm, "confirm-no-effect", false,
		"required when money may have moved: I verified with the provider that it did not")
	return cmd
}

func adminFailCmd() *cobra.Command {
	var reason string
	var version int64
	var confirm bool
	cmd := &cobra.Command{
		Use:   "fail <workflow-id>",
		Short: "Close a workflow whose parked charge never charged the customer",
		Long:  "If the charge may have happened (an ambiguous attempt), --confirm-no-effect is required.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.Fail(ctx, id, version, reason, confirm); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "workflow %s marked FAILED\n", id)
				return nil
			})
		},
	}
	addMutationFlags(cmd, &reason, &version)
	cmd.Flags().BoolVar(&confirm, "confirm-no-effect", false,
		"required when money may have moved: I verified with the provider that it did not")
	return cmd
}

// adminConflictCmd groups the commands for real provider conflicts (D16).
func adminConflictCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "conflict",
		Short: "Provider news that contradicts idem: money may have moved both ways",
	}
	var asJSON bool
	list := &cobra.Command{
		Use:   "list",
		Short: "List open conflicts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				conflicts, err := a.ListConflicts(ctx)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				if asJSON {
					enc := json.NewEncoder(out)
					enc.SetIndent("", "  ")
					return enc.Encode(conflicts)
				}
				tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "EVENT\tWORKFLOW\tTYPE\tRECEIVED\tNOTE")
				for _, c := range conflicts {
					_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", c.EventID, c.WorkflowID, c.Type,
						c.ReceivedAt.UTC().Format(time.RFC3339), c.Note)
				}
				return tw.Flush()
			})
		},
	}
	list.Flags().BoolVar(&asJSON, "json", false, "print JSON")

	var reason string
	resolve := &cobra.Command{
		Use:   "resolve <event-id>",
		Short: "Record that a conflict was resolved outside idem (audited)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.ResolveConflict(ctx, args[0], reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "conflict %s resolved\n", args[0])
				return nil
			})
		},
	}
	resolve.Flags().StringVar(&reason, "reason", "", "what was done (recorded in the audit log)")
	_ = resolve.MarkFlagRequired("reason")
	cmd.AddCommand(list, resolve)
	return cmd
}

// adminOutboxCmd lists and redelivers workflow outcomes that have not
// reached their shop (slice 2.5).
func adminOutboxCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "outbox",
		Short: "Workflow outcomes not delivered to their shop",
	}
	var asJSON bool
	var filter engine.OutboxFilter
	list := &cobra.Command{
		Use:   "list",
		Short: "List events not delivered (PENDING, DEAD, NO_ENDPOINT, SKIPPED), oldest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				events, err := a.ListOutbox(ctx, filter)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				if asJSON {
					enc := json.NewEncoder(out)
					enc.SetIndent("", "  ")
					return enc.Encode(events)
				}
				tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "EVENT\tVERSION\tWORKFLOW\tCLIENT\tTYPE\tSTATUS\tATTEMPTS\tCREATED\tLAST ERROR")
				for _, e := range events {
					_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", e.ID, e.Version, e.WorkflowID, e.ClientID, e.Type,
						e.Status, e.Attempts, e.CreatedAt.UTC().Format(time.RFC3339), truncate(e.LastError, 60))
				}
				return tw.Flush()
			})
		},
	}
	list.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	list.Flags().BoolVar(&filter.OnlyDead, "dead", false, "only events the relay gave up on")
	list.Flags().BoolVar(&filter.All, "all", false, "delivered events too (to resend one a shop lost)")

	var reason string
	var version int64
	redeliver := &cobra.Command{
		Use:   "redeliver <event-id>",
		Short: "Send an event again with a fresh retry budget (audited)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.Redeliver(ctx, id, version, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "event %s queued for delivery\n", id)
				return nil
			})
		},
	}
	redeliver.Flags().StringVar(&reason, "reason", "", "why (recorded in the audit log)")
	_ = redeliver.MarkFlagRequired("reason")
	redeliver.Flags().Int64Var(&version, "expect-version", 0,
		"the VERSION shown by `idem admin outbox list`; refused if the event changed since")
	_ = redeliver.MarkFlagRequired("expect-version")

	var bulkReason, client, status string
	redeliverAll := &cobra.Command{
		Use:   "redeliver-all",
		Short: "Send again every event of one client in one status: DEAD, NO_ENDPOINT or SKIPPED (audited, one row per event)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				n, err := a.RedeliverAll(ctx, client, status, bulkReason)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%d event(s) of client %s queued for delivery\n", n, client)
				return nil
			})
		},
	}
	redeliverAll.Flags().StringVar(&client, "client", "", "the client whose events to resend")
	redeliverAll.Flags().StringVar(&status, "status", "", "DEAD, NO_ENDPOINT or SKIPPED")
	redeliverAll.Flags().StringVar(&bulkReason, "reason", "", "why (recorded in the audit log, once per event)")
	for _, f := range []string{"client", "status", "reason"} {
		_ = redeliverAll.MarkFlagRequired(f)
	}
	cmd.AddCommand(list, redeliver, redeliverAll)
	return cmd
}

// adminReconCmd shows what reconciliation found (slice 2.6).
func adminReconCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "recon", Short: "Differences between the provider's records and the ledger"}
	var asJSON bool
	var runFlag string
	list := &cobra.Command{
		Use:   "list",
		Short: "List the discrepancies of the latest successful reconciliation (or --run)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var runID *uuid.UUID
			if runFlag != "" {
				id, err := parseID(runFlag)
				if err != nil {
					return err
				}
				runID = &id
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				run, ds, err := a.ListReconciliation(ctx, runID)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				if asJSON {
					enc := json.NewEncoder(out)
					enc.SetIndent("", "  ")
					return enc.Encode(map[string]any{"run": run, "discrepancies": ds})
				}
				_, _ = fmt.Fprintf(out, "run %s: window %s to %s, finished %s; %d workflows (%d in flight), %d discrepancies\n\n",
					run.ID, run.WindowFrom.UTC().Format(time.RFC3339), run.WindowTo.UTC().Format(time.RFC3339),
					run.FinishedAt.UTC().Format(time.RFC3339), run.Workflows, run.InFlight, run.Discrepancies)
				tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "WORKFLOW\tKIND\tTYPE\tPROVIDER\tLEDGER\tPARKED\tDETAIL")
				for _, d := range ds {
					wf := "-"
					if d.WorkflowID != nil {
						wf = d.WorkflowID.String()
					}
					_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%v\t%s\n", wf, d.Kind, d.EffectType, amount(d.ProviderAmount),
						amount(d.LedgerAmount), d.Parked, truncate(d.Detail, 80))
				}
				return tw.Flush()
			})
		},
	}
	list.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	list.Flags().StringVar(&runFlag, "run", "", "a run ID instead of the latest")
	cmd.AddCommand(list)
	return cmd
}

func amount(v *int64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatInt(*v, 10)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
