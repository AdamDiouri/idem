package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ue usageError
	msg := err.Error()
	if errors.As(err, &ue) || strings.Contains(msg, "required flag") || strings.Contains(msg, "unknown flag") ||
		strings.Contains(msg, "accepts ") || strings.Contains(msg, "unknown command") {
		return 2
	}
	return 1
}

// runAdmin runs `idem admin <args>` and returns the exit code. main and the
// tests share it.
func runAdmin(args []string, stdout, stderr io.Writer) int {
	root := &cobra.Command{Use: "idem", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(adminCmd())
	root.SetArgs(append([]string{"admin"}, args...))
	root.SetOut(stdout)
	root.SetErr(stderr)
	err := root.Execute()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
	}
	return exitCode(err)
}

func adminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Inspect and resolve workflows that need a human (audited)",
		Long: "Every change requires --reason and is recorded in admin_actions.\n" +
			"See docs/runbooks/needs-attention.md for which action fits which situation.",
	}
	cmd.AddCommand(adminListCmd(), adminRetryCmd(), adminNewGenerationCmd(), adminConfirmCmd(), adminCompensateCmd(), adminFailCmd())
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

func addReason(cmd *cobra.Command, reason *string) {
	cmd.Flags().StringVar(reason, "reason", "", "why (recorded in the audit log)")
	_ = cmd.MarkFlagRequired("reason")
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
				_, _ = fmt.Fprintln(tw, "WORKFLOW\tSTEP\tSTATUS\tREASON\tATTEMPTS\tPARKED FOR\tLAST ERROR")
				for _, p := range parked {
					why := p.DeadReason
					if why == "" {
						why = "refused"
					}
					_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", p.WorkflowID, p.Step, p.StepStatus, why,
						p.Attempts, p.ParkedFor.Round(time.Second), truncate(p.LastError, 60))
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
				if err := a.RetryStep(ctx, id, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "step %s handed back to workers (same idempotency key)\n", id)
				return nil
			})
		},
	}
	addReason(cmd, &reason)
	return cmd
}

func adminNewGenerationCmd() *cobra.Command {
	var reason string
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
				newID, err := a.NewGeneration(ctx, id, reason, confirm)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "step %s superseded by new step %s (new idempotency key)\n", id, newID)
				return nil
			})
		},
	}
	addReason(cmd, &reason)
	cmd.Flags().BoolVar(&confirm, "confirm-no-effect", false, "I verified the old key moved no money")
	return cmd
}

func adminConfirmCmd() *cobra.Command {
	var reason, ref string
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
				if err := a.ConfirmStep(ctx, id, ref, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "step %s confirmed as succeeded (provider ref %s); ledger posted\n", id, ref)
				return nil
			})
		},
	}
	addReason(cmd, &reason)
	cmd.Flags().StringVar(&ref, "provider-ref", "", "provider reference of the effect (e.g. po_…)")
	_ = cmd.MarkFlagRequired("provider-ref")
	return cmd
}

func adminCompensateCmd() *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "compensate <workflow-id>",
		Short: "Refund the customer of a parked payout (merchant verified NOT paid)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.Compensate(ctx, id, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "workflow %s is compensating: refund queued\n", id)
				return nil
			})
		},
	}
	addReason(cmd, &reason)
	return cmd
}

func adminFailCmd() *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "fail <workflow-id>",
		Short: "Close a workflow whose parked charge never charged the customer",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return withAdmin(cmd, func(ctx context.Context, a *engine.Admin) error {
				if err := a.Fail(ctx, id, reason); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "workflow %s marked FAILED\n", id)
				return nil
			})
		},
	}
	addReason(cmd, &reason)
	return cmd
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
