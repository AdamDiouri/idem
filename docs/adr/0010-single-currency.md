# D10. A single currency (EUR), with a `currency` column kept

**Status:** Accepted 2026-11-08 · built in slice 1.1

## Context
Multi-currency needs exchange rates, per-currency accounts and rounding rules, none of which teach anything about retries and crash safety.

## Decision
Only EUR is accepted. Every amount still carries a `currency` column, constrained to `'EUR'`.

## Consequences
- Adding currencies later is a constraint change and new accounts, not a schema rewrite.
- Requests in any other currency are rejected at validation.

## Alternatives rejected
- **Multi-currency from day one:** cost without learning value for this project.
