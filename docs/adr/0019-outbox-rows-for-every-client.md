# D19. Outbox rows are written for every client, even without a webhook URL

**Status:** Accepted 2026-11-10 (owner's decision) · built in slice 2.5 ([M2 design](../superpowers/specs/2026-11-10-milestone-2-design.md) §5.3)

## Context
Some clients configure no webhook URL and integrate by polling `GET /workflows/{id}`. Skipping their outbox rows would special-case invariant I8 and lose the history if a URL is added later. Marking them delivered would be false; alerting on them would page for a legitimate integration.

## Decision
Outbox rows are written for every client. The relay marks rows of clients without a URL `NO_ENDPOINT`: not delivered, not alerted. `idem admin outbox redeliver` can send them once a URL exists. The relay logs at startup which clients have no URL.

## Consequences
- I8 (every terminal transition has exactly one outbox event) has no exceptions.
- Status values say what happened: nothing was delivered.

## Alternatives rejected
- **Skip rows for clients without a URL:** a special case in the invariant and lost history.
- **Mark them delivered:** a lie in the data.
