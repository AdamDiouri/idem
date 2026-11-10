# D14. Outbound delivery is at least once, with a stable event ID

**Status:** Accepted 2026-11-10 · not yet built (planned for slice 2.5; [M2 design](../superpowers/specs/2026-11-10-milestone-2-design.md) §5.3)

## Context
A relay that crashes after the shop answered `2xx` but before marking the event delivered cannot know whether the shop got it. Exactly-once delivery over HTTP is not achievable without the receiver's help.

## Decision
The relay (`idem relay`, any number may run) delivers each outbox row at least once, signed, with `Idem-Event-Id` set to the outbox row's ID. Shops deduplicate on that ID; the README tells them how. Failed deliveries back off with jitter and end `DEAD` after `IDEM_RELAY_MAX_ATTEMPTS`, with an alert and an admin redeliver command.

## Consequences
- Effectively-once at the shop, with the shop's cooperation, which is how real payment providers do it.
- No ordering promise is needed: a workflow has exactly one terminal event.

## Alternatives rejected
- **At most once:** a crash loses the outcome; the shop never learns it.
- **Claiming exactly once:** not true over HTTP.
