package webhook

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// Inbox stores verified provider events durably (design §4.3).
type Inbox struct {
	pool *pgxpool.Pool
}

// NewInbox returns an Inbox on pool.
func NewInbox(pool *pgxpool.Pool) *Inbox { return &Inbox{pool: pool} }

// Store records the event, byte-exact, unless it is already there. inserted is
// false for a duplicate (F15). It returns once the row is committed, so the
// caller may then acknowledge the webhook.
func (i *Inbox) Store(ctx context.Context, eventID, eventType string, payload []byte) (bool, error) {
	n, err := db.New(i.pool).InsertInboxEvent(ctx, db.InsertInboxEventParams{
		ProviderEventID: eventID, Type: eventType, Payload: payload,
	})
	if err != nil {
		return false, fmt.Errorf("store webhook %s: %w", eventID, err)
	}
	return n == 1, nil
}
