package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/google/uuid"
)

// StepIdempotencyKey derives the outbound idempotency key sent to the payment
// provider for one intended effect: hex(sha256("<workflow_id>:<step>:<generation>")).
//
// The result is stored on the step row at creation and read from there forever
// after; it must never be recomputed for an existing step (design D6).
func StepIdempotencyKey(workflowID uuid.UUID, step string, generation int) string {
	sum := sha256.Sum256([]byte(workflowID.String() + ":" + step + ":" + strconv.Itoa(generation)))
	return hex.EncodeToString(sum[:])
}
