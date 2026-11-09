package workflow_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/workflow"
)

func TestStepIdempotencyKey_Golden(t *testing.T) {
	id := uuid.MustParse("01936f3e-8c2a-7b1e-9f00-000000000001")
	require.Equal(t, "5ed0d798d03e205ca7bb0a9b68179ac2a558f9467a1255e8108b3e170c59af24",
		workflow.StepIdempotencyKey(id, "charge", 0))
	require.Equal(t, "c49a445fb3a15237bf4c53a3c36a75f145786cf63862f4cb74041cee46f52be4",
		workflow.StepIdempotencyKey(id, "charge", 1))
}
