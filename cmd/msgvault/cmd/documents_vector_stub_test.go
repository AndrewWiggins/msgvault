//go:build !sqlite_vec && !pgvector

package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocumentVectorStubGenerationIsActionable(t *testing.T) {
	_, err := runConfiguredDocumentVectorGeneration(t.Context(), nil, 1, 1)
	require.ErrorContains(t, err, "rebuild with sqlite_vec or pgvector support")
}
