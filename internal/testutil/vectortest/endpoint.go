// Package vectortest shares generation upgrade scenarios across vector backends.
package vectortest

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/vector"
)

// EndpointAdoption checks persisted identities across backend opens.
// The caller owns backend creation, reopening, and cleanup.
func EndpointAdoption(t *testing.T, b vector.Backend, db *sql.DB, rebind func(string) string, reopen func(string) vector.Backend, writable bool) {
	t.Helper()
	ctx := t.Context()
	const legacy = "m:768:p1-111111:c0:e1"
	const differentPolicy = "m:768:p1-111111:c6000:e1:smt-email"
	cfg := vector.Config{Embeddings: vector.EmbeddingsConfig{Model: "m", Dimension: 768, Endpoint: "https://first.example/v1"}}
	hash := cfg.Embeddings.EndpointFingerprint()
	retired, err := b.CreateGeneration(ctx, "m", 768, legacy)
	require.NoError(t, err)
	require.NoError(t, b.RetireGeneration(ctx, retired, true))
	active, err := b.CreateGeneration(ctx, "m", 768, legacy)
	require.NoError(t, err)
	require.NoError(t, b.ActivateGeneration(ctx, active, true))
	building, err := b.CreateGeneration(ctx, "m", 768, differentPolicy)
	require.NoError(t, err)
	b = reopen(hash)
	suffix := ""
	if writable {
		suffix = ":u" + hash
	}
	got, err := b.ActiveGeneration(ctx)
	require.NoError(t, err)
	assert.Equal(t, active, got.ID)
	assert.Equal(t, legacy+suffix, got.Fingerprint)
	pending, err := b.BuildingGeneration(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, building, pending.ID)
	assert.Equal(t, differentPolicy+suffix, pending.Fingerprint)
	var retiredFP string
	require.NoError(t, db.QueryRowContext(ctx, rebind(`SELECT fingerprint FROM index_generations WHERE id = ?`), int64(retired)).Scan(&retiredFP))
	assert.Equal(t, legacy, retiredFP)
	if !writable {
		return
	}
	cfg.Embeddings.Endpoint = "https://second.example/v1"
	b = reopen(cfg.Embeddings.EndpointFingerprint())
	_, err = vector.ResolveActiveForFingerprint(ctx, b, cfg.GenerationFingerprint())
	require.ErrorIs(t, err, vector.ErrIndexStale)
	cfg.Embeddings.Endpoint = "https://first.example/v1"
	got, err = vector.ResolveActiveForFingerprint(ctx, b, cfg.GenerationFingerprint())
	require.NoError(t, err)
	assert.Equal(t, active, got.ID)
	// A future release may append another segment after the endpoint.
	require.NoError(t, b.RetireGeneration(ctx, building, true))
	future := differentPolicy + ":u" + strings.Repeat("a", 64) + ":future1"
	building, err = b.CreateGeneration(ctx, "m", 768, future)
	require.NoError(t, err)
	b = reopen(hash)
	pending, err = b.BuildingGeneration(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, building, pending.ID)
	assert.Equal(t, future, pending.Fingerprint)
}
