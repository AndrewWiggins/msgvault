package document

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/vector/embed"
)

func TestWorkerResumesPackedRequestsAcrossCancellation(t *testing.T) {
	f := seedSemanticSearch(t, "first", "second", "third", "fourth")
	spec := f.generation.DocumentVectorGenerationSpec
	spec.Fingerprint = strings.Repeat("e", 64)
	gen, _, err := f.store.Store.EnsureDocumentVectorGeneration(t.Context(), spec)
	require.NoError(t, err)
	var completed atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		var request struct {
			Inputs [][]string `json:"inputs"`
		}
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&request)) || !assert.Len(t, request.Inputs, 1) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		assert.Len(t, request.Inputs[0], 1, "contextual extraction chunks keep their stable document boundary")
		completed.Add(1)
		_, err := w.Write([]byte(`{"data":[{"index":0,"data":[{"index":0,"embedding":[1,2,3]}]}]}`))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(jobctx.WithProgress(t.Context()))
	defer cancel()
	client := embed.NewVoyageClient(embed.VoyageConfig{
		Endpoint: server.URL, Model: "synthetic", Dimension: 3, Timeout: 2 * time.Minute, MaxRetries: 1,
		Limits: embed.RequestLimits{MaxDocuments: 1},
		BeforeRequest: func(requestCtx context.Context) error {
			if completed.Load() == 3 {
				cancel()
			}
			return requestCtx.Err()
		},
	})
	backend := &fakeDocumentVectorBackend{put: func(ctx context.Context, _ []Embedding) error { return ctx.Err() }}
	now := time.Now().UTC()
	deps := WorkerDeps{
		Now:    func() time.Time { return now },
		Ledger: f.store.Store, Provider: client, Backend: backend,
		Owner: "deadline-worker", Dimension: 3, MaxInputChars: 1000,
		LeaseDuration: 2 * time.Minute, HeartbeatInterval: 20 * time.Second,
		RetryDelay: time.Minute, MaxAttempts: 5, ContextualDocuments: true,
		// The fixture already supplies extracted chunk text. This test exercises
		// request packing, claim fencing, and publication through the real ledger.
		prepareInputs: func(_ context.Context, _ Ledger, _ Recipe, claims []*store.DocumentVectorChunkClaim) (map[string]string, error) {
			texts := make(map[string]string, len(claims))
			for _, claim := range claims {
				texts[claim.Token] = claim.Text
			}
			return texts, nil
		},
	}

	require := require.New(t)
	assert := assert.New(t)

	result, err := NewWorker(deps).Run(ctx, GenerationID(gen.ID), 10)
	require.ErrorIs(err, context.Canceled)
	assert.Equal(3, result.Published)
	assert.True(jobctx.HasProgress(ctx))
	coverage, err := f.store.Store.GetDocumentVectorCoverage(t.Context(), gen.ID)
	require.NoError(err)
	assert.Equal(int64(3), coverage.Ready)
	// A stopped worker leaves only the unfinished request leased. The next
	// scheduler pass resumes it after the lease expires.
	now = now.Add(2 * time.Minute)
	ctx, cancelResume := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancelResume()
	result, err = NewWorker(deps).Run(ctx, GenerationID(gen.ID), 10)
	require.NoError(err)
	assert.Equal(1, result.Published)
	coverage, err = f.store.Store.GetDocumentVectorCoverage(t.Context(), gen.ID)
	require.NoError(err)
	assert.Equal(int64(4), coverage.Ready)
	assert.Equal(int64(4), completed.Load(), "published requests are not sent again")
}
