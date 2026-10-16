//go:build sqlite_vec

package embed

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
)

func TestContextWorkerResumesPackedRequestsAcrossCancellation(t *testing.T) {
	var completed atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		var request voyageRequest
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&request)) || !assert.Len(t, request.Inputs, 1) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		assert.Len(t, request.Inputs[0], 2, "both messages in a contextual window travel together")
		data := make([]map[string]any, len(request.Inputs[0]))
		for i := range data {
			data[i] = map[string]any{"index": i, "embedding": []float32{1, 2, 3, 4}}
		}
		completed.Add(1)
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"index": 0, "data": data}}}))
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(jobctx.WithProgress(t.Context()))
	defer cancel()
	f := newContextWorkerFixture(t, func(deps *ContextWorkerDeps) {
		deps.ChangeBatchSize, deps.ReconcileBatchSize = 32, 32
		deps.Client = NewVoyageClient(VoyageConfig{
			Endpoint: server.URL, Model: "synthetic", Dimension: 4, Timeout: 2 * time.Minute, MaxRetries: 1,
			Limits: RequestLimits{MaxDocuments: 1},
			BeforeRequest: func(requestCtx context.Context) error {
				if completed.Load() == 3 {
					cancel()
				}
				return requestCtx.Err()
			},
		})
	})
	day := time.Date(2026, 8, 8, 9, 0, 0, 0, time.UTC)
	for i := range 4 {
		f.seed("beeper", f.chatID, day.Add(time.Duration(i)*time.Hour), "first message")
		f.seed("beeper", f.chatID, day.Add(time.Duration(i)*time.Hour+time.Minute), "second message")
	}

	require := require.New(t)
	assert := assert.New(t)

	_, err := f.worker.RunOnce(ctx, f.gen, testEmbeddingPassScope())
	require.ErrorIs(err, context.Canceled)
	assert.True(jobctx.HasProgress(ctx))
	docs, err := f.backend.ListDocumentsForScope(t.Context(), f.gen, fmt.Sprintf("chat:%d:2026-08-08", f.chatID))
	require.NoError(err)
	assert.Len(docs, 3, "each completed request is durable before starting the next")
	assert.Equal(8, f.missing(), "coverage waits for the complete source scope")
	f.restartWorker()
	ctx, cancelResume := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancelResume()
	result, err := f.worker.RunOnce(ctx, f.gen, testEmbeddingPassScope())
	require.NoError(err)
	assert.True(result.Contextual.Converged)
	assert.Zero(f.missing())
	assert.Equal(int64(4), completed.Load(), "resume does not repeat completed provider requests")
}
