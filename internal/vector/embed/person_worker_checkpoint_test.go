package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
	"go.kenn.io/msgvault/internal/vector"
)

func TestPersonWorkerCheckpointsPackedPrefixBeforeCancellation(t *testing.T) {
	for _, name := range []string{"unchanged", "revision changed", "budget denial"} {
		changed := name == "revision changed"
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			source := newPersonWorkerSource(personDocument(1, "one", "Synthetic One"), personDocument(2, "two", "Synthetic Two"))
			backend := newPersonWorkerBackend()
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var input voyageRequest
				if !assert.NoError(json.NewDecoder(r.Body).Decode(&input)) {
					http.Error(w, "invalid", http.StatusBadRequest)
					return
				}
				if !assert.Len(input.Inputs, 1) {
					http.Error(w, "invalid", http.StatusBadRequest)
					return
				}
				if changed && requests.Load() == 0 {
					source.put(personDocument(1, "new-one", "Updated One"))
				}
				requests.Add(1)
				_, err := w.Write([]byte(`{"data":[{"index":0,"data":[{"index":0,"embedding":[1,2]}]}]}`))
				assert.NoError(err)
			}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithCancel(jobctx.WithProgress(t.Context()))
			defer cancel()
			client := NewVoyageClient(VoyageConfig{Endpoint: server.URL, Model: "synthetic", Dimension: 2, MaxRetries: 1,
				Limits: RequestLimits{MaxDocuments: 1}, BeforeRequest: func(ctx context.Context) error {
					if requests.Load() == 1 {
						revisions, err := backend.ListPersonRevisions(t.Context(), vector.GenerationID(1))
						require.NoError(err)
						if changed {
							assert.Empty(revisions, "a stale response remains unpublished")
							assert.False(jobctx.HasProgress(ctx))
						} else {
							assert.Equal(map[int64]string{1: "one"}, revisions, "prefix is durable before the next request")
						}
						if name == "budget denial" {
							return jobctx.ErrRunBudgetExceeded
						}
						cancel()
					}
					return ctx.Err()
				},
			})
			worker := newTestPersonWorker(source, backend, client, 32)
			result, err := worker.RunOnce(ctx, 1, testEmbeddingPassScope())
			if name == "budget denial" {
				require.ErrorIs(err, jobctx.ErrRunBudgetExceeded)
				assert.Zero(result.Failed, "the unstarted person stays unfinished")
			} else {
				require.ErrorIs(err, context.Canceled)
			}
			assert.Equal(!changed, jobctx.HasProgress(ctx))
			worker.deps.Client = NewVoyageClient(VoyageConfig{Endpoint: server.URL, Model: "synthetic", Dimension: 2, MaxRetries: 1, Limits: RequestLimits{MaxDocuments: 1}})
			result, err = worker.RunOnce(t.Context(), 1, testEmbeddingPassScope())
			require.NoError(err)
			if changed {
				assert.Equal(2, result.Succeeded)
				assert.Equal(int64(3), requests.Load(), "resume retries the stale revision and unfinished person")
			} else {
				assert.Equal(1, result.Succeeded)
				assert.Equal(int64(2), requests.Load(), "restart sends only the unfinished person")
			}
		})
	}
}
