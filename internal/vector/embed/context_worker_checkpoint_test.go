//go:build sqlite_vec

package embed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
)

func TestContextWorkerStagesShiftedWindowAcrossCancellation(t *testing.T) {
	for _, change := range []string{"unchanged", "source revised", "wrong vector dimension", "anchor removed again"} {
		t.Run(change, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			f := newContextWorkerFixture(t, func(d *ContextWorkerDeps) {
				policy := AssemblyPolicy{MaxChunkRunes: 200, MaxDocumentUTF8Bytes: 334, ChatGap: 30 * time.Minute}
				d.Assembler = CompositeAssembler{Policy: policy, Chat: ChatWindowAssembler{Policy: policy}}
				d.ChangeBatchSize, d.ReconcileBatchSize = 32, 32
			})
			day := time.Date(2026, 8, 8, 9, 0, 0, 0, time.UTC)
			var ids []int64
			for i := range 6 {
				ids = append(ids, f.seed("beeper", f.chatID, day.Add(time.Duration(i)*time.Minute), "abcdefghij"))
			}
			_, err := f.run()
			require.NoError(err)
			scope := fmt.Sprintf("chat:%d:2026-08-08", f.chatID)
			before, err := f.backend.ListDocumentsForScope(t.Context(), f.gen, scope)
			require.NoError(err)
			require.Len(before, 3)
			require.Len(before[0].Members, 2)
			_, err = f.store.DB().Exec(`UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, ids[0])
			require.NoError(err)
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
				requests.Add(1)
				data := make([]map[string]any, len(input.Inputs[0]))
				for i := range data {
					data[i] = map[string]any{"index": i, "embedding": []float32{1, 2, 3, 4}}
				}
				assert.NoError(json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"index": 0, "data": data}}}))
			}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithCancel(jobctx.WithProgress(t.Context()))
			defer cancel()
			f.deps.Client = NewVoyageClient(VoyageConfig{Endpoint: server.URL, Model: "synthetic", Dimension: 4, MaxRetries: 1,
				Limits: RequestLimits{MaxDocuments: 1}, BeforeRequest: func(ctx context.Context) error {
					if requests.Load() == 1 {
						cancel()
					}
					return ctx.Err()
				},
			})
			f.restartWorker()
			_, err = f.worker.RunOnce(ctx, f.gen, testEmbeddingPassScope())
			require.ErrorIs(err, context.Canceled)
			after, err := f.backend.ListDocumentsForScope(t.Context(), f.gen, scope)
			require.NoError(err)
			assert.Equal(before, after, "old searchable membership survives an incomplete replacement")
			expectedRequests := int64(3)
			switch change {
			case "source revised":
				require.NoError(f.store.UpsertMessageBody(ids[1], sql.NullString{String: "revised source", Valid: true}, sql.NullString{}))
				expectedRequests = 4
			case "anchor removed again":
				_, err := f.store.DB().Exec(`UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, ids[1])
				require.NoError(err)
				expectedRequests = 1 // The surviving windows match the original publications.
			case "wrong vector dimension":
				_, err := f.backend.DB().Exec(`UPDATE embedding_chunk_checkpoints SET vector_json = '{"vectors":[[1,2],[1,2]]}'`)
				require.NoError(err)
				expectedRequests = 4
			}
			f.deps.Client = NewVoyageClient(VoyageConfig{Endpoint: server.URL, Model: "synthetic", Dimension: 4, MaxRetries: 1, Limits: RequestLimits{MaxDocuments: 1}})
			f.restartWorker()
			result, err := f.run()
			require.NoError(err)
			assert.True(result.Contextual.Converged)
			assert.Zero(f.missing())
			var staged int
			require.NoError(f.backend.DB().QueryRow(`SELECT COUNT(*) FROM embedding_chunk_checkpoints`).Scan(&staged))
			assert.Zero(staged, "complete replacement clears superseded document keys")
			assert.Equal(expectedRequests, requests.Load(), "restart reuses only a matching completed document")
		})
	}
}

func TestContextWorkerFailedDocumentCheckpointKeepsScopeUnpublished(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	f := newContextWorkerFixture(t, nil)
	f.seed("beeper", f.chatID, time.Date(2026, 8, 8, 9, 0, 0, 0, time.UTC), "checkpoint failure")
	_, err := f.backend.DB().Exec(`CREATE TRIGGER reject_document_checkpoint BEFORE INSERT ON embedding_chunk_checkpoints BEGIN SELECT RAISE(ABORT, 'synthetic checkpoint failure'); END`)
	require.NoError(err)
	ctx := jobctx.WithProgress(t.Context())
	_, err = f.worker.RunOnce(ctx, f.gen, testEmbeddingPassScope())
	require.ErrorContains(err, "synthetic checkpoint failure")
	assert.False(jobctx.HasProgress(ctx))
	var documents int
	require.NoError(f.backend.DB().QueryRow(`SELECT COUNT(*) FROM embedding_documents`).Scan(&documents))
	assert.Zero(documents)
	assert.Equal(1, f.missing())
}

func TestContextWorkerRestoresTruncatedDocumentCheckpoint(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var spans [2]struct {
		start, end int
		truncated  bool
	}
	for i := range 2 {
		f := newContextWorkerFixture(t, func(d *ContextWorkerDeps) {
			policy := AssemblyPolicy{MaxChunkRunes: 16384}
			d.Assembler = CompositeAssembler{Policy: policy, Chat: ChatWindowAssembler{Policy: policy}}
		})
		conversation, err := f.store.EnsureConversation(f.sourceID, "ordinary", "Ordinary")
		require.NoError(err)
		id := f.seed("email", conversation, time.Date(2026, 8, 8, 9, 0, 0, 0, time.UTC), strings.Repeat("filler words ", 700)+"reject-huge-document")
		f.client.rejectText = "reject-huge-document"
		if i == 1 {
			f.deps.Hooks.BeforePublish = func() error { return errors.New("synthetic interruption") }
			f.restartWorker()
			_, err = f.run()
			require.ErrorContains(err, "synthetic interruption")
			completed := f.client.Calls()
			f.deps.Hooks.BeforePublish = nil
			f.restartWorker()
			_, err = f.run()
			require.NoError(err)
			assert.Equal(completed, f.client.Calls(), "resume reuses the truncated provider result")
		} else {
			_, err = f.run()
			require.NoError(err)
		}
		require.NoError(f.backend.DB().QueryRow(`SELECT chunk_char_start,chunk_char_end,truncated FROM embeddings WHERE message_id = ?`, id).Scan(&spans[i].start, &spans[i].end, &spans[i].truncated))
		assert.Zero(f.missing())
		var staged int
		require.NoError(f.backend.DB().QueryRow(`SELECT COUNT(*) FROM embedding_chunk_checkpoints`).Scan(&staged))
		assert.Zero(staged)
	}
	assert.True(spans[0].truncated)
	assert.Positive(spans[0].end)
	assert.Equal(spans[0], spans[1], "resumed offsets match uninterrupted publication")
}
