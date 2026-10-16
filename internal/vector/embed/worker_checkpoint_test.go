//go:build sqlite_vec

package embed

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
)

type slowChunkClient struct {
	EmbeddingClient

	completedInputs int
}

func (c *slowChunkClient) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	select {
	case <-time.After(90 * time.Second):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	vectors, err := c.EmbeddingClient.Embed(ctx, inputs)
	if err == nil {
		c.completedInputs += len(inputs)
	}
	return vectors, err
}

func TestWorkerResumesChunksAcrossPassDeadlines(t *testing.T) {
	for _, edited := range []bool{false, true} {
		name := "unchanged"
		if edited {
			name = "edited between passes"
		}
		t.Run(name, func(t *testing.T) {
			f := newWorkerFixture(t, 2)
			_, err := f.MainDB.Exec(`UPDATE messages SET subject = ''`)
			require.NoError(t, err)
			_, err = f.MainDB.Exec(`UPDATE message_bodies SET body_text = ? WHERE message_id = 1`, strings.Repeat("a", 1280))
			require.NoError(t, err)
			_, err = f.MainDB.Exec(`UPDATE message_bodies SET body_text = 'later' WHERE message_id = 2`)
			require.NoError(t, err)
			synctest.Test(t, func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)
				client := &slowChunkClient{EmbeddingClient: f.FakeClient}
				passes, completedInputs := 2, 17
				if edited {
					passes, completedInputs = 3, 29
				}
				for pass := range passes {
					if edited && pass == 1 {
						_, err := f.MainDB.Exec(`UPDATE message_bodies SET body_text = ? WHERE message_id = 1`, strings.Repeat("b", 1280))
						require.NoError(err)
					}
					ctx, cancel := context.WithTimeoutCause(jobctx.WithProgress(t.Context()), 5*time.Minute, jobctx.ErrRunBudgetExceeded)
					worker := newTestWorker(f, 4)
					worker.deps.MaxInputChars = 80
					worker.deps.Client = client
					result, err := worker.RunOnce(ctx, f.BuildingGen, testEmbeddingPassScope())
					require.NoError(err)
					if pass < passes-1 {
						require.ErrorIs(context.Cause(ctx), jobctx.ErrRunBudgetExceeded)
						assert.True(jobctx.HasProgress(ctx), "completed requests retain the scheduler follow-up")
						assert.Zero(result.Succeeded, "a partial message is not covered")
						assert.Equal(2, countMissing(t, f.MainDB, int64(f.BuildingGen)))
						var published int
						require.NoError(f.VectorsDB.QueryRow(`SELECT COUNT(*) FROM embeddings`).Scan(&published))
						assert.Zero(published, "partial messages stay outside the search index")
					} else {
						assert.Equal(2, result.Succeeded)
						assert.Zero(countMissing(t, f.MainDB, int64(f.BuildingGen)))
					}
					cancel()
				}
				assert.Equal(completedInputs, client.completedInputs, "reuse only chunks whose inputs are unchanged")
				var chunks, checkpoints int
				require.NoError(f.VectorsDB.QueryRow(`SELECT COUNT(*) FROM embeddings WHERE generation_id = ?`, f.BuildingGen).Scan(&chunks))
				assert.Equal(17, chunks, "all chunks and the later message are published")
				require.NoError(f.VectorsDB.QueryRow(`SELECT COUNT(*) FROM embedding_chunk_checkpoints`).Scan(&checkpoints))
				assert.Zero(checkpoints, "publication removes staging data")
			})
		})
	}
}

func TestWorkerFailedChunkCheckpointDoesNotRecordProgress(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	f := newWorkerFixture(t, 1)
	_, err := f.MainDB.Exec(`UPDATE message_bodies SET body_text = ?`, strings.Repeat("a", 1280))
	require.NoError(err)
	_, err = f.VectorsDB.Exec(`CREATE TRIGGER reject_chunk_checkpoint BEFORE INSERT ON embedding_chunk_checkpoints
		BEGIN SELECT RAISE(ABORT, 'synthetic checkpoint failure'); END`)
	require.NoError(err)
	worker := newTestWorker(f, 4)
	worker.deps.MaxInputChars = 80
	worker.deps.MaxConsecutiveFailures = 1
	ctx := jobctx.WithProgress(t.Context())
	_, err = worker.RunOnce(ctx, f.BuildingGen, testEmbeddingPassScope())
	require.ErrorContains(err, "synthetic checkpoint failure")
	assert.False(jobctx.HasProgress(ctx))
	assert.Equal(1, countMissing(t, f.MainDB, int64(f.BuildingGen)))
}

func TestWorkerCheckpointCleanupFailurePreservesCompletedWork(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	f := newWorkerFixture(t, 1)
	_, err := f.MainDB.Exec(`UPDATE messages SET subject = ''`)
	require.NoError(err)
	_, err = f.MainDB.Exec(`UPDATE message_bodies SET body_text = ?`, strings.Repeat("a", 1280))
	require.NoError(err)
	_, err = f.VectorsDB.Exec(`CREATE TRIGGER reject_checkpoint_cleanup BEFORE DELETE ON embedding_chunk_checkpoints
		BEGIN SELECT RAISE(ABORT, 'synthetic checkpoint cleanup failure'); END`)
	require.NoError(err)
	worker := newTestWorker(f, 4)
	worker.deps.MaxInputChars = 80
	worker.deps.MaxConsecutiveFailures = 1
	var logs bytes.Buffer
	worker.deps.Log = slog.New(slog.NewTextHandler(&logs, nil))
	ctx := jobctx.WithProgress(t.Context())
	result, err := worker.RunOnce(ctx, f.BuildingGen, testEmbeddingPassScope())
	require.NoError(err, "cleanup must not turn committed publication into a worker failure")
	assert.Equal(1, result.Succeeded)
	assert.Zero(result.Failed)
	assert.Zero(countMissing(t, f.MainDB, int64(f.BuildingGen)))
	assert.True(jobctx.HasProgress(ctx))
	var published, checkpoints int
	require.NoError(f.VectorsDB.QueryRow(`SELECT COUNT(*) FROM embeddings WHERE generation_id = ?`, f.BuildingGen).Scan(&published))
	assert.Equal(16, published)
	require.NoError(f.VectorsDB.QueryRow(`SELECT COUNT(*) FROM embedding_chunk_checkpoints`).Scan(&checkpoints))
	assert.Positive(checkpoints, "the failed deletion leaves staging rows behind")
	assert.Contains(logs.String(), "level=WARN")
	assert.Contains(logs.String(), "checkpoint cleanup failed after coverage committed")
	assert.Contains(logs.String(), "synthetic checkpoint cleanup failure")
}

func TestWorkerBudgetDenialLeavesUnstartedMessagesUnfailed(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	f := newWorkerFixture(t, 2)
	calls := 0
	f.FakeClient.OnEmbed = func(inputs []string) ([][]float32, error) {
		calls++
		if calls == 2 {
			return nil, jobctx.ErrRunBudgetExceeded
		}
		return [][]float32{{1, 2, 3, 4}}, nil
	}
	worker := newTestWorker(f, 1)
	worker.deps.MaxConsecutiveFailures = 1
	ctx := jobctx.WithProgress(t.Context())
	result, err := worker.RunOnce(ctx, f.BuildingGen, testEmbeddingPassScope())
	require.ErrorIs(err, jobctx.ErrRunBudgetExceeded)
	assert.Equal(1, result.Succeeded)
	assert.Zero(result.Failed, "a denied request leaves its message unfinished")
	assert.Equal(1, countMissing(t, f.MainDB, int64(f.BuildingGen)))
	assert.True(jobctx.HasProgress(ctx))
}
