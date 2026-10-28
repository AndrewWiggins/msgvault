package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

func TestIncrementalSyncDoesNotReplayPurgedAcquisition(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	env := newTestEnv(t)
	source := seedReplaySource(t, env, "recovered")
	pending, err := env.Syncer.pendingFetchReplayIDs(source.ID)
	require.NoError(err)
	require.Equal([]string{"recovered"}, pending)
	seedMessages(env, 2, 1001, "keep", "recovered")
	env.Mock.MessagePages = [][]string{{"keep", "recovered"}}

	// Full recovery commits the RAW before attempting this separate journal write.
	_, err = env.Store.DB().Exec(`
		CREATE TRIGGER fail_acquisition_resolution BEFORE INSERT ON sync_run_items
		WHEN NEW.source_message_id = 'recovered' AND NEW.status = 'resolved'
		  AND NEW.error_kind = 'acquired'
		BEGIN
			SELECT RAISE(ABORT, 'synthetic acquisition resolution failure');
		END
	`)
	require.NoError(err)
	_, err = env.Syncer.Full(env.Context, testEmail)
	require.NoError(err)
	ids, err := env.Store.MessageExistsBatch(source.ID, []string{"keep", "recovered"})
	require.NoError(err)
	require.Len(ids, 2)
	raw, err := env.Store.GetMessageRaw(ids["recovered"])
	require.NoError(err)
	require.Equal(testMIME(), raw)
	var resolutions int
	require.NoError(env.Store.DB().QueryRow(`
		SELECT COUNT(*) FROM sync_run_items
		WHERE source_message_id = 'recovered' AND status = 'resolved'
	`).Scan(&resolutions))
	require.Zero(resolutions)
	_, err = env.Store.DB().Exec(`DROP TRIGGER fail_acquisition_resolution`)
	require.NoError(err)

	var sequence int
	var database, dbPath string
	require.NoError(env.Store.DB().QueryRow(`PRAGMA database_list`).Scan(&sequence, &database, &dbPath))
	require.NoError(env.Store.Close())
	env.Store, err = store.Open(dbPath)
	require.NoError(err)
	t.Cleanup(func() { require.NoError(env.Store.Close()) })
	env.Syncer = New(env.Mock, env.Store, nil)
	raw, err = env.Store.GetMessageRaw(ids["recovered"])
	require.NoError(err)
	require.Equal(testMIME(), raw, "recovered RAW survives closing and reopening SQLite")

	_, err = env.Store.MergeDuplicates(ids["keep"], []int64{ids["recovered"]}, "purge-replay")
	require.NoError(err)
	deleted, err := env.Store.DeleteDedupedBatch("purge-replay")
	require.NoError(err)
	require.Equal(int64(1), deleted)
	remaining, err := env.Store.MessageExistsBatch(source.ID, []string{"recovered"})
	require.NoError(err)
	require.Empty(remaining, "the real dedup purge removed the recovered occurrence")

	// The provider has no new events; only the earlier acquisition debt can replay.
	source, err = env.Store.GetSourceByID(source.ID)
	require.NoError(err)
	require.Equal("1001", source.SyncCursor.String)
	env.SetHistory(1001)
	calls := len(env.Mock.GetMessageCalls)
	_, err = env.Syncer.Incremental(env.Context, source)
	require.NoError(err)
	remaining, err = env.Store.MessageExistsBatch(source.ID, []string{"recovered"})
	require.NoError(err)
	assert.Empty(remaining, "idle incremental sync must not recreate an intentionally purged Gmail message")
	assert.Len(env.Mock.GetMessageCalls, calls, "purged acquisition debt must not trigger another download")
	if len(remaining) > 0 {
		// A recreated row is no longer dedup-hidden, so repeating the purge cannot remove it.
		deleted, err = env.Store.DeleteDedupedBatch("purge-replay")
		require.NoError(err)
		assert.Zero(deleted, "the recreated message is outside the original dedup batch")
	}
}
