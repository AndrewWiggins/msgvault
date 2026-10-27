package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
)

func TestAcquisitionReplayInitialGoneSurvivesReopen(t *testing.T) {
	for _, path := range []string{"message-added", "label-added"} {
		t.Run(path, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			source := e.CreateSourceWithHistory(t, "1000")
			record := historyAdded("gone")
			if path == "label-added" {
				record = historyLabelAdded("gone", "INBOX")
			}
			e.SetHistory(1001, record)
			e.Mock.GetMessageError["gone"] = gmail.ErrMessageGone

			summary, err := e.Syncer.Incremental(e.Context, source)
			require.NoError(t, err)
			assert.Zero(t, summary.Errors)
			assert.Zero(t, summary.MessagesAdded)
			run, err := e.Store.GetLatestSync(source.ID)
			require.NoError(t, err)
			items, err := e.Store.ListSyncRunItems(run.ID, store.SyncRunItemStatusSkipped, 100)
			require.NoError(t, err)
			require.Len(t, items, 1)
			assert.Equal(t, syncItemKindMessageGone, items[0].ErrorKind)
			assert.Equal(t, "gone", items[0].SourceMessageID)
			assert.Equal(t, gmail.ErrMessageGone.Error(), items[0].ErrorMessage)

			// The initial fetch must persist the terminal outcome. Reopen before
			// an idle incremental so only durable state decides whether to fetch.
			calls := len(e.Mock.GetMessageCalls)
			require.Equal(t, 1, calls)
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(source.ID)
			require.NoError(t, err)
			assert.Empty(t, pending)
			e.SetHistory(1001)
			_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
			require.NoError(t, err)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
			acquisitionReplayAbsent(t, e, "gone")
			var raws int
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM message_raw").Scan(&raws))
			assert.Zero(t, raws)
			persisted, err := e.Store.ListSyncRunItems(run.ID, store.SyncRunItemStatusSkipped, 100)
			require.NoError(t, err)
			assert.Equal(t, items, persisted, "the initial diagnostic remains unchanged")
			assert.Equal(t, "1001", acquisitionReplaySource(t, e).SyncCursor.String)
		})
	}
}
