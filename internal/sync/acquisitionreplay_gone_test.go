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
			must := require.New(t)
			checks := assert.New(t)
			e := acquisitionReplayEnv(t)
			source := e.CreateSourceWithHistory(t, "1000")
			record := historyAdded("gone")
			if path == "label-added" {
				record = historyLabelAdded("gone", "INBOX")
			}
			e.SetHistory(1001, record)
			e.Mock.GetMessageError["gone"] = gmail.ErrMessageGone

			summary, err := e.Syncer.Incremental(e.Context, source)
			must.NoError(err)
			checks.Zero(summary.Errors)
			checks.Zero(summary.MessagesAdded)
			run, err := e.Store.GetLatestSync(source.ID)
			must.NoError(err)
			items, err := e.Store.ListSyncRunItems(run.ID, store.SyncRunItemStatusSkipped, 100)
			must.NoError(err)
			must.Len(items, 1)
			checks.Equal(syncItemKindMessageGone, items[0].ErrorKind)
			checks.Equal("gone", items[0].SourceMessageID)
			checks.Equal(gmail.ErrMessageGone.Error(), items[0].ErrorMessage)

			// The initial fetch must persist the terminal outcome. Reopen before
			// an idle incremental so only durable state decides whether to fetch.
			calls := len(e.Mock.GetMessageCalls)
			must.Equal(1, calls)
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(source.ID)
			must.NoError(err)
			checks.Empty(pending)
			e.SetHistory(1001)
			_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
			must.NoError(err)
			checks.Len(e.Mock.GetMessageCalls, calls)
			acquisitionReplayAbsent(t, e, "gone")
			var raws int
			must.NoError(e.Store.DB().QueryRow("SELECT count(*) FROM message_raw").Scan(&raws))
			checks.Zero(raws)
			persisted, err := e.Store.ListSyncRunItems(run.ID, store.SyncRunItemStatusSkipped, 100)
			must.NoError(err)
			checks.Equal(items, persisted, "the initial diagnostic remains unchanged")
			checks.Equal("1001", acquisitionReplaySource(t, e).SyncCursor.String)
		})
	}
}
