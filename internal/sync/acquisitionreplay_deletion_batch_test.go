package sync

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

// More than two lookup chunks exercise rollback after earlier journal writes,
// repeated native IDs, and identical native IDs owned by different sources.
func TestAcquisitionReplayDeletionBatchesAtomic(t *testing.T) {
	for _, mode := range []string{"provider", "gc"} {
		t.Run(mode, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			if mode == "gc" && e.Store.IsPostgreSQL() {
				t.Skip("archive GC is SQLite-only")
			}
			const count = 1001
			ids := make([]string, count)
			for i := range ids {
				ids[i] = fmt.Sprintf("batch-%04d", i)
			}
			sources := make([]*store.Source, 2)
			for sourceIndex := range sources {
				source, err := e.Store.GetOrCreateSource("gmail", fmt.Sprintf("batch-%d@example.test", sourceIndex))
				require.NoError(t, err)
				sources[sourceIndex] = source
				conversationID, err := e.Store.EnsureConversation(source.ID, "synthetic-batch", "Synthetic batch")
				require.NoError(t, err)
				runID, err := e.Store.StartSync(source.ID, "incremental")
				require.NoError(t, err)
				for i, id := range ids {
					_, err = e.Store.PersistMessage(&store.MessagePersistData{
						Message: &store.Message{SourceID: source.ID, SourceMessageID: id, ConversationID: conversationID, MessageType: "email"},
						RawMIME: []byte("synthetic raw"),
					})
					require.NoError(t, err)
					if i%2 == 0 {
						require.NoError(t, e.Store.RecordSyncRunItem(store.SyncRunItem{
							SyncRunID: runID, SourceMessageID: id, Phase: "fetch", Status: "error", ErrorKind: "fetch_error",
						}))
					}
				}
				// Local intent must remain authoritative after the provider deletion
				// recorded by this batch.
				require.NoError(t, e.Store.RecordSyncRunItem(store.SyncRunItem{
					SyncRunID: runID, SourceMessageID: ids[0], Phase: "fetch", Status: "skipped", ErrorKind: store.AcquisitionReplayLocalDeletion,
				}))
			}
			// Unknown IDs and duplicate IDs straddling chunks must add no extra events.
			deletionIDs := append(append([]string(nil), ids...), ids[0], ids[count-1], "untracked")
			if mode == "gc" {
				_, err := e.Store.DB().Exec("UPDATE messages SET deleted_from_source_at=CURRENT_TIMESTAMP")
				require.NoError(t, err)
			}
			execute := func() error {
				if mode == "provider" {
					return e.Store.MarkMessagesDeletedBatch(sources[0].ID, deletionIDs)
				}
				plan, err := e.Store.PlanGCContext(t.Context())
				if err != nil {
					return err
				}
				_, err = e.Store.ExecuteGCContext(t.Context(), plan)
				return err
			}
			acquisitionReplayInstallFault(t, e, "batch_disposition_fault", "sync_run_items",
				"NEW.error_kind='acquisition_source_deleted' AND NEW.source_message_id='batch-1000'")
			require.Error(t, execute())
			acquisitionReplayReopen(t, e)
			var rows, raws, dispositions, tombstones int
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages").Scan(&rows))
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM message_raw").Scan(&raws))
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM sync_run_items WHERE error_kind='acquisition_source_deleted'").Scan(&dispositions))
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages WHERE deleted_from_source_at IS NOT NULL").Scan(&tombstones))
			assert.Equal(t, count*2, rows)
			assert.Equal(t, count*2, raws)
			assert.Zero(t, dispositions, "earlier chunks roll back with the failing chunk")
			if mode == "provider" {
				assert.Zero(t, tombstones)
			} else {
				assert.Equal(t, count*2, tombstones)
			}
			acquisitionReplayDropFault(t, e, "batch_disposition_fault", []string{"sync_run_items"})
			require.NoError(t, execute())
			// Repeated observations do not duplicate dispositions, including repeated
			// IDs split across the SQL lookup chunks.
			require.NoError(t, execute())
			acquisitionReplayReopen(t, e)
			for sourceIndex, source := range sources {
				require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind(`SELECT count(*) FROM sync_run_items item
     JOIN sync_runs run ON run.id=item.sync_run_id WHERE run.source_id=? AND item.error_kind='acquisition_source_deleted'`), source.ID).Scan(&dispositions))
				if sourceIndex == 0 || mode == "gc" {
					assert.Equal(t, (count+1)/2, dispositions)
				} else {
					assert.Zero(t, dispositions)
				}
				pending, err := e.Store.PendingAcquisitionReplayIDs(source.ID)
				require.NoError(t, err)
				assert.NotContains(t, pending, ids[0], "local intent remains terminal")
				if sourceIndex == 0 || mode == "gc" {
					assert.Empty(t, pending)
				} else {
					assert.Len(t, pending, (count-1)/2)
				}
			}
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages").Scan(&rows))
			if mode == "gc" {
				assert.Zero(t, rows)
			} else {
				assert.Equal(t, count*2, rows)
			}
		})
	}
}
