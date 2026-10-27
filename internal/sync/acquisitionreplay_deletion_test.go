package sync

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
)

type acquisitionDeletionHistoryAPI struct {
	*gmail.MockAPI
	recovery bool
	profiles int
}

func (a *acquisitionDeletionHistoryAPI) GetProfile(ctx context.Context) (*gmail.Profile, error) {
	p, err := a.MockAPI.GetProfile(ctx)
	if err != nil || !a.recovery {
		return p, err
	}
	a.profiles++
	copy := *p
	copy.HistoryID = 15000
	if a.profiles > 1 {
		copy.HistoryID = 20000
	}
	return &copy, nil
}

func (a *acquisitionDeletionHistoryAPI) ListHistory(ctx context.Context, h uint64, token string) (*gmail.HistoryResponse, error) {
	r, err := a.MockAPI.ListHistory(ctx, h, token)
	if err != nil {
		return nil, err
	}
	if token == "" {
		r.NextPageToken = "second-page"
	} else {
		r.History = nil
	}
	return r, nil
}

func TestAcquisitionReplayDeletionFailureKeepsHistoryCursor(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, archived := range []bool{false, true} {
			t.Run(fmt.Sprintf("recovery=%t/archived=%t", recovery, archived), func(t *testing.T) {
				e := acquisitionReplayEnv(t)
				s := acquisitionReplayDebt(t, e, "gone")
				if archived {
					delete(e.Mock.GetMessageError, "gone")
					_, err := e.Syncer.Incremental(t.Context(), s)
					require.NoError(t, err)
				}
				e.Mock.MessagePages = [][]string{{"gone"}}
				e.SetHistory(20000, historyDeleted("gone"))
				api := &acquisitionDeletionHistoryAPI{MockAPI: e.Mock, recovery: recovery}
				e.Syncer = New(api, e.Store, nil)
				// Only the lifecycle append fails. delete_error must remain writable
				// after the deletion transaction rolls back, but cannot replace replay.
				acquisitionReplayInstallFault(t, e, "source_disposition_fault", "sync_run_items", "NEW.error_kind='acquisition_source_deleted'")
				var err error
				if recovery {
					_, err = e.Syncer.RecoverExpiredHistory(t.Context(), s)
				} else {
					_, err = e.Syncer.Incremental(t.Context(), s)
				}
				assert.Error(t, err)
				wantCursor := "1001"
				if recovery {
					// A complete inventory still contained gone at H; its deletion
					// occurred afterward. Catch-up must retain H, not publish 20000.
					wantCursor = "15000"
				}
				assert.Equal(t, wantCursor, acquisitionReplaySource(t, e).SyncCursor.String)
				run, err := e.Store.GetLatestSync(s.ID)
				require.NoError(t, err)
				assert.Equal(t, store.SyncStatusFailed, run.Status)
				assert.Empty(t, run.CursorBefore.String, "must not checkpoint beyond the failed deletion page")
				assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "gone", store.AcquisitionReplaySourceDeleted))
				assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "gone", syncItemKindDeleteError))
				var tombstones int
				require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages WHERE deleted_from_source_at IS NOT NULL").Scan(&tombstones))
				assert.Zero(t, tombstones, "the message update and lifecycle append roll back together")

				acquisitionReplayReopen(t, e)
				assert.Equal(t, wantCursor, acquisitionReplaySource(t, e).SyncCursor.String)
				acquisitionReplayDropFault(t, e, "source_disposition_fault", []string{"sync_run_items"})
				e.Mock.HistoryCalls = nil
				_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
				require.NoError(t, err)
				wantStart := uint64(1001)
				if recovery {
					wantStart = 15000
				}
				assert.Equal(t, []uint64{wantStart, wantStart}, e.Mock.HistoryCalls)
				assert.Equal(t, "20000", acquisitionReplaySource(t, e).SyncCursor.String)
				assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "gone", store.AcquisitionReplaySourceDeleted))
				acquisitionReplayReopen(t, e)
				pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
				require.NoError(t, err)
				assert.Empty(t, pending)
				if archived {
					acquisitionReplayRaw(t, e, "gone")
				} else {
					acquisitionReplayAbsent(t, e, "gone")
				}
				e.SetHistory(20000)
				calls := len(e.Mock.GetMessageCalls)
				_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
				require.NoError(t, err)
				assert.Len(t, e.Mock.GetMessageCalls, calls)
			})
		}
	}
}

func TestAcquisitionReplayOrdinaryDeletionWithoutDebt(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprintf("update-failure=%t", fault), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := e.CreateSourceWithHistory(t, "1000")
			e.Mock.AddMessage("ordinary", testMIME(), []string{"INBOX"})
			e.SetHistory(1001, historyAdded("ordinary"))
			_, err := e.Syncer.Incremental(t.Context(), s)
			require.NoError(t, err)
			if fault {
				// A tombstone UPDATE failure has the same lack of durable replay
				// as a disposition failure, even when no acquisition debt exists.
				if e.Store.IsPostgreSQL() {
					_, err = e.Store.DB().Exec(`CREATE FUNCTION deletion_update_fault_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic tombstone update failure'; END $$`)
					require.NoError(t, err)
					_, err = e.Store.DB().Exec(`CREATE TRIGGER deletion_update_fault BEFORE UPDATE OF deleted_from_source_at ON messages FOR EACH ROW EXECUTE FUNCTION deletion_update_fault_fn()`)
				} else {
					_, err = e.Store.DB().Exec(`CREATE TRIGGER deletion_update_fault BEFORE UPDATE OF deleted_from_source_at ON messages BEGIN SELECT RAISE(ABORT,'synthetic tombstone update failure'); END`)
				}
				require.NoError(t, err)
			}
			e.SetHistory(2000, historyDeleted("ordinary"), historyDeleted("unknown"))
			_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			if fault {
				require.ErrorContains(t, err, "synthetic tombstone update failure")
				acquisitionReplayReopen(t, e)
				assert.Equal(t, "1001", acquisitionReplaySource(t, e).SyncCursor.String)
				var tombstones int
				require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages WHERE deleted_from_source_at IS NOT NULL").Scan(&tombstones))
				assert.Zero(t, tombstones)
				acquisitionReplayDropFault(t, e, "deletion_update_fault", []string{"messages"})
				_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			}
			require.NoError(t, err)
			acquisitionReplayReopen(t, e)
			assert.Equal(t, "2000", acquisitionReplaySource(t, e).SyncCursor.String)
			var tombstones int
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages WHERE deleted_from_source_at IS NOT NULL").Scan(&tombstones))
			assert.Equal(t, 1, tombstones)
			acquisitionReplayRaw(t, e, "ordinary")
			assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "ordinary", store.AcquisitionReplaySourceDeleted), "ordinary acquisitions without debt need no lifecycle append")
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Empty(t, pending)
		})
	}
}

func TestAcquisitionReplayFailedDeletionThenExpiredHistory(t *testing.T) {
	for _, archived := range []bool{false, true} {
		t.Run(fmt.Sprintf("archived=%t", archived), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "gone")
			if archived {
				delete(e.Mock.GetMessageError, "gone")
				_, err := e.Syncer.Incremental(t.Context(), s)
				require.NoError(t, err)
			}
			acquisitionReplayInstallFault(t, e, "expired_deletion_fault", "sync_run_items", "NEW.error_kind='acquisition_source_deleted'")
			e.SetHistory(2000, historyDeleted("gone"))
			_, err := e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.Error(t, err)
			acquisitionReplayReopen(t, e)
			assert.Equal(t, "1001", acquisitionReplaySource(t, e).SyncCursor.String)
			acquisitionReplayDropFault(t, e, "expired_deletion_fault", []string{"sync_run_items"})

			// The retained cursor can expire before retry. A complete presence
			// inventory must then confirm absence, including never-archived debt.
			delete(e.Mock.Messages, "gone")
			e.Mock.MessagePages = [][]string{{}}
			e.Mock.HistoryError = &gmail.NotFoundError{Path: "synthetic expired history"}
			_, err = e.Syncer.IncrementalWithHistoryRecovery(t.Context(), acquisitionReplaySource(t, e), nil)
			require.NoError(t, err)
			acquisitionReplayReopen(t, e)
			assert.Equal(t, "2000", acquisitionReplaySource(t, e).SyncCursor.String)
			assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "gone", store.AcquisitionReplaySourceDeleted))
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Empty(t, pending)
			if archived {
				acquisitionReplayRaw(t, e, "gone")
			} else {
				acquisitionReplayAbsent(t, e, "gone")
			}
			e.Mock.HistoryError = nil
			e.SetHistory(2000)
			calls := len(e.Mock.GetMessageCalls)
			_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
		})
	}
}
