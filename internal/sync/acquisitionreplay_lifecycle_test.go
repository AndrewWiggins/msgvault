package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
)

// SQL faults target the real disposition/ingestion transactions on both backends.
func acquisitionReplayInstallFault(t *testing.T, e *TestEnv, name, table, condition string) {
	t.Helper()
	if e.Store.IsPostgreSQL() {
		_, err := e.Store.DB().Exec("CREATE FUNCTION " + name + "_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic lifecycle journal failure'; END $$")
		require.NoError(t, err)
		when := ""
		if condition != "" {
			when = " WHEN (" + condition + ")"
		}
		_, err = e.Store.DB().Exec("CREATE TRIGGER " + name + " BEFORE INSERT ON " + table + " FOR EACH ROW" + when + " EXECUTE FUNCTION " + name + "_fn()")
		require.NoError(t, err)
	} else {
		when := ""
		if condition != "" {
			when = " WHEN " + condition
		}
		_, err := e.Store.DB().Exec("CREATE TRIGGER " + name + " BEFORE INSERT ON " + table + when + " BEGIN SELECT RAISE(ABORT,'synthetic lifecycle journal failure'); END")
		require.NoError(t, err)
	}
}

func acquisitionReplayDropFault(t *testing.T, e *TestEnv, name string, tables []string) {
	t.Helper()
	if e.Store.IsPostgreSQL() {
		for _, table := range tables {
			_, err := e.Store.DB().Exec("DROP TRIGGER IF EXISTS " + name + " ON " + table)
			require.NoError(t, err)
		}
		_, err := e.Store.DB().Exec("DROP FUNCTION IF EXISTS " + name + "_fn()")
		require.NoError(t, err)
	} else {
		_, err := e.Store.DB().Exec("DROP TRIGGER IF EXISTS " + name)
		require.NoError(t, err)
	}
}

func acquisitionReplayDebt(t *testing.T, e *TestEnv, id string) *store.Source {
	t.Helper()
	s := e.CreateSourceWithHistory(t, "1000")
	e.Mock.AddMessage(id, testMIME(), []string{"INBOX"})
	e.Mock.GetMessageError[id] = errors.New("synthetic acquisition unavailable")
	e.SetHistory(1001, historyAdded(id))
	result, err := e.Syncer.Incremental(t.Context(), s)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Errors)
	e.SetHistory(1001)
	acquisitionReplayReopen(t, e)
	return acquisitionReplaySource(t, e)
}

func acquisitionReplayDispositions(t *testing.T, e *TestEnv, sourceID int64, id, kind string) int {
	t.Helper()
	var count int
	require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind(`SELECT count(*) FROM sync_run_items item JOIN sync_runs run ON run.id=item.sync_run_id WHERE run.source_id=? AND item.source_message_id=? AND item.error_kind=?`), sourceID, id, kind).Scan(&count))
	return count
}

func acquisitionReplayPurge(t *testing.T, e *TestEnv, mode string) (int64, error) {
	t.Helper()
	switch mode {
	case "all":
		n, _, err := e.Store.DeleteAllDeduped()
		return n, err
	case "all-context":
		n, _, err := e.Store.DeleteAllDedupedContext(t.Context())
		return n, err
	case "batch":
		return e.Store.DeleteDedupedBatch("synthetic-batch")
	case "batches":
		return e.Store.DeleteDedupedBatchesContext(t.Context(), []string{"synthetic-batch", "synthetic-batch", "other-batch"})
	default:
		panic("unknown synthetic purge mode")
	}
}

func TestAcquisitionReplayBulkDedupDeletionLifecycle(t *testing.T) {
	for _, mode := range []string{"all", "all-context", "batch", "batches"} {
		for _, fault := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/journal-failure=%t", mode, fault), func(t *testing.T) {
				e := acquisitionReplayEnv(t)
				s := e.CreateSourceWithHistory(t, "1000")
				acquisitionReplayCreateAndResolveDebt(t, e, s, "native-id")
				_, err := e.Store.DB().Exec(e.Store.Rebind("UPDATE messages SET deleted_at=CURRENT_TIMESTAMP, delete_batch_id='synthetic-batch' WHERE source_id=? AND source_message_id='native-id'"), s.ID)
				require.NoError(t, err)
				if fault {
					acquisitionReplayInstallFault(t, e, "local_disposition_fault", "sync_run_items", "NEW.error_kind='acquisition_local_deletion'")
					n, err := acquisitionReplayPurge(t, e, mode)
					require.Error(t, err)
					assert.Zero(t, n)
					acquisitionReplayReopen(t, e)
					var rows int
					require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages WHERE source_message_id='native-id'").Scan(&rows))
					assert.Equal(t, 1, rows, "failed disposition rolls back the physical deletion")
					assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "native-id", store.AcquisitionReplayLocalDeletion))
					acquisitionReplayDropFault(t, e, "local_disposition_fault", []string{"sync_run_items"})
				}
				n, err := acquisitionReplayPurge(t, e, mode)
				require.NoError(t, err)
				assert.EqualValues(t, 1, n)
				acquisitionReplayReopen(t, e)
				calls := len(e.Mock.GetMessageCalls)
				_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
				require.NoError(t, err)
				assert.Len(t, e.Mock.GetMessageCalls, calls, "explicitly destroyed occurrence must not be downloaded again")
				acquisitionReplayAbsent(t, e, "native-id")
				assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "native-id", store.AcquisitionReplayLocalDeletion))
				requireAcquisitionErrorHistory(t, e, "native-id", 1)
				n, err = acquisitionReplayPurge(t, e, mode)
				require.NoError(t, err)
				assert.Zero(t, n)
			})
		}
	}
}

type acquisitionInventoryAPI struct {
	*gmail.MockAPI
	snapshotCalls int
	mode          string
	beforeHistory func()
}

func (a *acquisitionInventoryAPI) ListCompleteMessageSnapshot(ctx context.Context, token string) (*gmail.MessageListResponse, error) {
	a.snapshotCalls++
	if a.mode == "cancel" {
		return nil, context.Canceled
	}
	if a.mode == "cycle" {
		return &gmail.MessageListResponse{NextPageToken: "repeated"}, nil
	}
	if a.mode == "page-error" {
		if token == "" {
			return &gmail.MessageListResponse{Messages: []gmail.MessageID{{ID: "present"}}, NextPageToken: "page-2"}, nil
		}
		return nil, errors.New("synthetic inventory page error")
	}
	// Spam/Trash occur in the authoritative inventory although the pre-B
	// acquisition list can omit them. Presence alone must keep their debt.
	if a.mode == "special" {
		return &gmail.MessageListResponse{Messages: []gmail.MessageID{{ID: "lost"}, {ID: "present"}}}, nil
	}
	return a.MockAPI.ListCompleteMessageSnapshot(ctx, token)
}

func (a *acquisitionInventoryAPI) ListHistory(ctx context.Context, h uint64, token string) (*gmail.HistoryResponse, error) {
	if a.beforeHistory != nil {
		f := a.beforeHistory
		a.beforeHistory = nil
		f()
	}
	return a.MockAPI.ListHistory(ctx, h, token)
}

func TestAcquisitionReplayCompleteInventoryRetiresMissingDebt(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "lost")
			// Give reconciliation a real tombstone to roll back if journaling fails.
			e.Mock.AddMessage("archived-then-gone", testMIME(), []string{"INBOX"})
			e.Mock.MessagePages = [][]string{{"archived-then-gone"}}
			_, err := e.Syncer.Full(t.Context(), testEmail)
			require.NoError(t, err)
			delete(e.Mock.Messages, "archived-then-gone")
			delete(e.Mock.Messages, "lost")
			e.Mock.AddMessage("present", testMIME(), []string{"INBOX"})
			e.Mock.MessagePages = [][]string{{"present"}}
			e.Mock.Profile.HistoryID = 15000
			e.Mock.HistoryID = 15000
			if fault {
				acquisitionReplayInstallFault(t, e, "inventory_disposition_fault", "sync_run_items", "NEW.error_kind='acquisition_source_deleted'")
			}
			_, err = e.Syncer.RecoverExpiredHistory(t.Context(), s)
			if fault {
				require.Error(t, err)
				assert.Equal(t, "1001", acquisitionReplaySource(t, e).SyncCursor.String)
				assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "lost", store.AcquisitionReplaySourceDeleted))
				assertDeletedFromSource(t, e.Store, "archived-then-gone", false)
				acquisitionReplayReopen(t, e)
				acquisitionReplayDropFault(t, e, "inventory_disposition_fault", []string{"sync_run_items"})
				_, err = e.Syncer.RecoverExpiredHistory(t.Context(), acquisitionReplaySource(t, e))
			}
			require.NoError(t, err)
			assertDeletedFromSource(t, e.Store, "archived-then-gone", true)
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Empty(t, pending, "absence is acknowledged even without an archive row")
			calls := len(e.Mock.GetMessageCalls)
			_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
			assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "lost", store.AcquisitionReplaySourceDeleted))
			_, err = e.Syncer.RecoverExpiredHistory(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "lost", store.AcquisitionReplaySourceDeleted), "inventory acknowledgement is idempotent")
			requireAcquisitionErrorHistory(t, e, "lost", 1)
		})
	}
}

func TestAcquisitionReplayInvalidInventoryKeepsDebt(t *testing.T) {
	for _, mode := range []string{"cancel", "page-error", "cycle", "query", "limit", "ordinary-filtered-full", "special", "still-present"} {
		t.Run(mode, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "lost")
			e.Mock.AddMessage("present", testMIME(), []string{"INBOX"})
			e.Mock.MessagePages = [][]string{{"present"}}
			e.Mock.Profile.HistoryID = 15000
			api := &acquisitionInventoryAPI{MockAPI: e.Mock, mode: mode}
			opts := DefaultOptions()
			if mode == "query" || mode == "ordinary-filtered-full" {
				opts.Query = "subject:present"
			}
			if mode == "limit" {
				opts.Limit = 1
			}
			if mode == "still-present" {
				e.Mock.MessagePages = [][]string{{"present"}, {"lost"}}
			}
			e.Syncer = New(api, e.Store, opts)
			var err error
			if mode == "ordinary-filtered-full" {
				_, err = e.Syncer.Full(t.Context(), testEmail)
			} else {
				_, err = e.Syncer.RecoverExpiredHistory(t.Context(), s)
			}
			if mode == "special" || mode == "still-present" || mode == "ordinary-filtered-full" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Contains(t, pending, "lost")
			assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "lost", store.AcquisitionReplaySourceDeleted))
			delete(e.Mock.GetMessageError, "lost")
			e.SetHistory(15000)
			e.Syncer = New(e.Mock, e.Store, nil)
			_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			acquisitionReplayRaw(t, e, "lost")
		})
	}
}

func TestAcquisitionReplayLifecycleTransitionMatrix(t *testing.T) {
	for _, transition := range []string{"unresolved", "acquired", "accidental-raw-loss", "local-deletion", "dedup-destruction", "history-deleted", "inventory-deleted", "ambiguous-404", "incomplete-inventory"} {
		t.Run(transition, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "native-id")
			acquired := transition == "acquired" || transition == "accidental-raw-loss" || transition == "local-deletion" || transition == "dedup-destruction"
			if acquired {
				delete(e.Mock.GetMessageError, "native-id")
				_, err := e.Syncer.Incremental(t.Context(), s)
				require.NoError(t, err)
				acquisitionReplayRaw(t, e, "native-id")
			}
			switch transition {
			case "accidental-raw-loss":
				_, err := e.Store.DB().Exec(e.Store.Rebind("DELETE FROM message_raw WHERE message_id=(SELECT id FROM messages WHERE source_id=? AND source_message_id='native-id')"), s.ID)
				require.NoError(t, err)
			case "local-deletion", "dedup-destruction":
				_, err := e.Store.DB().Exec("UPDATE messages SET deleted_at=CURRENT_TIMESTAMP, delete_batch_id='synthetic-batch'")
				require.NoError(t, err)
				mode := "batch"
				if transition == "dedup-destruction" {
					mode = "all"
				}
				_, err = acquisitionReplayPurge(t, e, mode)
				require.NoError(t, err)
			case "history-deleted":
				e.SetHistory(2000, historyDeleted("native-id"))
				_, err := e.Syncer.Incremental(t.Context(), s)
				require.NoError(t, err)
				e.SetHistory(2000)
			case "inventory-deleted":
				delete(e.Mock.Messages, "native-id")
				e.Mock.MessagePages = [][]string{{}}
				e.SetHistory(2000)
				_, err := e.Syncer.RecoverExpiredHistory(t.Context(), s)
				require.NoError(t, err)
			case "ambiguous-404":
				e.Mock.GetMessageError["native-id"] = &gmail.NotFoundError{Path: "/messages/native-id"}
				_, err := e.Syncer.Incremental(t.Context(), s)
				require.NoError(t, err)
			case "incomplete-inventory":
				e.Syncer = New(&acquisitionInventoryAPI{MockAPI: e.Mock, mode: "page-error"}, e.Store, nil)
				_, err := e.Syncer.RecoverExpiredHistory(t.Context(), s)
				require.Error(t, err)
			}
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			wantPending := transition == "unresolved" || transition == "accidental-raw-loss" || transition == "ambiguous-404" || transition == "incomplete-inventory"
			if wantPending {
				assert.Contains(t, pending, "native-id")
			} else {
				assert.Empty(t, pending)
			}
			var diagnostics int
			require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM sync_run_items WHERE source_message_id='native-id' AND status='error'").Scan(&diagnostics))
			assert.GreaterOrEqual(t, diagnostics, 1)
			// With the fault repaired, only live debt should fetch on an idle sync.
			delete(e.Mock.GetMessageError, "native-id")
			e.Mock.HistoryRecords = nil
			e.Syncer = New(e.Mock, e.Store, nil)
			calls := len(e.Mock.GetMessageCalls)
			_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			if wantPending {
				assert.Len(t, e.Mock.GetMessageCalls, calls+1)
				acquisitionReplayRaw(t, e, "native-id")
			} else {
				assert.Len(t, e.Mock.GetMessageCalls, calls)
			}
			acquisitionReplayReopen(t, e)
			pending, err = e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Empty(t, pending)
		})
	}
}

func TestAcquisitionReplayInventoryHistoryHandoff(t *testing.T) {
	for _, outcome := range []string{"acquired", "fetch-error", "journal-error", "history-error"} {
		t.Run(outcome, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "lost")
			e.Mock.MessagePages = [][]string{{}}
			api := &acquisitionInventoryAPI{MockAPI: e.Mock, beforeHistory: func() {
				e.SetHistory(20000, historyAdded("lost"))
				delete(e.Mock.GetMessageError, "lost")
				if outcome == "fetch-error" || outcome == "journal-error" {
					e.Mock.GetMessageError["lost"] = errors.New("synthetic catch-up acquisition error")
				}
				if outcome == "journal-error" {
					acquisitionReplayInstallFault(t, e, "catchup_journal_fault", "sync_run_items", "NEW.status='error'")
				}
				if outcome == "history-error" {
					e.Mock.HistoryError = errors.New("synthetic catch-up history error")
				}
			}}
			// Profile H=15000 precedes inventory; the catch-up profile is 20000.
			e.Mock.HistoryCalls = nil
			seq := &acquisitionHandoffAPI{acquisitionInventoryAPI: api}
			e.Syncer = New(seq, e.Store, nil)
			_, err := e.Syncer.RecoverExpiredHistory(t.Context(), s)
			if outcome == "journal-error" || outcome == "history-error" {
				require.Error(t, err)
				assert.NotEqual(t, "20000", acquisitionReplaySource(t, e).SyncCursor.String, "no final cursor after failed catch-up")
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []uint64{15000}, e.Mock.HistoryCalls, "history must bridge all changes since before inventory")
			if outcome == "journal-error" {
				acquisitionReplayDropFault(t, e, "catchup_journal_fault", []string{"sync_run_items"})
			}
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			if outcome == "fetch-error" {
				assert.Contains(t, pending, "lost")
			} else {
				assert.Empty(t, pending)
			}
			assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "lost", store.AcquisitionReplaySourceDeleted))
			delete(e.Mock.GetMessageError, "lost")
			e.Mock.HistoryError = nil
			e.SetHistory(20000, historyAdded("lost"))
			e.Syncer = New(e.Mock, e.Store, nil)
			_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			acquisitionReplayRaw(t, e, "lost")
		})
	}
}

type acquisitionHandoffAPI struct {
	*acquisitionInventoryAPI
	profiles int
}

func (a *acquisitionHandoffAPI) GetProfile(ctx context.Context) (*gmail.Profile, error) {
	a.profiles++
	p, err := a.MockAPI.GetProfile(ctx)
	if err != nil {
		return nil, err
	}
	copy := *p
	if a.profiles == 1 {
		copy.HistoryID = 15000
	} else {
		copy.HistoryID = 20000
	}
	return &copy, nil
}

func TestAcquisitionReplayInventoryKeepsNativeSourceIdentity(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := acquisitionReplayDebt(t, e, "shared-native-id")
	other, err := e.Store.GetOrCreateSource("gmail", "other@example.com")
	require.NoError(t, err)
	recordSyncRunItems(t, e, other.ID, store.SyncStatusCompleted, store.SyncRunItem{SourceMessageID: "shared-native-id", Phase: "fetch", Status: "error", ErrorKind: "fetch_error"})
	delete(e.Mock.Messages, "shared-native-id")
	e.Mock.MessagePages = [][]string{{}}
	e.SetHistory(15000)
	_, err = e.Syncer.RecoverExpiredHistory(t.Context(), s)
	require.NoError(t, err)
	acquisitionReplayReopen(t, e)
	pending, err := e.Store.PendingAcquisitionReplayIDs(other.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"shared-native-id"}, pending)
	assert.Zero(t, acquisitionReplayDispositions(t, e, other.ID, "shared-native-id", store.AcquisitionReplaySourceDeleted))
	assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "shared-native-id", store.AcquisitionReplaySourceDeleted))
}

func TestAcquisitionReplayDedupTransactionAndSourceIdentity(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprint(all), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := e.CreateSourceWithHistory(t, "1000")
			for _, id := range []string{"first-native-id", "second-native-id"} {
				acquisitionReplayCreateAndResolveDebt(t, e, s, id)
				_, err := e.Store.DB().Exec(e.Store.Rebind("UPDATE messages SET deleted_at=CURRENT_TIMESTAMP,delete_batch_id=? WHERE source_id=? AND source_message_id=?"), id, s.ID, id)
				require.NoError(t, err)
			}
			other, err := e.Store.GetOrCreateSource("gmail", "other@example.com")
			require.NoError(t, err)
			conversation, err := e.Store.EnsureConversation(other.ID, "control-thread", "Synthetic control")
			require.NoError(t, err)
			_, err = e.Store.UpsertMessage(&store.Message{SourceID: other.ID, SourceMessageID: "first-native-id", ConversationID: conversation, MessageType: "email"})
			require.NoError(t, err)
			recordSyncRunItems(t, e, other.ID, store.SyncStatusCompleted, store.SyncRunItem{SourceMessageID: "first-native-id", Phase: "fetch", Status: "error", ErrorKind: "fetch_error"})
			purge := func() (int64, error) {
				if all {
					n, _, err := e.Store.DeleteAllDedupedContext(t.Context())
					return n, err
				}
				return e.Store.DeleteDedupedBatchesContext(t.Context(), []string{"first-native-id", "second-native-id"})
			}
			acquisitionReplayInstallFault(t, e, "second_local_fault", "sync_run_items", "NEW.error_kind='acquisition_local_deletion' AND NEW.source_message_id='second-native-id'")
			n, err := purge()
			require.Error(t, err)
			assert.Zero(t, n)
			acquisitionReplayReopen(t, e)
			for _, id := range []string{"first-native-id", "second-native-id"} {
				assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, id, store.AcquisitionReplayLocalDeletion))
				var rows int
				require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind("SELECT count(*) FROM messages WHERE source_id=? AND source_message_id=?"), s.ID, id).Scan(&rows))
				assert.Equal(t, 1, rows, "both deletion and earlier dispositions roll back")
			}
			acquisitionReplayDropFault(t, e, "second_local_fault", []string{"sync_run_items"})
			n, err = purge()
			require.NoError(t, err)
			assert.EqualValues(t, 2, n)
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(other.ID)
			require.NoError(t, err)
			assert.Equal(t, []string{"first-native-id"}, pending, "native ID is scoped to its source")
			assert.Zero(t, acquisitionReplayDispositions(t, e, other.ID, "first-native-id", store.AcquisitionReplayLocalDeletion))
			for _, id := range []string{"first-native-id", "second-native-id"} {
				assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, id, store.AcquisitionReplayLocalDeletion))
				var diagnostics int
				require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind("SELECT count(*) FROM sync_run_items item JOIN sync_runs run ON run.id=item.sync_run_id WHERE run.source_id=? AND item.source_message_id=? AND item.status='error'"), s.ID, id).Scan(&diagnostics))
				assert.Equal(t, 1, diagnostics)
			}
		})
	}
}
