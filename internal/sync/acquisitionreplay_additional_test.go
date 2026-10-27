package sync

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
)

func TestAcquisitionReplayRepeatedFailuresAndBoundedFull(t *testing.T) {
	for _, origin := range []string{"incremental", "full"} {
		for _, scope := range []string{"query", "limit"} {
			t.Run(origin+"/"+scope, func(t *testing.T) {
				e := acquisitionReplayEnv(t)
				s := e.CreateSourceWithHistory(t, "1000")
				e.Mock.AddMessage("lost", testMIME(), []string{"INBOX"})
				e.Mock.MessagePages = [][]string{{"lost"}}
				e.SetHistory(1001, historyAdded("lost"))
				acquisitionReplayTrigger(t, e, "message_raw")
				var first *gmail.SyncSummary
				var err error
				if origin == "full" {
					first, err = e.Syncer.Full(e.Context, testEmail)
				} else {
					first, err = e.Syncer.Incremental(e.Context, s)
				}
				require.NoError(t, err)
				require.Equal(t, int64(1), first.Errors)
				e.SetHistory(1001)
				for i := 0; i < 2; i++ {
					acquisitionReplayReopen(t, e)
					retry, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
					require.NoError(t, err)
					require.Equal(t, int64(1), retry.Errors)
					acquisitionReplayAbsent(t, e, "lost")
				}
				acquisitionReplayDrop(t, e)
				// A different scope sees only an unrelated message; it must not clear debt.
				e.Mock.AddMessage("other", testMIME(), []string{"INBOX"})
				e.Mock.MessagePages = [][]string{{"other"}}
				opts := DefaultOptions()
				if scope == "query" {
					opts.Query = "subject:other"
				} else {
					opts.Limit = 1
				}
				e.Syncer = New(e.Mock, e.Store, opts)
				_, err = e.Syncer.Full(e.Context, testEmail)
				require.NoError(t, err)
				acquisitionReplayReopen(t, e)
				retry, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
				require.NoError(t, err)
				require.Equal(t, int64(1), retry.MessagesAdded)
				acquisitionReplayRaw(t, e, "lost")
				calls := len(e.Mock.GetMessageCalls)
				acquisitionReplayReopen(t, e)
				_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
				require.NoError(t, err)
				require.Len(t, e.Mock.GetMessageCalls, calls)
				var n int
				require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages WHERE source_message_id='lost'").Scan(&n))
				require.Equal(t, 1, n)
				var diagnostics int
				require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM sync_run_items WHERE source_message_id='lost' AND status='error'").Scan(&diagnostics))
				require.Equal(t, 3, diagnostics)
			})
		}
	}
}

func TestAcquisitionReplayJournalFailureDoesNotPublishProgress(t *testing.T) {
	for _, kind := range []string{"incremental", "full"} {
		t.Run(kind, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := e.CreateSourceWithHistory(t, "1000")
			seedMessages(e, 2, 1001, "lost", "other")
			e.Mock.MessagePages = [][]string{{"lost"}, {"other"}}
			e.Mock.GetMessageError["lost"] = errors.New("synthetic download error")
			e.SetHistory(1001, historyAdded("lost"))
			acquisitionReplayInstallFault(t, e, "fail_journal", "sync_run_items", "")
			var err error
			if kind == "incremental" {
				_, err = e.Syncer.Incremental(e.Context, s)
			} else {
				_, err = e.Syncer.Full(e.Context, testEmail)
			}
			require.ErrorContains(t, err, "persist acquisition diagnostics")
			require.Equal(t, "1000", acquisitionReplaySource(t, e).SyncCursor.String)
			var token string
			require.NoError(t, e.Store.DB().QueryRow("SELECT COALESCE(cursor_before,'') FROM sync_runs ORDER BY id DESC LIMIT 1").Scan(&token))
			require.Empty(t, token)
			acquisitionReplayDropFault(t, e, "fail_journal", []string{"sync_run_items"})
			delete(e.Mock.GetMessageError, "lost")
			acquisitionReplayReopen(t, e)
			if kind == "incremental" {
				_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
			} else {
				_, err = e.Syncer.Full(e.Context, testEmail)
			}
			require.NoError(t, err)
			acquisitionReplayRaw(t, e, "lost")
		})
	}
}

func TestAcquisitionReplayDebtLegacyGoneAndSourceIsolation(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	// Legacy untyped, failed run: no inference from counters or cursors needed.
	recordSyncRunItems(t, e, s.ID, store.SyncStatusFailed,
		store.SyncRunItem{SourceMessageID: "lost", Phase: "ingest", Status: "error", ErrorKind: "ingest_error"},
		store.SyncRunItem{SourceMessageID: "gone", Phase: "fetch", Status: "error", ErrorKind: "fetch_error"})
	_, err := e.Store.DB().Exec(e.Store.Rebind("UPDATE sync_runs SET sync_type='' WHERE source_id=?"), s.ID)
	require.NoError(t, err)
	other, err := e.Store.GetOrCreateSource("gmail", "other@example.com")
	require.NoError(t, err)
	recordSyncRunItems(t, e, other.ID, store.SyncStatusCompleted, store.SyncRunItem{SourceMessageID: "foreign", Phase: "fetch", Status: "error", ErrorKind: "fetch_error"})
	e.Mock.AddMessage("lost", testMIME(), []string{"INBOX"})
	e.Mock.GetMessageError["gone"] = gmail.ErrMessageGone
	first, err := e.Syncer.Incremental(e.Context, s)
	require.NoError(t, err)
	require.Equal(t, int64(1), first.MessagesAdded)
	require.Equal(t, int64(1), first.MessagesSkipped)
	acquisitionReplayRaw(t, e, "lost")
	require.NotContains(t, e.Mock.GetMessageCalls, "foreign")
	calls := len(e.Mock.GetMessageCalls)
	acquisitionReplayReopen(t, e)
	_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Len(t, e.Mock.GetMessageCalls, calls)
	// A newer failure must re-open debt even after a durable gone acknowledgment.
	recordIncrementalSyncRunItems(t, e, s.ID, store.SyncStatusCompleted, store.SyncRunItem{SourceMessageID: "gone", Phase: "fetch", Status: "error", ErrorKind: "fetch_error"})
	delete(e.Mock.GetMessageError, "gone")
	e.Mock.AddMessage("gone", testMIME(), nil)
	_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	acquisitionReplayRaw(t, e, "gone")
}

func TestAcquisitionReplayGeneric404RemainsDebt(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	recordIncrementalSyncRunItems(t, e, s.ID, store.SyncStatusCompleted,
		store.SyncRunItem{SourceMessageID: "404-id", Phase: "fetch", Status: "error", ErrorKind: "fetch_error"})
	e.Mock.GetMessageError["404-id"] = &gmail.NotFoundError{Path: "synthetic-404"}
	first, err := e.Syncer.Incremental(e.Context, s)
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Errors)
	acquisitionReplayAbsent(t, e, "404-id")
	second404, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Equal(t, int64(1), second404.Errors)
	acquisitionReplayAbsent(t, e, "404-id")
	// The generic-404 classification must survive a normal database reopen.
	acquisitionReplayReopen(t, e)
	require.Len(t, e.Mock.GetMessageCalls, 2, "both 404 attempts occurred before reopen")
	delete(e.Mock.GetMessageError, "404-id")
	e.Mock.AddMessage("404-id", testMIME(), nil)
	third, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Equal(t, int64(1), third.MessagesAdded)
	acquisitionReplayRaw(t, e, "404-id")
}

func TestAcquisitionReplayResolvedDebtSurvivesSourceDeletionAndGC(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	acquisitionReplayCreateAndResolveDebt(t, e, s, "source-delete")
	require.NoError(t, e.Store.MarkMessageDeleted(s.ID, "source-delete"))
	acquisitionReplayReopen(t, e)
	calls := len(e.Mock.GetMessageCalls)
	_, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Len(t, e.Mock.GetMessageCalls, calls, "source deletion tombstone resolves the replay")
	if e.Store.IsPostgreSQL() {
		return // archive GC is intentionally SQLite-only
	}
	plan, err := e.Store.PlanGCContext(e.Context)
	require.NoError(t, err)
	deleted, err := e.Store.ExecuteGCContext(e.Context, plan)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	acquisitionReplayReopen(t, e)
	calls = len(e.Mock.GetMessageCalls)
	_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Len(t, e.Mock.GetMessageCalls, calls, "Gmail-deleted message must not be reacquired after GC")
	requireAcquisitionErrorHistory(t, e, "source-delete", 1)
}

func TestAcquisitionReplayUnexpectedRawLossRemainsRecoverable(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	acquisitionReplayCreateAndResolveDebt(t, e, s, "raw-loss")
	_, err := e.Store.DB().Exec(e.Store.Rebind(`DELETE FROM message_raw WHERE message_id=(
		SELECT id FROM messages WHERE source_id=? AND source_message_id='raw-loss')`), s.ID)
	require.NoError(t, err)
	acquisitionReplayReopen(t, e)
	_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Len(t, e.Mock.GetMessageCalls, 3, "unexpected RAW loss must trigger repair")
	acquisitionReplayRaw(t, e, "raw-loss")
}

func acquisitionReplayCreateAndResolveDebt(t *testing.T, e *TestEnv, s *store.Source, id string) {
	t.Helper()
	e.Mock.AddMessage(id, testMIME(), []string{"INBOX"})
	e.Mock.MessagePages = [][]string{{id}}
	e.SetHistory(1001, historyAdded(id))
	acquisitionReplayTrigger(t, e, "message_raw")
	first, err := e.Syncer.Incremental(e.Context, s)
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Errors)
	acquisitionReplayDrop(t, e)
	e.SetHistory(1001)
	acquisitionReplayReopen(t, e)
	recovered, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Equal(t, int64(1), recovered.MessagesAdded)
	acquisitionReplayRaw(t, e, id)
	requireAcquisitionErrorHistory(t, e, id, 1)
	var resolved int
	require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind(`
		SELECT count(*) FROM sync_run_items WHERE source_message_id=? AND error_kind=?`),
		id, store.AcquisitionReplayResolved).Scan(&resolved))
	require.Equal(t, 1, resolved, "successful recovery decision is durable")
}

func requireAcquisitionErrorHistory(t *testing.T, e *TestEnv, id string, want int) {
	t.Helper()
	var count int
	require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind(`
		SELECT count(*) FROM sync_run_items WHERE source_message_id=? AND status='error'`), id).Scan(&count))
	require.Equal(t, want, count, "diagnostic history remains append-only")
}

func TestAcquisitionReplayReplayRepairsMessageWithoutRaw(t *testing.T) {
	e := acquisitionReplayEnv(t)
	seedMessages(e, 1, 1000, "lost")
	_, err := e.Syncer.Full(e.Context, testEmail)
	require.NoError(t, err)
	_, err = e.Store.DB().Exec("DELETE FROM message_raw")
	require.NoError(t, err)
	s := acquisitionReplaySource(t, e)
	recordSyncRunItems(t, e, s.ID, store.SyncStatusCompleted, store.SyncRunItem{SourceMessageID: "lost", Phase: "ingest", Status: "error", ErrorKind: "ingest_error"})
	acquisitionReplayReopen(t, e)
	_, err = e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	acquisitionReplayRaw(t, e, "lost")
	var n int
	require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM messages").Scan(&n))
	require.Equal(t, 1, n)
}
