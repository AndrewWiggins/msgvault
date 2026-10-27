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
			_, err := e.Store.DB().Exec("CREATE TRIGGER fail_journal BEFORE INSERT ON sync_run_items BEGIN SELECT RAISE(ABORT,'synthetic journal error'); END")
			require.NoError(t, err)
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
			_, err = e.Store.DB().Exec("DROP TRIGGER fail_journal")
			require.NoError(t, err)
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
	_, err := e.Store.DB().Exec("UPDATE sync_runs SET sync_type='' WHERE source_id=?", s.ID)
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
	delete(e.Mock.GetMessageError, "404-id")
	e.Mock.AddMessage("404-id", testMIME(), nil)
	third, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
	require.NoError(t, err)
	require.Equal(t, int64(1), third.MessagesAdded)
	acquisitionReplayRaw(t, e, "404-id")
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
