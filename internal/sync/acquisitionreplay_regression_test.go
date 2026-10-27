package sync

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func acquisitionReplayEnv(t *testing.T) *TestEnv {
	t.Helper()
	dir := t.TempDir()
	var st *store.Store
	if store.IsPostgresURL(os.Getenv("MSGVAULT_TEST_DB")) {
		st = testutil.NewTestStore(t)
		var database, schema string
		require.NoError(t, st.DB().QueryRow("SELECT current_database(), current_schema()").Scan(&database, &schema))
		u, err := url.Parse(os.Getenv("MSGVAULT_TEST_DB"))
		require.NoError(t, err)
		u.Path = "/" + database
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dir = u.String()
	} else {
		// Keep the durable filename used by reopen helpers, using the existing
		// initialized SQLite fixture rather than replaying schema DDL per test.
		st = testutil.NewSQLiteTestStoreAt(t, filepath.Join(dir, "durable.db"))
	}
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{EmailAddress: testEmail, HistoryID: 1000}
	e := &TestEnv{Store: st, Mock: mock, Syncer: New(mock, st, nil), TmpDir: dir, Context: context.Background()}
	t.Cleanup(func() { require.NoError(t, e.Store.Close()) })
	return e
}
func acquisitionReplayReopen(t *testing.T, e *TestEnv) {
	t.Helper()
	api := e.Syncer.client
	require.NoError(t, e.Store.Close())
	path := filepath.Join(e.TmpDir, "durable.db")
	if store.IsPostgresURL(e.TmpDir) {
		path = e.TmpDir
	}
	st, err := store.Open(path)
	require.NoError(t, err)
	e.Store = st
	e.Syncer = New(api, st, nil)
}
func acquisitionReplaySource(t *testing.T, e *TestEnv) *store.Source {
	t.Helper()
	s, err := e.Store.GetSourceByIdentifier(testEmail)
	require.NoError(t, err)
	return s
}
func acquisitionReplayAbsent(t *testing.T, e *TestEnv, id string) {
	t.Helper()
	s := acquisitionReplaySource(t, e)
	m, err := e.Store.MessageExistsBatch(s.ID, []string{id})
	require.NoError(t, err)
	require.Empty(t, m)
	var raws int
	require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM message_raw").Scan(&raws))
	t.Logf("missing=%s total_raws=%d cursor=%s", id, raws, s.SyncCursor.String)
}
func acquisitionReplayRaw(t *testing.T, e *TestEnv, id string) {
	t.Helper()
	s := acquisitionReplaySource(t, e)
	m, err := e.Store.MessageExistsBatch(s.ID, []string{id})
	require.NoError(t, err)
	require.Contains(t, m, id)
	raw, err := e.Store.GetMessageRaw(m[id])
	require.NoError(t, err)
	require.Equal(t, testMIME(), raw)
}
func acquisitionReplayTrigger(t *testing.T, e *TestEnv, table string) {
	t.Helper()
	require.Contains(t, []string{"messages", "message_raw"}, table)
	acquisitionReplayInstallFault(t, e, "acquisitionReplay_fail", table, "")
}
func acquisitionReplayDrop(t *testing.T, e *TestEnv) {
	t.Helper()
	acquisitionReplayDropFault(t, e, "acquisitionReplay_fail", []string{"messages", "message_raw"})
}
func acquisitionReplayDropIfPresent(t *testing.T, e *TestEnv) {
	t.Helper()
	acquisitionReplayDrop(t, e)
}
func acquisitionReplayRunRecord(t *testing.T, e *TestEnv, runID int64, wantPhase string) {
	t.Helper()
	var status, cursor string
	var errors int
	require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind("SELECT status,cursor_after,errors_count FROM sync_runs WHERE id=?"), runID).Scan(&status, &cursor, &errors))
	require.Equal(t, "completed", status)
	require.Equal(t, 1, errors)
	items, err := e.Store.ListSyncRunItems(runID, store.SyncRunItemStatusError, 100)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, wantPhase, items[0].Phase)
	t.Logf("durable run=%d status=%s cursor_after=%s errors=%d item=%+v", runID, status, cursor, errors, items[0])
}
func TestAcquisitionReplayScenario1(t *testing.T) {
	for _, table := range []string{"messages", "message_raw"} {
		t.Run(table, func(t *testing.T) {
			must := require.New(t)
			e := acquisitionReplayEnv(t)
			s := e.CreateSourceWithHistory(t, "1000")
			e.Mock.AddMessage("lost", testMIME(), []string{"INBOX"})
			e.SetHistory(1001, historyAdded("lost"))
			acquisitionReplayTrigger(t, e, table)
			first, err := e.Syncer.Incremental(e.Context, s)
			must.NoError(err)
			must.Equal(int64(1), first.Errors)
			must.Equal(int64(0), first.MessagesAdded)
			must.Equal([]string{"lost"}, e.Mock.GetMessageCalls)
			acquisitionReplayAbsent(t, e, "lost")
			acquisitionReplayDrop(t, e)
			acquisitionReplayReopen(t, e)
			must.Equal("1001", acquisitionReplaySource(t, e).SyncCursor.String)
			acquisitionReplayRunRecord(t, e, first.SyncRunID, "ingest")
			e.SetHistory(1001)
			for i := range 2 {
				next, err := e.Syncer.Incremental(e.Context, acquisitionReplaySource(t, e))
				must.NoError(err)
				must.Zero(next.Errors)
				must.Equal(int64(1-i), next.MessagesAdded)
				acquisitionReplayReopen(t, e)
				acquisitionReplayRaw(t, e, "lost")
			}
			must.Equal([]string{"lost", "lost"}, e.Mock.GetMessageCalls)
			positive, err := e.Syncer.Full(e.Context, testEmail)
			must.NoError(err)
			must.Zero(positive.MessagesAdded)
			acquisitionReplayRaw(t, e, "lost")
		})
	}
}
func TestAcquisitionReplayScenario2(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, failure := range []string{"fetch", "messages", "message_raw"} {
			t.Run(fmt.Sprintf("expired=%t/%s", recovery, failure), func(t *testing.T) {
				must := require.New(t)
				e := acquisitionReplayEnv(t)
				seedMessages(e, 3, 1001, "good1", "lost", "good2")
				// Target only the missing message: use a batch hook to install a SQL trigger
				// immediately before its single-message batch and remove it before the next.
				if failure == "fetch" {
					e.Mock.GetMessageError["lost"] = errors.New("AcquisitionReplay synthetic transient fetch")
				}
				if failure != "fetch" {
					e.Mock.MessagePages = [][]string{{"good1"}, {"lost"}, {"good2"}}
					api := &replayControlAPI{MockAPI: e.Mock, beforeBatch: func(_ int, ids []string) {
						acquisitionReplayDropIfPresent(t, e)
						if len(ids) == 1 && ids[0] == "lost" {
							acquisitionReplayTrigger(t, e, failure)
						}
					}}
					e.Syncer = New(api, e.Store, nil)
				}
				var first *gmail.SyncSummary
				var err error
				if recovery {
					s := e.CreateSourceWithHistory(t, "900")
					e.Mock.HistoryError = &gmail.NotFoundError{Path: "synthetic-history"}
					// Make only the first history request expire; catchup starts at snapshot=1001.
					first, err = e.Syncer.IncrementalWithHistoryRecovery(e.Context, s, nil)
				} else {
					first, err = e.Syncer.Full(e.Context, testEmail)
				}
				must.NoError(err)
				must.GreaterOrEqual(first.Errors, int64(1))
				must.Equal(int64(2), first.MessagesAdded)
				if failure != "fetch" {
					acquisitionReplayDropIfPresent(t, e)
				}
				// Remove the fault-injection wrapper as well as its trigger;
				// the final positive-control full must not reinstall the fault.
				e.Syncer = New(e.Mock, e.Store, nil)
				delete(e.Mock.GetMessageError, "lost")
				e.Mock.HistoryError = nil
				e.SetHistory(1001)
				acquisitionReplayReopen(t, e)
				s := acquisitionReplaySource(t, e)
				must.Equal("1001", s.SyncCursor.String)
				run, err := e.Store.GetLastSuccessfulSyncByType(s.ID, "full")
				must.NoError(err)
				phase := "ingest"
				if failure == "fetch" {
					phase = "fetch"
				}
				acquisitionReplayRunRecord(t, e, run.ID, phase)
				acquisitionReplayAbsent(t, e, "lost")
				before := len(e.Mock.GetMessageCalls)
				for i := range 2 {
					next, err := e.Syncer.IncrementalWithHistoryRecovery(e.Context, acquisitionReplaySource(t, e), nil)
					must.NoError(err)
					must.Equal(int64(1-i), next.MessagesAdded)
					acquisitionReplayReopen(t, e)
					acquisitionReplayRaw(t, e, "lost")
				}
				must.Len(e.Mock.GetMessageCalls, before+1)
				_, err = e.Syncer.Full(e.Context, testEmail)
				must.NoError(err)
				acquisitionReplayRaw(t, e, "lost")
				acquisitionReplayRaw(t, e, "good1")
				acquisitionReplayRaw(t, e, "good2")
			})
		}
	}
}
