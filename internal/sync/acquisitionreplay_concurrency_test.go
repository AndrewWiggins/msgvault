package sync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
)

// Block at the real Store's completed RAW lookup, after the driver has closed
// its rows and before the caller can append a resolution. No production hook
// or timing assumption is needed. These tests do not run in parallel because
// SQL logging uses the process-wide slog handler.
type acquisitionReplayBarrier struct {
	slog.Handler
	match   func(string) bool
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (b *acquisitionReplayBarrier) Enabled(context.Context, slog.Level) bool { return true }
func (b *acquisitionReplayBarrier) Handle(ctx context.Context, r slog.Record) error {
	var stmt string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "stmt" {
			stmt = a.Value.String()
		}
		return true
	})
	if b.match(stmt) && b.armed.CompareAndSwap(true, false) {
		close(b.reached)
		<-b.release
	}
	if b.Handler.Enabled(ctx, r.Level) {
		return b.Handler.Handle(ctx, r)
	}
	return nil
}

func acquisitionReplayPause(t *testing.T, match func(string) bool) (wait func(), resume func()) {
	t.Helper()
	old := slog.Default()
	// Do not forward to slog's default handler: SetDefault redirects the
	// standard logger, and forwarding to that handler would recurse.
	b := &acquisitionReplayBarrier{Handler: slog.NewTextHandler(os.Stderr, nil), match: match, reached: make(chan struct{}), release: make(chan struct{})}
	b.armed.Store(true)
	slog.SetDefault(slog.New(b))
	var once stdsync.Once
	resume = func() { once.Do(func() { close(b.release) }) }
	t.Cleanup(func() { resume(); slog.SetDefault(old) })
	wait = func() {
		t.Helper()
		select {
		case <-b.reached:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "replay did not reach SQL barrier")
		}
	}
	return wait, resume
}

func acquisitionReplayHide(t *testing.T, e *TestEnv, sourceID int64) {
	t.Helper()
	_, err := e.Store.DB().Exec(e.Store.Rebind(`UPDATE messages SET deleted_at=CURRENT_TIMESTAMP, delete_batch_id='synthetic-batch' WHERE source_id=? AND source_message_id='race-id'`), sourceID)
	require.NoError(t, err)
}

func acquisitionReplayDatabase(e *TestEnv) string {
	if store.IsPostgresURL(e.TmpDir) {
		return e.TmpDir
	}
	return filepath.Join(e.TmpDir, "durable.db")
}

// Invoked only by the parent test, with its freshly created synthetic DB.
// The child inherits the parent's network namespace and seccomp restrictions.
func TestAcquisitionReplayCleanupProcess(t *testing.T) {
	path := os.Getenv("MSGVAULT_TEST_ACQUISITION_PURGE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	st, err := store.Open(path)
	require.NoError(t, err)
	defer st.Close()
	busy := os.Getenv("MSGVAULT_TEST_ACQUISITION_PURGE_BUSY") == "1"
	if busy {
		st.DB().SetMaxOpenConns(1)
		statement := "PRAGMA busy_timeout=0"
		if st.IsPostgreSQL() {
			statement = "SET lock_timeout='250ms'"
		}
		_, err := st.DB().Exec(statement)
		require.NoError(t, err)
	}
	var n int64
	if os.Getenv("MSGVAULT_TEST_ACQUISITION_UNDO") == "1" {
		n, err = st.UndoDedup("synthetic-batch")
	} else {
		n, _, err = st.DeleteAllDedupedContext(t.Context())
	}
	if busy {
		require.Error(t, err, "a concurrent resolution still owns the transaction lock")
		assert.Contains(t, err.Error(), "lock")
		assert.Zero(t, n)
		return
	}
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func acquisitionReplayProcessPurge(t *testing.T, e *TestEnv) {
	acquisitionReplayProcessPurgeWithLock(t, e, false)
}

func acquisitionReplayProcessPurgeWithLock(t *testing.T, e *TestEnv, busy bool) {
	acquisitionReplayProcessMutation(t, e, busy, false)
}

func acquisitionReplayProcessMutation(t *testing.T, e *TestEnv, busy, undo bool) {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestAcquisitionReplayCleanupProcess$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "MSGVAULT_TEST_ACQUISITION_PURGE="+acquisitionReplayDatabase(e))
	if busy {
		cmd.Env = append(cmd.Env, "MSGVAULT_TEST_ACQUISITION_PURGE_BUSY=1")
	}
	if undo {
		cmd.Env = append(cmd.Env, "MSGVAULT_TEST_ACQUISITION_UNDO=1")
	}
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestAcquisitionReplayPurgeKeepsJournaledPopulationHidden(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	acquisitionReplayCreateAndResolveDebt(t, e, s, "race-id")
	acquisitionReplayHide(t, e, s.ID)
	wait, resume := acquisitionReplayTransactionPause(t, e, "INSERT", "sync_run_items", "NEW.error_kind='acquisition_local_deletion'")
	done := make(chan error, 1)
	go func() { _, _, err := e.Store.DeleteAllDedupedContext(t.Context()); done <- err }()
	wait()
	acquisitionReplayProcessMutation(t, e, true, true)
	resume()
	require.NoError(t, <-done)
	acquisitionReplayDropFault(t, e, "replay_barrier", []string{"sync_run_items"})
	acquisitionReplayReopen(t, e)
	acquisitionReplayAbsent(t, e, "race-id")
	assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
	n, err := e.Store.UndoDedup("synthetic-batch")
	require.NoError(t, err)
	assert.Zero(t, n)
	calls := len(e.Mock.GetMessageCalls)
	acquisitionReplayRun(t, e, false)
	assert.Len(t, e.Mock.GetMessageCalls, calls)
	acquisitionReplayAbsent(t, e, "race-id")
}

// Stop inside an actual write transaction. SQLite invokes a driver function;
// PostgreSQL's trigger waits on an advisory lock owned by the test connection.
// Observing the server's waiting lock, rather than elapsed time, is the barrier.
func acquisitionReplayTransactionPause(t *testing.T, e *TestEnv, event, table, condition string) (func(), func()) {
	t.Helper()
	var wait, resume func()
	if e.Store.IsPostgreSQL() {
		conn, err := e.Store.DB().Conn(t.Context())
		require.NoError(t, err)
		_, err = conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(15140001)")
		require.NoError(t, err)
		ret := "NEW"
		if event == "DELETE" {
			ret = "OLD"
		}
		_, err = e.Store.DB().Exec("CREATE FUNCTION replay_barrier_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(15140001); RETURN " + ret + "; END $$; CREATE TRIGGER replay_barrier AFTER " + event + " ON " + table + " FOR EACH ROW WHEN (" + condition + ") EXECUTE FUNCTION replay_barrier_fn()")
		require.NoError(t, err)
		wait = func() {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				var waiting int
				err := e.Store.DB().QueryRow("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=15140001 AND NOT granted").Scan(&waiting)
				assert.NoError(c, err)
				assert.Positive(c, waiting)
			}, 30*time.Second, 10*time.Millisecond)
		}
		var once stdsync.Once
		resume = func() {
			once.Do(func() {
				_, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(15140001)")
				assert.NoError(t, err)
				assert.NoError(t, conn.Close())
			})
		}
	} else {
		e.Store.DB().SetMaxOpenConns(1)
		reached, release := make(chan struct{}), make(chan struct{})
		conn, err := e.Store.DB().Conn(t.Context())
		require.NoError(t, err)
		require.NoError(t, conn.Raw(func(driverConn any) error {
			return driverConn.(*sqlite3.SQLiteConn).RegisterFunc("replay_barrier", func() int { close(reached); <-release; return 1 }, false)
		}))
		require.NoError(t, conn.Close())
		_, err = e.Store.DB().Exec("CREATE TRIGGER replay_barrier AFTER " + event + " ON " + table + " WHEN " + condition + " BEGIN SELECT replay_barrier(); END")
		require.NoError(t, err)
		wait = func() {
			select {
			case <-reached:
			case <-time.After(30 * time.Second):
				require.FailNow(t, "SQLite transaction barrier not reached")
			}
		}
		var once stdsync.Once
		resume = func() { once.Do(func() { close(release) }) }
	}
	t.Cleanup(func() { resume(); acquisitionReplayDropFault(t, e, "replay_barrier", []string{table}) })
	return wait, resume
}

func TestAcquisitionReplayResolutionTransactionExcludesPurge(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "race-id")
			delete(e.Mock.GetMessageError, "race-id")
			_, err := e.Syncer.ingestMessage(t.Context(), s.ID, e.Mock.Messages["race-id"], "thread", nil)
			require.NoError(t, err)
			acquisitionReplayHide(t, e, s.ID)
			wait, resume := acquisitionReplayTransactionPause(t, e, "INSERT", "sync_run_items", "NEW.error_kind='acquisition_resolved'")
			done := make(chan error, 1)
			go func() {
				if full {
					_, err := e.Syncer.Full(t.Context(), testEmail)
					done <- err
				} else {
					_, err := e.Syncer.Incremental(t.Context(), s)
					done <- err
				}
			}()
			wait() // INSERT has executed, but its transaction has not committed.
			acquisitionReplayProcessPurgeWithLock(t, e, true)
			resume()
			require.NoError(t, <-done)
			acquisitionReplayDropFault(t, e, "replay_barrier", []string{"sync_run_items"})
			acquisitionReplayReopen(t, e)
			acquisitionReplayRaw(t, e, "race-id")
			assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayResolved))
			assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
			acquisitionReplayProcessPurge(t, e)
			acquisitionReplayReopen(t, e)
			calls := len(e.Mock.GetMessageCalls)
			acquisitionReplayRun(t, e, false)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
			acquisitionReplayAbsent(t, e, "race-id")
		})
	}
}

func TestAcquisitionReplayCanceledPurgeRollsBackDisposition(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	acquisitionReplayCreateAndResolveDebt(t, e, s, "race-id")
	acquisitionReplayHide(t, e, s.ID)
	wait, resume := acquisitionReplayTransactionPause(t, e, "DELETE", "messages", "OLD.source_message_id='race-id'")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := e.Store.DeleteAllDedupedContext(ctx); done <- err }()
	wait() // policy INSERT executed inside the still-open deletion transaction.
	cancel()
	resume()
	require.ErrorIs(t, <-done, context.Canceled)
	acquisitionReplayReopen(t, e)
	assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
	acquisitionReplayRaw(t, e, "race-id")
	calls := len(e.Mock.GetMessageCalls)
	acquisitionReplayRun(t, e, false)
	assert.Len(t, e.Mock.GetMessageCalls, calls)
	acquisitionReplayRaw(t, e, "race-id")
}

func TestAcquisitionReplayLocalDeletionPrecedesLateDiagnostics(t *testing.T) {
	for _, kind := range []string{"fetch_error", "batch_fetch_error", "ingest_error", "gmail_not_found", "message_gone", store.AcquisitionReplayResolved, store.AcquisitionReplaySourceDeleted} {
		t.Run(kind, func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := e.CreateSourceWithHistory(t, "1000")
			acquisitionReplayCreateAndResolveDebt(t, e, s, "race-id")
			acquisitionReplayHide(t, e, s.ID)
			acquisitionReplayProcessPurge(t, e)
			acquisitionReplayReopen(t, e)
			var runID int64
			require.NoError(t, e.Store.DB().QueryRow(e.Store.Rebind("SELECT max(id) FROM sync_runs WHERE source_id=?"), s.ID).Scan(&runID))
			item := store.SyncRunItem{SyncRunID: runID, SourceMessageID: "race-id", Phase: "fetch", Status: store.SyncRunItemStatusSkipped, ErrorKind: kind, ErrorMessage: "synthetic stale outcome"}
			if kind == "ingest_error" {
				item.Phase = "ingest"
			}
			if kind == "ingest_error" || strings.HasSuffix(kind, "fetch_error") {
				item.Status = store.SyncRunItemStatusError
			}
			require.NoError(t, e.Store.RecordSyncRunItem(item))
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Empty(t, pending)
			calls := len(e.Mock.GetMessageCalls)
			acquisitionReplayRun(t, e, false)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
			acquisitionReplayAbsent(t, e, "race-id")
			assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
		})
	}
}

func TestAcquisitionReplayLocalDeletionSurvivesNewHistory(t *testing.T) {
	for _, labels := range []bool{false, true} {
		t.Run(fmt.Sprint(labels), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := e.CreateSourceWithHistory(t, "1000")
			acquisitionReplayCreateAndResolveDebt(t, e, s, "race-id")
			acquisitionReplayHide(t, e, s.ID)
			acquisitionReplayProcessPurge(t, e)
			acquisitionReplayReopen(t, e)
			record := historyAdded("race-id")
			if labels {
				record = historyLabelAdded("race-id", "INBOX")
			}
			e.SetHistory(1002, record)
			result, err := e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
			require.NoError(t, err)
			assert.Zero(t, result.Errors)
			assert.Zero(t, result.MessagesAdded)
			acquisitionReplayReopen(t, e)
			acquisitionReplayAbsent(t, e, "race-id")
			calls := len(e.Mock.GetMessageCalls)
			e.SetHistory(1002)
			acquisitionReplayRun(t, e, false)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
			acquisitionReplayAbsent(t, e, "race-id")
		})
	}
}

func TestAcquisitionReplayDeletionBeforeFirstDiagnostic(t *testing.T) {
	e := acquisitionReplayEnv(t)
	s := e.CreateSourceWithHistory(t, "1000")
	e.Mock.AddMessage("race-id", testMIME(), []string{"INBOX"})
	e.SetHistory(1001, historyAdded("race-id"))
	result, err := e.Syncer.Incremental(t.Context(), s)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	var diagnostics int
	require.NoError(t, e.Store.DB().QueryRow("SELECT count(*) FROM sync_run_items").Scan(&diagnostics))
	require.Zero(t, diagnostics, "no debt or lifecycle event preceded the purge")
	acquisitionReplayHide(t, e, s.ID)
	acquisitionReplayProcessPurge(t, e)
	acquisitionReplayReopen(t, e)
	assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
	// A stale label-refresh/acquisition failure may be its first diagnostic.
	require.NoError(t, e.Store.RecordSyncRunItem(store.SyncRunItem{SyncRunID: result.SyncRunID, SourceMessageID: "race-id", Phase: "ingest", Status: store.SyncRunItemStatusError, ErrorKind: "ingest_error", ErrorMessage: "synthetic late first diagnostic"}))
	acquisitionReplayReopen(t, e)
	calls := len(e.Mock.GetMessageCalls)
	e.SetHistory(1001)
	acquisitionReplayRun(t, e, false)
	assert.Len(t, e.Mock.GetMessageCalls, calls)
	acquisitionReplayAbsent(t, e, "race-id")
	requireAcquisitionErrorHistory(t, e, "race-id", 1)
}

func TestAcquisitionReplayConcurrentLocalDeletion(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "incremental"
		if full {
			name = "full"
		}
		for _, process := range []bool{false, true} {
			processName := "/same-process"
			if process {
				processName = "/separate-process"
			}
			t.Run(name+processName, func(t *testing.T) {
				e := acquisitionReplayEnv(t)
				s := acquisitionReplayDebt(t, e, "race-id")
				delete(e.Mock.GetMessageError, "race-id")
				// Model a committed RAW followed by an interruption before resolution.
				_, err := e.Syncer.ingestMessage(t.Context(), s.ID, e.Mock.Messages["race-id"], "thread", nil)
				require.NoError(t, err)
				acquisitionReplayHide(t, e, s.ID)
				acquisitionReplayReopen(t, e)
				wait, resume := acquisitionReplayPause(t, func(stmt string) bool {
					return strings.Contains(stmt, "FROM messages m JOIN message_raw mr")
				})
				done := make(chan error, 1)
				go func() {
					if full {
						_, err := e.Syncer.Full(t.Context(), testEmail)
						done <- err
					} else {
						_, err := e.Syncer.Incremental(t.Context(), s)
						done <- err
					}
				}()
				wait()
				if process {
					acquisitionReplayProcessPurge(t, e)
				} else {
					n, err := acquisitionReplayPurge(t, e, "all")
					require.NoError(t, err)
					require.EqualValues(t, 1, n)
				}
				acquisitionReplayAbsent(t, e, "race-id")
				resume()
				require.NoError(t, <-done)
				acquisitionReplayReopen(t, e)
				pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
				require.NoError(t, err)
				assert.Empty(t, pending, "a stale resolution must not reopen intentional destruction")
				assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayResolved))
				assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
				calls := len(e.Mock.GetMessageCalls)
				_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
				require.NoError(t, err)
				assert.Len(t, e.Mock.GetMessageCalls, calls)
				acquisitionReplayAbsent(t, e, "race-id")
				requireAcquisitionErrorHistory(t, e, "race-id", 1)
			})
		}
	}
}

func acquisitionReplayRun(t *testing.T, e *TestEnv, full bool) {
	t.Helper()
	var err error
	if full {
		_, err = e.Syncer.Full(t.Context(), testEmail)
	} else {
		_, err = e.Syncer.Incremental(t.Context(), acquisitionReplaySource(t, e))
	}
	require.NoError(t, err)
}

func TestAcquisitionReplayDeletionOrdering(t *testing.T) {
	for _, full := range []bool{false, true} {
		for _, order := range []string{"deletion-first", "resolution-first", "journal-failure", "delete-rollback", "canceled"} {
			t.Run(fmt.Sprintf("full=%t/%s", full, order), func(t *testing.T) {
				e := acquisitionReplayEnv(t)
				s := e.CreateSourceWithHistory(t, "1000")
				acquisitionReplayCreateAndResolveDebt(t, e, s, "race-id")
				acquisitionReplayHide(t, e, s.ID)
				acquisitionReplayReopen(t, e)
				if order == "resolution-first" {
					acquisitionReplayRun(t, e, full)
					acquisitionReplayReopen(t, e)
				}
				switch order {
				case "journal-failure":
					acquisitionReplayInstallFault(t, e, "purge_fault", "sync_run_items", "NEW.error_kind='acquisition_local_deletion'")
				case "delete-rollback":
					if e.Store.IsPostgreSQL() {
						_, err := e.Store.DB().Exec(`CREATE FUNCTION purge_fault_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic delete rollback'; END $$; CREATE TRIGGER purge_fault BEFORE DELETE ON messages FOR EACH ROW EXECUTE FUNCTION purge_fault_fn()`)
						require.NoError(t, err)
					} else {
						_, err := e.Store.DB().Exec(`CREATE TRIGGER purge_fault BEFORE DELETE ON messages BEGIN SELECT RAISE(ABORT,'synthetic delete rollback'); END`)
						require.NoError(t, err)
					}
				}
				ctx, cancel := context.WithCancel(t.Context())
				if order == "canceled" {
					cancel()
				}
				n, _, err := e.Store.DeleteAllDedupedContext(ctx)
				cancel()
				failed := order == "journal-failure" || order == "delete-rollback" || order == "canceled"
				if failed {
					require.Error(t, err)
					assert.Zero(t, n)
				} else {
					require.NoError(t, err)
					assert.EqualValues(t, 1, n)
				}
				acquisitionReplayReopen(t, e)
				if failed {
					acquisitionReplayRaw(t, e, "race-id")
					assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
					if order != "canceled" {
						acquisitionReplayDropFault(t, e, "purge_fault", []string{"sync_run_items", "messages"})
					}
				} else {
					acquisitionReplayAbsent(t, e, "race-id")
					assert.Equal(t, 1, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
				}
				acquisitionReplayRun(t, e, full)
				acquisitionReplayReopen(t, e)
				calls := len(e.Mock.GetMessageCalls)
				acquisitionReplayRun(t, e, false)
				assert.Len(t, e.Mock.GetMessageCalls, calls)
				if failed {
					acquisitionReplayRaw(t, e, "race-id")
				} else {
					acquisitionReplayAbsent(t, e, "race-id")
				}
				requireAcquisitionErrorHistory(t, e, "race-id", 1)
			})
		}
	}
}

type acquisitionReplayFetchBarrier struct {
	*gmail.MockAPI
	reached chan struct{}
	release chan struct{}
}

func (a *acquisitionReplayFetchBarrier) GetMessagesRawBatchWithErrors(ctx context.Context, ids []string) ([]gmail.RawMessageBatchResult, error) {
	results, err := a.MockAPI.GetMessagesRawBatchWithErrors(ctx, ids)
	close(a.reached)
	<-a.release
	return results, err
}

func TestAcquisitionReplayDeletionDuringFetch(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "race-id")
			delete(e.Mock.GetMessageError, "race-id")
			api := &acquisitionReplayFetchBarrier{MockAPI: e.Mock, reached: make(chan struct{}), release: make(chan struct{})}
			e.Syncer = New(api, e.Store, nil)
			done := make(chan error, 1)
			go func() {
				if full {
					_, err := e.Syncer.Full(t.Context(), testEmail)
					done <- err
				} else {
					_, err := e.Syncer.Incremental(t.Context(), s)
					done <- err
				}
			}()
			select {
			case <-api.reached:
			case <-time.After(30 * time.Second):
				require.FailNow(t, "fetch barrier not reached")
			}
			// Another writer has archived the occurrence before explicit cleanup.
			_, err := New(e.Mock, e.Store, nil).ingestMessage(t.Context(), s.ID, e.Mock.Messages["race-id"], "thread", nil)
			require.NoError(t, err)
			acquisitionReplayHide(t, e, s.ID)
			acquisitionReplayProcessPurge(t, e)
			close(api.release)
			require.NoError(t, <-done)
			acquisitionReplayReopen(t, e)
			acquisitionReplayAbsent(t, e, "race-id")
			assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayResolved))
			calls := len(e.Mock.GetMessageCalls)
			acquisitionReplayRun(t, e, false)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
			acquisitionReplayAbsent(t, e, "race-id")
		})
	}
}

func TestAcquisitionReplayRawLossDuringResolution(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			e := acquisitionReplayEnv(t)
			s := acquisitionReplayDebt(t, e, "race-id")
			delete(e.Mock.GetMessageError, "race-id")
			_, err := e.Syncer.ingestMessage(t.Context(), s.ID, e.Mock.Messages["race-id"], "thread", nil)
			require.NoError(t, err)
			wait, resume := acquisitionReplayPause(t, func(stmt string) bool { return strings.Contains(stmt, "FROM messages m JOIN message_raw mr") })
			done := make(chan error, 1)
			go func() {
				if full {
					_, err := e.Syncer.Full(t.Context(), testEmail)
					done <- err
				} else {
					_, err := e.Syncer.Incremental(t.Context(), s)
					done <- err
				}
			}()
			wait()
			_, err = e.Store.DB().Exec("DELETE FROM message_raw")
			require.NoError(t, err)
			resume()
			require.NoError(t, <-done)
			acquisitionReplayReopen(t, e)
			pending, err := e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Equal(t, []string{"race-id"}, pending)
			assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayResolved))
			assert.Zero(t, acquisitionReplayDispositions(t, e, s.ID, "race-id", store.AcquisitionReplayLocalDeletion))
			acquisitionReplayRun(t, e, false)
			acquisitionReplayReopen(t, e)
			acquisitionReplayRaw(t, e, "race-id")
			pending, err = e.Store.PendingAcquisitionReplayIDs(s.ID)
			require.NoError(t, err)
			assert.Empty(t, pending)
			calls := len(e.Mock.GetMessageCalls)
			acquisitionReplayRun(t, e, false)
			assert.Len(t, e.Mock.GetMessageCalls, calls)
		})
	}
}
