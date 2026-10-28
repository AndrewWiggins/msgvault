package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

type acquisitionLookupQuery struct {
	statement string
	arguments int64
}

// Observe executed production statements, not elapsed-time thresholds. No tests
// using this process-wide logger run in parallel.
type acquisitionLookupTrace struct {
	mu      sync.Mutex
	queries []acquisitionLookupQuery
	pages   []acquisitionLookupQuery
	writes  []acquisitionLookupQuery
}

func (*acquisitionLookupTrace) Enabled(context.Context, slog.Level) bool { return true }
func (h *acquisitionLookupTrace) WithAttrs([]slog.Attr) slog.Handler     { return h }
func (h *acquisitionLookupTrace) WithGroup(string) slog.Handler          { return h }
func (h *acquisitionLookupTrace) Handle(_ context.Context, record slog.Record) error {
	var query acquisitionLookupQuery
	record.Attrs(func(attribute slog.Attr) bool {
		switch attribute.Key {
		case "stmt":
			query.statement = attribute.Value.String()
		case "nargs":
			query.arguments = attribute.Value.Int64()
		}
		return true
	})
	if strings.Contains(query.statement, "MAX(candidate.id)") {
		h.mu.Lock()
		h.queries = append(h.queries, query)
		h.mu.Unlock()
	} else if strings.Contains(query.statement, "SELECT messages.source_id, messages.source_message_id") {
		h.mu.Lock()
		h.pages = append(h.pages, query)
		h.mu.Unlock()
	} else if strings.Contains(query.statement, "INSERT INTO sync_run_items") {
		h.mu.Lock()
		h.writes = append(h.writes, query)
		h.mu.Unlock()
	}
	return nil
}

func traceAcquisitionLookups(t *testing.T) *acquisitionLookupTrace {
	t.Helper()
	trace := &acquisitionLookupTrace{}
	previous := slog.Default()
	slog.SetDefault(slog.New(trace))
	store.ConfigureSQLLogging(store.SQLLogOptions{FullTrace: true, MaxStmtChars: 1_000_000})
	t.Cleanup(func() {
		slog.SetDefault(previous)
		store.ConfigureSQLLogging(store.SQLLogOptions{})
	})
	return trace
}

func (h *acquisitionLookupTrace) take() []acquisitionLookupQuery {
	h.mu.Lock()
	defer h.mu.Unlock()
	queries := h.queries
	h.queries = nil
	return queries
}

type acquisitionLookupEvent struct {
	runID  int64
	native string
	phase  string
	status string
	kind   string
}

func insertAcquisitionLookupEvents(t *testing.T, st *store.Store, events []acquisitionLookupEvent) {
	t.Helper()
	must := require.New(t)
	tx, err := st.DB().BeginTx(t.Context(), nil)
	must.NoError(err)
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < len(events); start += 500 {
		var values []string
		var args []any
		for _, event := range events[start:min(start+500, len(events))] {
			values = append(values, "(?,?,?,?,?,'synthetic','2000-01-01 00:00:00')")
			args = append(args, event.runID, event.native, event.phase, event.status, event.kind)
		}
		_, err = tx.ExecContext(t.Context(), st.Rebind(`INSERT INTO sync_run_items
			(sync_run_id, source_message_id, phase, status, error_kind, error_message, created_at)
			VALUES `+strings.Join(values, ",")), args...)
		must.NoError(err)
	}
	must.NoError(tx.Commit())
}

func TestAcquisitionLifecycleLookupOnceAndBounded(t *testing.T) {
	for _, count := range []int{0, 1, 500, 1001, 35001} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			must := require.New(t)
			checks := assert.New(t)
			st := testutil.NewTestStore(t)
			source, err := st.GetOrCreateSource("gmail", "lookup@example.test")
			must.NoError(err)
			other, err := st.GetOrCreateSource("gmail", "other-lookup@example.test")
			must.NoError(err)
			oldRun, err := st.StartSync(source.ID, "full")
			must.NoError(err)
			must.NoError(st.CompleteSync(oldRun, ""))
			newRun, err := st.StartSync(source.ID, "incremental")
			must.NoError(err)
			otherRun, err := st.StartSync(other.ID, "incremental")
			must.NoError(err)

			var events []acquisitionLookupEvent
			// Unrequested identities must not be materialized in the lookup result.
			for i := range 3000 {
				events = append(events, acquisitionLookupEvent{oldRun, fmt.Sprintf("background-%d", i), "fetch", "error", "fetch_error"})
			}
			ids := make([]string, 0, count+2)
			tracked := min(count, 1500)
			for i := range count {
				ids = append(ids, fmt.Sprintf("native-%05d", i))
			}
			if count > 0 {
				ids[0] = "native-\"quoted\\identity"
			}
			for i, id := range ids[:tracked] {
				events = append(events,
					acquisitionLookupEvent{oldRun, id, "fetch", "error", "fetch_error"},
					acquisitionLookupEvent{newRun, id, "ingest", "error", "ingest_error"},
					acquisitionLookupEvent{otherRun, id, "fetch", "skipped", store.AcquisitionReplaySourceDeleted},
				)
				switch i % 4 {
				case 0:
					// Appended later to an older run: MAX(item.id), not MAX(run.id)
					// or a timestamp, determines the latest relevant event.
					events = append(events, acquisitionLookupEvent{oldRun, id, "fetch", "skipped", store.AcquisitionReplaySourceDeleted})
				case 1:
					// A matching kind in an unrelated phase must remain excluded.
					events = append(events, acquisitionLookupEvent{oldRun, id, "labels", "skipped", store.AcquisitionReplaySourceDeleted})
				case 2:
					// This helper deliberately has no status predicate.
					events = append(events, acquisitionLookupEvent{oldRun, id, "fetch", "legacy", store.AcquisitionReplaySourceDeleted})
				}
			}
			insertAcquisitionLookupEvents(t, st, events)
			if count > 0 {
				// Duplicates span write chunks, including the last, partial chunk.
				ids = append(ids, ids[0], ids[count-1])
			}
			trace := traceAcquisitionLookups(t)
			for range 2 {
				must.NoError(st.MarkMessagesDeletedBatch(source.ID, ids))
				queries := trace.take()
				if count == 0 {
					checks.Empty(queries)
					continue
				}
				must.Len(queries, 1, "one history traversal, independent of the number of write chunks")
				checks.Equal(int64(2), queries[0].arguments, "JSON input exceeds the SQL placeholder limit without expanding the statement")
				encoded, err := json.Marshal(ids)
				must.NoError(err)
				rows, err := st.DB().QueryContext(t.Context(), queries[0].statement, source.ID, string(encoded))
				must.NoError(err)
				defer func() { _ = rows.Close() }()
				resultIDs := make(map[string]bool)
				for rows.Next() {
					var id, kind string
					must.NoError(rows.Scan(&id, &kind))
					resultIDs[id] = true
				}
				must.NoError(rows.Err())
				must.NoError(rows.Close())
				checks.Len(resultIDs, tracked, "the result is bounded by requested tracked IDs, not the source journal")
			}
			for _, write := range trace.writes {
				checks.LessOrEqual(write.arguments, int64(500*4), "disposition writes bind four values per ID, at most 500 IDs")
			}
			var own, foreign, messages int
			must.NoError(st.DB().QueryRow(st.Rebind(`SELECT count(*) FROM sync_run_items item
				JOIN sync_runs run ON run.id=item.sync_run_id WHERE run.source_id=?
				AND item.phase='fetch' AND item.error_kind='acquisition_source_deleted'`), source.ID).Scan(&own))
			must.NoError(st.DB().QueryRow(st.Rebind(`SELECT count(*) FROM sync_run_items item
				JOIN sync_runs run ON run.id=item.sync_run_id WHERE run.source_id=?
				AND item.error_kind='acquisition_source_deleted'`), other.ID).Scan(&foreign))
			must.NoError(st.DB().QueryRow(`SELECT count(*) FROM messages`).Scan(&messages))
			checks.Equal(tracked, own, "append only changed dispositions; retain the latest event in older runs")
			checks.Equal(tracked, foreign, "another source's terminal events neither suppress own dispositions nor change")
			checks.Zero(messages, "history-only identities do not require archived messages")
		})
	}
}

func TestAcquisitionLifecycleLookupAfterPhysicalRemoval(t *testing.T) {
	must := require.New(t)
	checks := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource("gmail", "removed@example.test")
	must.NoError(err)
	run, err := st.StartSync(source.ID, "full")
	must.NoError(err)
	conversation, err := st.EnsureConversation(source.ID, "removed", "Synthetic removal")
	must.NoError(err)
	_, err = st.PersistMessage(&store.MessagePersistData{
		Message: &store.Message{SourceID: source.ID, SourceMessageID: "removed", ConversationID: conversation, MessageType: "email"},
		RawMIME: []byte("synthetic RAW"), RawFormat: "mime",
	})
	must.NoError(err)
	insertAcquisitionLookupEvents(t, st, []acquisitionLookupEvent{
		{run, "removed", "fetch", "error", "fetch_error"},
		{run, "never-archived", "fetch", "error", "fetch_error"},
	})
	_, err = st.DB().Exec(st.Rebind(`DELETE FROM messages WHERE source_id=?`), source.ID)
	must.NoError(err)
	must.NoError(st.MarkMessagesDeletedBatch(source.ID, []string{"removed", "never-archived", "untracked"}))
	var count int
	must.NoError(st.DB().QueryRow(`SELECT count(*) FROM sync_run_items WHERE error_kind='acquisition_source_deleted'`).Scan(&count))
	checks.Equal(2, count)
}

func TestAcquisitionLifecycleGCReadsEachSourceOnce(t *testing.T) {
	if store.IsPostgresURL(os.Getenv("MSGVAULT_TEST_DB")) {
		t.Skip("archive GC is SQLite-only; provider history lookups run on both backends")
	}
	must := require.New(t)
	checks := assert.New(t)
	st := testutil.NewSQLiteTestStore(t)
	const perSource = 1001
	var sources, conversations []int64
	var events []acquisitionLookupEvent
	for index, sourceType := range []string{"gmail", "gmail", "imap"} {
		source, err := st.GetOrCreateSource(sourceType, fmt.Sprintf("gc-lookup-%d@example.test", index))
		must.NoError(err)
		conversation, err := st.EnsureConversation(source.ID, "gc-lookup", "Synthetic GC lookup")
		must.NoError(err)
		run, err := st.StartSync(source.ID, "incremental")
		must.NoError(err)
		sources = append(sources, source.ID)
		conversations = append(conversations, conversation)
		for i := range 3000 {
			events = append(events, acquisitionLookupEvent{run, fmt.Sprintf("not-a-candidate-%d", i), "fetch", "error", "fetch_error"})
		}
		for i := 0; i < perSource; i += 2 {
			events = append(events, acquisitionLookupEvent{run, fmt.Sprintf("gc-native-%04d", i), "fetch", "error", "fetch_error"})
		}
	}
	insertAcquisitionLookupEvents(t, st, events)
	tx, err := st.DB().BeginTx(t.Context(), nil)
	must.NoError(err)
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < perSource*len(sources); start += 500 {
		var values []string
		var args []any
		for i := start; i < min(start+500, perSource*len(sources)); i++ {
			sourceIndex := i % len(sources)
			values = append(values, "(?,?,?,'email',CURRENT_TIMESTAMP)")
			args = append(args, sources[sourceIndex], conversations[sourceIndex], fmt.Sprintf("gc-native-%04d", i/len(sources)))
		}
		_, err = tx.Exec(`INSERT INTO messages(source_id, conversation_id, source_message_id, message_type, deleted_from_source_at) VALUES `+strings.Join(values, ","), args...)
		must.NoError(err)
	}
	// Retained messages in the same source must never enter the GC history set.
	_, err = tx.Exec(`INSERT INTO messages(source_id, conversation_id, source_message_id, message_type) VALUES (?,?,'not-a-candidate-0','email')`, sources[0], conversations[0])
	must.NoError(err)
	// A source-deleted managed draft is also outside the validated plan even
	// though the same source has ordinary purge candidates on every page.
	result, err := tx.Exec(`INSERT INTO messages(source_id, conversation_id, source_message_id, message_type, deleted_from_source_at)
		VALUES (?,?,'not-a-candidate-1','email',CURRENT_TIMESTAMP)`, sources[0], conversations[0])
	must.NoError(err)
	draftMessageID, err := result.LastInsertId()
	must.NoError(err)
	_, err = tx.Exec(`INSERT INTO gmail_drafts(draft_id, source_id, gmail_draft_id, current_message_id, current_gmail_message_id, thread_id, revision)
		VALUES ('retained-lookup-draft',?,'synthetic-remote-draft',?,'not-a-candidate-1','gc-lookup',1)`, sources[0], draftMessageID)
	must.NoError(err)
	_, err = tx.Exec(`INSERT INTO message_raw(message_id,raw_data,raw_format,compression) SELECT id,X'73796E746865746963','mime','none' FROM messages`)
	must.NoError(err)
	must.NoError(tx.Commit())
	plan, err := st.PlanGCContext(t.Context())
	must.NoError(err)
	must.Equal(int64(perSource*len(sources)), plan.SourceDeleted)
	trace := traceAcquisitionLookups(t)
	deleted, err := st.ExecuteGCContext(t.Context(), plan)
	must.NoError(err)
	checks.Equal(plan.SourceDeleted, deleted)
	queries := trace.take()
	must.Len(queries, 2, "each Gmail source is read once, across every candidate page")
	for _, page := range trace.pages {
		checks.LessOrEqual(page.arguments, int64(500), "candidate lookups remain bounded by primary-key pages")
	}
	checks.Len(trace.pages, (len(plan.SourceDeletedIDs)+499)/500)
	for _, write := range trace.writes {
		checks.LessOrEqual(write.arguments, int64(500*4))
	}
	for index, query := range queries {
		checks.Equal(int64(2), query.arguments)
		rows, err := st.DB().Query(query.statement, sources[index], sources[index])
		must.NoError(err)
		defer func() { _ = rows.Close() }()
		checks.False(rows.Next(), "purged plan IDs cannot select retained messages or unrelated journal identities")
		must.NoError(rows.Err())
		must.NoError(rows.Close())
	}
	for index, source := range sources {
		var dispositions int
		must.NoError(st.DB().QueryRow(`SELECT count(*) FROM sync_run_items item JOIN sync_runs run ON run.id=item.sync_run_id
			WHERE run.source_id=? AND item.error_kind='acquisition_source_deleted'`, source).Scan(&dispositions))
		want := (perSource + 1) / 2
		if index == 2 {
			want = 0
		}
		checks.Equal(want, dispositions)
	}
	var messages, raws int
	must.NoError(st.DB().QueryRow(`SELECT count(*) FROM messages`).Scan(&messages))
	must.NoError(st.DB().QueryRow(`SELECT count(*) FROM message_raw`).Scan(&raws))
	checks.Equal(2, messages)
	checks.Equal(2, raws)
}
