package store_test

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

// Run explicitly with MSGVAULT_TEST_ACQUISITION_PERF=1. This exercises the real
// deletion and GC APIs, reports complete operation costs and explains captured
// production SQL. There are no wall-clock assertions. MSGVAULT_TEST_DB selects
// PostgreSQL through the normal native fixtures; public GC remains SQLite-only.
// The same test can be copied to an older revision to compare query shapes.
func TestAcquisitionLifecyclePlansAndScaling(t *testing.T) {
	if os.Getenv("MSGVAULT_TEST_ACQUISITION_PERF") != "1" {
		t.Skip("set MSGVAULT_TEST_ACQUISITION_PERF=1 to measure a synthetic 400k-event journal")
	}
	must := require.New(t)
	st := testutil.NewTestStore(t)
	var sources, conversations []int64
	const history = 100000
	for sourceIndex := range 4 {
		source, err := st.GetOrCreateSource("gmail", fmt.Sprintf("scaling-%d@example.test", sourceIndex))
		must.NoError(err)
		conversation, err := st.EnsureConversation(source.ID, "scaling", "Synthetic scaling")
		must.NoError(err)
		sources = append(sources, source.ID)
		conversations = append(conversations, conversation)
		var runs []int64
		for range 40 {
			run, err := st.StartSync(source.ID, "incremental")
			must.NoError(err)
			runs = append(runs, run)
			must.NoError(st.CompleteSync(run, ""))
		}
		events := make([]acquisitionLookupEvent, 0, history)
		for i := range history {
			phase, status, kind := "fetch", "error", "fetch_error"
			switch (i / 10000) % 10 {
			case 1:
				status, kind = "skipped", store.AcquisitionReplayResolved
			case 2:
				status, kind = "skipped", store.AcquisitionReplaySourceDeleted
			case 3:
				status, kind = "skipped", "message_gone"
			case 4:
				phase, kind = "ingest", "ingest_error"
			case 5:
				status, kind = "skipped", "gmail_not_found"
			case 6, 7, 8:
				phase, status, kind = "labels", "skipped", "label_unchanged"
			}
			if i%997 == 0 {
				phase, status, kind = "fetch", "skipped", store.AcquisitionReplayLocalDeletion
			}
			events = append(events, acquisitionLookupEvent{
				runs[(i/500)%len(runs)], fmt.Sprintf("scale-%05d", i%10000), phase, status, kind,
			})
		}
		insertAcquisitionLookupEvents(t, st, events)
	}
	// Three quarters have archived content; the remainder exist only in the
	// journal. Include tombstone writes in the complete provider operation cost.
	seedAcquisitionLookupProviderMessages(t, st, sources[0], conversations[0])
	if st.IsPostgreSQL() {
		_, err := st.DB().Exec("VACUUM ANALYZE sync_run_items")
		must.NoError(err)
	}
	_, err := st.DB().Exec("ANALYZE")
	must.NoError(err)
	trace := traceAcquisitionLookups(t)
	for _, count := range []int{1, 500, 2500, 10000} {
		ids := make([]string, count)
		for i := range ids {
			ids[i] = fmt.Sprintf("scale-%05d", i)
		}
		var durations []time.Duration
		var queries []acquisitionLookupQuery
		for range 3 {
			start := time.Now()
			must.NoError(st.MarkMessagesDeletedBatch(sources[0], ids))
			durations = append(durations, time.Since(start))
			queries = trace.take()
			var tombstones int
			must.NoError(st.DB().QueryRow(`SELECT count(*) FROM messages WHERE deleted_from_source_at IS NOT NULL`).Scan(&tombstones))
			must.Equal(count-count/4, tombstones)
			_, err := st.DB().Exec(`UPDATE messages SET deleted_from_source_at=NULL`)
			must.NoError(err)
			_, err = st.DB().Exec(`DELETE FROM sync_run_items WHERE id>400000`)
			must.NoError(err)
		}
		logAcquisitionLookupCost(t, "provider", count, durations, len(queries))
		must.NotEmpty(queries)
		var args []any
		args = append(args, sources[0])
		if strings.Contains(queries[0].statement, "json") {
			encoded, err := json.Marshal(ids)
			must.NoError(err)
			args = append(args, string(encoded))
		} else {
			for _, id := range ids[:min(count, 500)] {
				args = append(args, id)
			}
		}
		logAcquisitionLookupPlan(t, st, queries[0].statement, args...)
	}
	_, err = st.DB().Exec(`DELETE FROM messages`)
	must.NoError(err)
	if st.IsPostgreSQL() {
		t.Log("GC unavailable: the public archive GC API is SQLite-only; no PostgreSQL GC cost is claimed")
		return
	}
	for _, count := range []int{1, 500, 2500, 10000} {
		var durations []time.Duration
		var queries []acquisitionLookupQuery
		for range 3 {
			seedAcquisitionLookupGCCandidates(t, st, sources, conversations, count)
			start := time.Now()
			plan, err := st.PlanGCContext(t.Context())
			must.NoError(err)
			deleted, err := st.ExecuteGCContext(t.Context(), plan)
			durations = append(durations, time.Since(start))
			must.NoError(err)
			must.Equal(int64(count), deleted)
			queries = trace.take()
			_, err = st.DB().Exec(`DELETE FROM sync_run_items WHERE id>400000`)
			must.NoError(err)
		}
		logAcquisitionLookupCost(t, "gc-plan-and-execute", count, durations, len(queries))
		must.NotEmpty(queries)
		args := []any{sources[0]}
		if strings.Contains(queries[0].statement, "FROM messages") {
			args = append(args, sources[0])
		} else {
			for i := range min((count+1)/2, 250) {
				id := fmt.Sprintf("scale-%05d", i)
				if i%4 == 0 {
					id = fmt.Sprintf("untracked-%05d", i)
				}
				args = append(args, id)
			}
		}
		// QUERY PLAN describes the actual captured statement without executing
		// another GC. The candidates have already been removed at this point.
		logAcquisitionLookupPlan(t, st, queries[0].statement, args...)
	}
}

func seedAcquisitionLookupProviderMessages(t *testing.T, st *store.Store, source, conversation int64) {
	t.Helper()
	must := require.New(t)
	tx, err := st.DB().BeginTx(t.Context(), nil)
	must.NoError(err)
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < 10000; start += 500 {
		var values []string
		var args []any
		for i := start; i < start+500; i++ {
			if i%4 == 3 {
				continue
			}
			values = append(values, "(?,?,?,'email')")
			args = append(args, source, conversation, fmt.Sprintf("scale-%05d", i))
		}
		_, err = tx.Exec(st.Rebind(`INSERT INTO messages(source_id,conversation_id,source_message_id,message_type) VALUES `+strings.Join(values, ",")), args...)
		must.NoError(err)
	}
	_, err = tx.Exec(st.Rebind(`INSERT INTO message_raw(message_id,raw_data,raw_format,compression)
		SELECT id,?,'mime','none' FROM messages`), []byte("synthetic RAW"))
	must.NoError(err)
	must.NoError(tx.Commit())
}

func seedAcquisitionLookupGCCandidates(t *testing.T, st *store.Store, sources, conversations []int64, count int) {
	t.Helper()
	must := require.New(t)
	tx, err := st.DB().BeginTx(t.Context(), nil)
	must.NoError(err)
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < count; start += 500 {
		var values []string
		var args []any
		for i := start; i < min(start+500, count); i++ {
			id := fmt.Sprintf("scale-%05d", i/2)
			if i%8 < 2 {
				id = fmt.Sprintf("untracked-%05d", i/2)
			}
			values = append(values, "(?,?,?,'email',CURRENT_TIMESTAMP)")
			args = append(args, sources[i%2], conversations[i%2], id)
		}
		_, err = tx.Exec(`INSERT INTO messages(source_id,conversation_id,source_message_id,message_type,deleted_from_source_at) VALUES `+strings.Join(values, ","), args...)
		must.NoError(err)
	}
	_, err = tx.Exec(`INSERT INTO message_raw(message_id,raw_data,raw_format,compression) SELECT id,X'73796E746865746963','mime','none' FROM messages`)
	must.NoError(err)
	must.NoError(tx.Commit())
}

func logAcquisitionLookupCost(t *testing.T, operation string, ids int, durations []time.Duration, queries int) {
	t.Helper()
	slices.Sort(durations)
	result := struct {
		Operation string    `json:"operation"`
		IDs       int       `json:"ids"`
		Queries   int       `json:"history_queries"`
		TimesMS   []float64 `json:"times_ms"`
	}{Operation: operation, IDs: ids, Queries: queries}
	for _, duration := range durations {
		result.TimesMS = append(result.TimesMS, float64(duration.Microseconds())/1000)
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	t.Logf("acquisition-cost %s", encoded)
}

func logAcquisitionLookupPlan(t *testing.T, st *store.Store, statement string, args ...any) {
	t.Helper()
	must := require.New(t)
	prefix := "EXPLAIN QUERY PLAN "
	if st.IsPostgreSQL() {
		prefix = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
	}
	rows, err := st.DB().QueryContext(t.Context(), prefix+statement, args...)
	must.NoError(err)
	defer func() { _ = rows.Close() }()
	t.Logf("acquisition-statement %s", statement)
	for rows.Next() {
		if st.IsPostgreSQL() {
			var plan string
			must.NoError(rows.Scan(&plan))
			t.Logf("acquisition-plan %s", plan)
		} else {
			var id, parent, unused int
			var detail string
			must.NoError(rows.Scan(&id, &parent, &unused, &detail))
			t.Logf("acquisition-plan %d %d %s", id, parent, detail)
		}
	}
	must.NoError(rows.Err())
	must.NoError(rows.Close())
}
