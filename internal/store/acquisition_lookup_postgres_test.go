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

// Exercise the executed SQL without requiring autovacuum or ANALYZE before a
// deletion. The larger opt-in fixture also reports complete API costs and plans.
func TestAcquisitionLifecyclePostgresStatistics(t *testing.T) {
	if !store.IsPostgresURL(os.Getenv("MSGVAULT_TEST_DB")) {
		t.Skip("native PostgreSQL planner regression")
	}
	history := 5000
	if os.Getenv("MSGVAULT_TEST_ACQUISITION_PERF") == "1" {
		history = 50000
	}
	for _, statistics := range []string{"absent", "stale", "present"} {
		t.Run(statistics, func(t *testing.T) {
			must := require.New(t)
			st := testutil.NewTestStore(t)
			// This test-only lock excludes automatic ANALYZE but permits the
			// production API's reads/writes on other connections. ANALYZE below
			// runs on the lock-owning connection. No planner settings are changed.
			statsTx, err := st.DB().BeginTx(t.Context(), nil)
			must.NoError(err)
			defer func() { _ = statsTx.Rollback() }()
			_, err = statsTx.Exec("LOCK TABLE sync_run_items IN SHARE UPDATE EXCLUSIVE MODE")
			must.NoError(err)
			var sources, runs []int64
			for i := range 4 {
				source, err := st.GetOrCreateSource("gmail", fmt.Sprintf("statistics-%d@example.test", i))
				must.NoError(err)
				sources = append(sources, source.ID)
				run, err := st.StartSync(source.ID, "incremental")
				must.NoError(err)
				runs = append(runs, run)
			}
			if statistics == "stale" {
				var events []acquisitionLookupEvent
				for _, run := range runs {
					for i := range 100 {
						events = append(events, acquisitionLookupEvent{run, fmt.Sprintf("legacy-%d", i), "labels", "skipped", "label_unchanged"})
					}
				}
				insertAcquisitionLookupEvents(t, st, events)
				_, err = statsTx.Exec("ANALYZE sync_run_items, sync_runs")
				must.NoError(err)
			}
			for _, run := range runs {
				events := make([]acquisitionLookupEvent, 0, history)
				for i := range history {
					events = append(events, acquisitionLookupEvent{run, fmt.Sprintf("%016x", 2*i), "fetch", "error", "fetch_error"})
				}
				insertAcquisitionLookupEvents(t, st, events)
			}
			if statistics == "present" {
				_, err = statsTx.Exec("ANALYZE sync_run_items, sync_runs")
				must.NoError(err)
			}
			var phaseStats string
			must.NoError(statsTx.QueryRow(`SELECT COALESCE((SELECT most_common_vals::text FROM pg_stats
				WHERE schemaname=current_schema() AND tablename='sync_run_items' AND attname='phase'), '')`).Scan(&phaseStats))
			switch statistics {
			case "absent":
				must.Empty(phaseStats)
			case "stale":
				must.Contains(phaseStats, "labels")
				must.NotContains(phaseStats, "fetch")
			case "present":
				must.Contains(phaseStats, "fetch")
			}
			var seedMax int64
			must.NoError(st.DB().QueryRow("SELECT MAX(id) FROM sync_run_items").Scan(&seedMax))
			trace := traceAcquisitionLookups(t)
			for _, count := range []int{1, 500, history * 2} {
				ids := make([]string, count)
				for i := range ids {
					ids[i] = fmt.Sprintf("%016x", i)
				}
				// These duplicates cross write chunks; only even IDs are tracked.
				requested := append(append([]string(nil), ids...), ids[0], ids[count-1])
				passes := 1
				if os.Getenv("MSGVAULT_TEST_ACQUISITION_PERF") == "1" {
					passes = 3
				}
				var queries []acquisitionLookupQuery
				for pass := range passes {
					_, err = st.DB().Exec(st.Rebind("DELETE FROM sync_run_items WHERE id>?"), seedMax)
					must.NoError(err)
					start := time.Now()
					must.NoError(st.MarkMessagesDeletedBatch(sources[0], requested))
					t.Logf("statistics-cost state=%s ids=%d pass=%d full_api_ms=%.3f", statistics, count, pass, float64(time.Since(start).Microseconds())/1000)
					queries = trace.take()
					must.Len(queries, 1)
				}
				var dispositions, foreign int
				must.NoError(st.DB().QueryRow(st.Rebind(`SELECT count(*), count(*) FILTER (WHERE sync_run_id<>?)
					FROM sync_run_items WHERE id>?`), runs[0], seedMax).Scan(&dispositions, &foreign))
				must.Equal((count+1)/2, dispositions)
				must.Zero(foreign)
				encoded, err := json.Marshal(ids)
				must.NoError(err)
				var explained string
				must.NoError(statsTx.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+queries[0].statement,
					sources[0], string(encoded)).Scan(&explained))
				checkAcquisitionLookupRequestTraversals(t, explained, count)
				if statistics == "present" && count == 500 {
					// An equivalent semijoin can also read the request once. The
					// regression guard must accept it without requiring a specific
					// join type or PostgreSQL's rendered "hashed SubPlan" text.
					alternate := strings.Replace(queries[0].statement, " IS TRUE", "", 1)
					var alternatePlan string
					must.NoError(statsTx.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+alternate,
						sources[0], string(encoded)).Scan(&alternatePlan))
					checkAcquisitionLookupRequestTraversals(t, alternatePlan, count)
				}
				if os.Getenv("MSGVAULT_TEST_ACQUISITION_PERF") == "1" {
					t.Logf("statistics-plan state=%s ids=%d %s", statistics, count, explained)
				}
				// Also exercise a prepared statement's generic plan. Values here
				// are generated hex IDs, not caller SQL or private data.
				_, err = statsTx.Exec("SET LOCAL plan_cache_mode=force_generic_plan")
				must.NoError(err)
				_, err = statsTx.Exec("PREPARE acquisition_lookup(bigint,text) AS " + queries[0].statement)
				must.NoError(err)
				must.NoError(statsTx.QueryRowContext(t.Context(), fmt.Sprintf(
					"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE acquisition_lookup(%d,'%s')", sources[0], encoded)).Scan(&explained))
				checkAcquisitionLookupRequestTraversals(t, explained, count)
				_, err = statsTx.Exec("DEALLOCATE acquisition_lookup")
				must.NoError(err)
				_, err = statsTx.Exec("RESET plan_cache_mode")
				must.NoError(err)
				_, err = st.DB().Exec(st.Rebind("DELETE FROM sync_run_items WHERE id>?"), seedMax)
				must.NoError(err)
			}
		})
	}
}

func checkAcquisitionLookupRequestTraversals(t *testing.T, explained string, count int) {
	t.Helper()
	type node struct {
		Type     string  `json:"Node Type"`
		Function string  `json:"Function Name"`
		Subplan  string  `json:"Subplan Name"`
		CTE      string  `json:"CTE Name"`
		Loops    float64 `json:"Actual Loops"`
		Rows     float64 `json:"Actual Rows"`
		Workers  int     `json:"Workers Launched"`
		Plans    []node  `json:"Plans"`
	}
	var plans []struct {
		Plan node `json:"Plan"`
	}
	must := require.New(t)
	must.NoError(json.Unmarshal([]byte(explained), &plans))
	must.Len(plans, 1)
	workers := 1
	var nodes []node
	var visit func(node)
	visit = func(n node) {
		workers += n.Workers
		nodes = append(nodes, n)
		for _, child := range n.Plans {
			visit(child)
		}
	}
	visit(plans[0].Plan)
	// Materializing JSON once is insufficient if its rows are then rescanned
	// per event. Follow request CTEs as well as inline materialization, without
	// constraining the join, aggregate, sort or index chosen by PostgreSQL.
	requestCTEs := make(map[string]bool)
	var containsRequest func(node) bool
	containsRequest = func(n node) bool {
		if n.Function == "jsonb_array_elements_text" || requestCTEs[n.CTE] {
			return true
		}
		return slices.ContainsFunc(n.Plans, containsRequest)
	}
	for changed := true; changed; {
		changed = false
		for _, n := range nodes {
			if name, found := strings.CutPrefix(n.Subplan, "CTE "); found && !requestCTEs[name] && containsRequest(n) {
				requestCTEs[name] = true
				changed = true
			}
		}
	}
	functions := 0
	for _, n := range nodes {
		if n.Function == "jsonb_array_elements_text" {
			functions++
			// Each parallel worker may own one copy. No rescan per event.
			must.LessOrEqual(n.Loops, float64(workers))
			must.InDelta(count, n.Rows, 0)
		}
		if requestCTEs[n.CTE] || (n.Type == "Materialize" && containsRequest(n)) {
			must.LessOrEqual(n.Loops, float64(workers), "materialized request must not be rescanned per event: %s", explained)
		}
	}
	must.Equal(1, functions)
}
