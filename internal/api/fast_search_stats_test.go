package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/query"
	"go.kenn.io/msgvault/internal/query/querytest"
	"go.kenn.io/msgvault/internal/search"
)

func TestHandleFastSearchStatsAreOptIn(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		parameter string
		wantView  query.ViewType
		wantStats bool
	}{
		{name: "omitted", wantView: query.ViewNoStats},
		{name: "senders", parameter: "&view_type=senders", wantView: query.ViewSenders, wantStats: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			assert := assert.New(t)
			var gotView query.ViewType
			engine := &querytest.MockEngine{
				SearchFastWithStatsFunc: func(_ context.Context, _ *search.Query, _ string,
					_ query.MessageFilter, view query.ViewType, _, _ int) (*query.SearchFastResult, error) {
					gotView = view
					result := &query.SearchFastResult{TotalCount: 7}
					if view != query.ViewNoStats {
						result.Stats = &query.TotalStats{}
					}
					return result, nil
				},
			}
			server := newTestServerWithEngine(t, engine)
			response := doGet(server, "/api/v1/search/fast?q=needle"+tc.parameter)
			require.Equal(http.StatusOK, response.Code, "body: %s", response.Body.String())
			assert.Equal(tc.wantView, gotView)
			var body map[string]json.RawMessage
			require.NoError(json.NewDecoder(response.Body).Decode(&body))
			stats, present := body["stats"]
			assert.Equal(tc.wantStats, present)
			if tc.wantStats {
				assert.NotEqual("null", string(stats))
			}
		})
	}
}

func TestHandleFastSearchPageOnly(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	engine := &querytest.MockEngine{
		SearchFastWithStatsFunc: func(_ context.Context, _ *search.Query, _ string,
			filter query.MessageFilter, _ query.ViewType, _, _ int) (*query.SearchFastResult, error) {
			assert.True(filter.Pagination.PageOnly)
			return &query.SearchFastResult{TotalCount: 7}, nil
		},
	}
	server := newTestServerWithEngine(t, engine)
	response := doGet(server, "/api/v1/search/fast?q=needle&page_only=true")
	require.Equal(http.StatusOK, response.Code, "body: %s", response.Body.String())
	var body map[string]json.RawMessage
	require.NoError(json.NewDecoder(response.Body).Decode(&body))
	assert.JSONEq("-1", string(body["total_count"]))
	assert.NotContains(body, "stats")
}

func TestHandleFastSearchPageOnlyRejectsView(t *testing.T) {
	engine := &querytest.MockEngine{
		SearchFastWithStatsFunc: func(_ context.Context, _ *search.Query, _ string,
			_ query.MessageFilter, _ query.ViewType, _, _ int) (*query.SearchFastResult, error) {
			assert.Fail(t, "rejected request must not reach the engine")
			return &query.SearchFastResult{}, nil
		},
	}
	response := doGet(newTestServerWithEngine(t, engine), "/api/v1/search/fast?q=needle&page_only=true&view_type=senders")
	assert.Equal(t, http.StatusBadRequest, response.Code)
}

func TestHandleDeepSearchPageOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		scope   string
		rows    int
		hasMore bool
	}{
		{"probe row", "", 3, true},
		{"last page", "", 2, false},
		{"body last page", "body", 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			called := false
			engine := &querytest.MockEngine{
				SearchDeepFunc: func(_ context.Context, _ *search.Query, _ query.MessageFilter, limit, offset int) ([]query.MessageSummary, error) {
					called = true
					assert.Empty(tc.scope)
					assert.Equal(3, limit)
					assert.Equal(4, offset)
					return make([]query.MessageSummary, tc.rows), nil
				},
				SearchDeepWithStatsFunc: func(_ context.Context, _ *search.Query, _ query.MessageFilter, _, _ int) (*query.SearchFastResult, error) {
					assert.Fail("page-only search must skip counts and stats")
					return &query.SearchFastResult{}, nil
				},
			}
			bodyEngine := &bodySearchTestEngine{
				MockEngine: engine,
				searchMessageBodiesFunc: func(_ context.Context, _ *search.Query, limit, offset int) ([]query.MessageSummary, error) {
					called = true
					assert.Equal("body", tc.scope)
					assert.Equal(3, limit)
					assert.Equal(4, offset)
					return make([]query.MessageSummary, tc.rows), nil
				},
			}
			response := doGet(newTestServerWithEngine(t, bodyEngine), "/api/v1/search/deep?q=needle&page_only=true&limit=2&offset=4&scope="+tc.scope)
			require.Equal(http.StatusOK, response.Code, "body: %s", response.Body.String())
			assert.True(called)
			var body map[string]json.RawMessage
			require.NoError(json.NewDecoder(response.Body).Decode(&body))
			assert.JSONEq("-1", string(body["total_count"]))
			assert.JSONEq(strconv.FormatBool(tc.hasMore), string(body["has_more"]))
			assert.NotContains(body, "stats")
			var messages []MessageSummary
			require.NoError(json.Unmarshal(body["messages"], &messages))
			assert.Len(messages, 2)
		})
	}
}
