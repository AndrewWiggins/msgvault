package store_test

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestAcquisitionLocalDeletionIndexUpgrade(t *testing.T) {
	for _, name := range []string{"fresh", "existing-v13", "official-v14", "experimental-v14", "invalid-index"} {
		t.Run(name, func(t *testing.T) {
			must := require.New(t)
			checks := assert.New(t)
			if name == "invalid-index" && !store.IsPostgresURL(os.Getenv("MSGVAULT_TEST_DB")) {
				t.Skip("PostgreSQL concurrent builds can leave an invalid index")
			}
			st, path := reopenableAcquisitionStore(t)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			source, err := st.GetOrCreateSource("gmail", "index@example.test")
			must.NoError(err)
			other, err := st.GetOrCreateSource("gmail", "other-index@example.test")
			must.NoError(err)
			conversation, err := st.EnsureConversation(other.ID, "synthetic-thread", "Synthetic upgrade")
			must.NoError(err)
			archived, err := st.PersistMessage(&store.MessagePersistData{
				Message: &store.Message{
					SourceID: other.ID, SourceMessageID: "archived",
					ConversationID: conversation, MessageType: "email",
				},
				RawMIME:   []byte("synthetic preserved RAW"),
				RawFormat: "mime",
			})
			must.NoError(err)
			run, err := st.StartSync(source.ID, "incremental")
			must.NoError(err)
			otherRun, err := st.StartSync(other.ID, "incremental")
			must.NoError(err)
			for range 2 {
				must.NoError(st.RecordSyncRunItem(store.SyncRunItem{
					SyncRunID: run, SourceMessageID: "same-native", Phase: "fetch",
					Status: store.SyncRunItemStatusSkipped, ErrorKind: store.AcquisitionReplayLocalDeletion,
					ErrorMessage: "synthetic intentional deletion",
				}))
			}
			must.NoError(st.RecordSyncRunItem(store.SyncRunItem{
				SyncRunID: otherRun, SourceMessageID: "same-native", Phase: "fetch",
				Status: store.SyncRunItemStatusError, ErrorKind: "fetch_error",
				ErrorMessage: "synthetic original diagnostic",
			}))
			if name != "fresh" {
				// A version number cannot distinguish the two v14 contracts.
				// Reconstruct each variant and verify its historical fingerprint
				// before setup can add either the Inline seed or the index.
				version := "14"
				if name == "existing-v13" || name == "invalid-index" {
					version = "13"
				}
				if name != "experimental-v14" {
					_, err = st.DB().Exec("DROP INDEX idx_sync_run_items_local_deletion")
					must.NoError(err)
				}
				if name != "official-v14" {
					_, err = st.DB().Exec("DELETE FROM applied_migrations WHERE name='communication_services_inline_v1'")
					must.NoError(err)
					_, err = st.DB().Exec("DELETE FROM communication_services WHERE slug='inline'")
					must.NoError(err)
				}
				_, err = st.DB().Exec(st.Rebind("UPDATE archive_metadata SET value=? WHERE key='schema_version'"), version)
				must.NoError(err)
				sqliteDigest, pgDigest := schemaContractDigests[13], schemaContractPostgresDigests[13]
				switch name {
				case "official-v14":
					sqliteDigest, pgDigest = schemaContractDigests[14], schemaContractPostgresDigests[14]
				case "experimental-v14":
					// The index-only v14 predates the official Inline seed. Keep
					// its alternate digests outside the canonical version history.
					sqliteDigest = "e29d6da841eff56a84293786236df395bbb63a30c83f588d9a95d7ce2ffe4b66"
					pgDigest = "d2ec44a7dd55a81f63467b940d842374712a2b01013734fbb62800ce17904487"
				}
				if st.IsPostgreSQL() {
					must.Equal(pgDigest, schemaContractPostgresDigest(t, st))
				} else {
					must.Equal(sqliteDigest, schemaContractSQLiteDigest(t, st))
				}
				if name == "invalid-index" {
					// Duplicate local decisions make this concurrent UNIQUE build
					// fail after leaving an invalid index with the migration's name.
					_, err = st.DB().Exec(`CREATE UNIQUE INDEX CONCURRENTLY idx_sync_run_items_local_deletion
						ON sync_run_items(source_message_id, sync_run_id)
						WHERE phase='fetch' AND status='skipped' AND error_kind='acquisition_local_deletion'`)
					must.Error(err)
					var invalid bool
					must.NoError(st.DB().QueryRow(`SELECT NOT indisvalid FROM pg_index
						WHERE indexrelid='idx_sync_run_items_local_deletion'::regclass`).Scan(&invalid))
					must.True(invalid)
				}
				must.NoError(st.Close())
				st, err = store.Open(path)
				must.NoError(err)
				must.Error(st.ValidateSchemaContext(t.Context()))
				must.NoError(st.InitSchemaContext(t.Context()))
			}
			for range 2 {
				must.NoError(st.Close())
				st, err = store.Open(path)
				must.NoError(err)
				must.NoError(st.ValidateSchemaContext(t.Context()))
				raw, err := st.GetMessageRaw(archived)
				must.NoError(err)
				checks.Equal([]byte("synthetic preserved RAW"), raw)
				version, err := st.SchemaVersionContext(t.Context())
				must.NoError(err)
				checks.Equal(store.SchemaVersion, version)
				assertCombinedSchemaObjects(t, st)
				pending, err := st.PendingAcquisitionReplayIDs(source.ID)
				must.NoError(err)
				checks.Empty(pending)
				pending, err = st.PendingAcquisitionReplayIDs(other.ID)
				must.NoError(err)
				checks.Equal([]string{"same-native"}, pending)
				var diagnostics int
				must.NoError(st.DB().QueryRow("SELECT count(*) FROM sync_run_items WHERE error_message IN ('synthetic intentional deletion','synthetic original diagnostic')").Scan(&diagnostics))
				checks.Equal(3, diagnostics)
				_, err = st.ScopedToSync(source.ID, run).PersistMessage(&store.MessagePersistData{
					Message: &store.Message{SourceID: source.ID, SourceMessageID: "same-native", MessageType: "email"},
					RawMIME: []byte("synthetic raw"), RawFormat: "mime",
				})
				must.ErrorIs(err, store.ErrAcquisitionLocallyDeleted)
				_, err = st.ScopedToSync(other.ID, otherRun).PersistMessage(&store.MessagePersistData{
					Message: &store.Message{
						SourceID: other.ID, SourceMessageID: "same-native",
						ConversationID: conversation, MessageType: "email",
					},
					RawMIME: []byte("synthetic raw"), RawFormat: "mime",
				})
				must.NoError(err)
				must.NoError(st.InitSchemaContext(t.Context()), "repeated setup must retain a valid, nonunique source-aware index")
			}
		})
	}
}

// Inspect definitions as well as behavior: a same-named unique or unfiltered
// index would change acquisition semantics or bring back journal-wide scans.
func assertCombinedSchemaObjects(t *testing.T, st *store.Store) {
	t.Helper()
	var definition string
	if st.IsPostgreSQL() {
		var valid, unique bool
		require.NoError(t, st.DB().QueryRow(`SELECT pg_get_indexdef(i.indexrelid), i.indisvalid, i.indisunique
            FROM pg_index i WHERE i.indexrelid='idx_sync_run_items_local_deletion'::regclass`).Scan(&definition, &valid, &unique))
		assert.True(t, valid)
		assert.False(t, unique)
	} else {
		require.NoError(t, st.DB().QueryRow(`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_sync_run_items_local_deletion'`).Scan(&definition))
		assert.NotContains(t, strings.ToUpper(definition), "UNIQUE")
	}
	assert.Contains(t, definition, "(source_message_id, sync_run_id)")
	for _, value := range []string{"WHERE", "phase", "'fetch'", "status", "'skipped'", "error_kind", "'acquisition_local_deletion'"} {
		assert.Contains(t, definition, value)
	}
	inline, err := st.ResolveCommunicationServiceContext(t.Context(), "inline")
	require.NoError(t, err)
	assert.True(t, inline.IsSystem)
	assert.Equal(t, store.ScopePolicyRequired, inline.ScopePolicy)
	require.NotNil(t, inline.DefaultScopeKind)
	assert.Equal(t, "server", *inline.DefaultScopeKind)
	var applied, failures int
	require.NoError(t, st.DB().QueryRow(`SELECT count(*) FROM applied_migrations WHERE name='communication_services_inline_v1' AND version=1`).Scan(&applied))
	assert.Equal(t, 1, applied)
	require.NoError(t, st.DB().QueryRow(`SELECT count(*) FROM source_messaging_route_failures`).Scan(&failures))
	assert.Zero(t, failures)
}

func reopenableAcquisitionStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acquisition.db")
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
		path = u.String()
	} else {
		st = testutil.NewSQLiteTestStoreAt(t, path)
	}
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st, path
}

func TestAcquisitionLocalDeletionDoesNotBlockInline(t *testing.T) {
	must := require.New(t)
	checks := assert.New(t)
	st := testutil.NewTestStore(t)
	gmail, err := st.GetOrCreateSource("gmail", "source-isolation@example.test")
	must.NoError(err)
	inline, err := st.GetOrCreateSource("inline", "api.inline.chat:user:42")
	must.NoError(err)
	run, err := st.StartSync(gmail.ID, "incremental")
	must.NoError(err)
	const nativeID = "chat:123:message:7"
	must.NoError(st.RecordSyncRunItem(store.SyncRunItem{
		SyncRunID: run, SourceMessageID: nativeID, Phase: "fetch",
		Status: store.SyncRunItemStatusSkipped, ErrorKind: store.AcquisitionReplayLocalDeletion,
	}))
	inlineRun, err := st.StartSync(inline.ID, "incremental")
	must.NoError(err)
	conversation, err := st.EnsureConversationWithType(inline.ID, "chat:123", "group_chat", "Synthetic Inline group")
	must.NoError(err)
	// Native identifiers belong to a source. An absorbing Gmail decision must
	// not prevent an unrelated importer from persisting the same identifier.
	id, err := st.ScopedToSync(inline.ID, inlineRun).PersistMessage(&store.MessagePersistData{
		Message: &store.Message{SourceID: inline.ID, SourceMessageID: nativeID, ConversationID: conversation, MessageType: "chat"},
		RawMIME: []byte(`{"synthetic":true}`), RawFormat: "json",
	})
	must.NoError(err)
	raw, err := st.GetMessageRaw(id)
	must.NoError(err)
	checks.Equal([]byte(`{"synthetic":true}`), raw)
	pending, err := st.PendingAcquisitionReplayIDs(gmail.ID)
	must.NoError(err)
	checks.Empty(pending)
}
