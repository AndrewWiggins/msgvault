package store_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestAcquisitionLocalDeletionIndexUpgrade(t *testing.T) {
	for _, name := range []string{"fresh", "existing-v13", "invalid-index"} {
		t.Run(name, func(t *testing.T) {
			if name == "invalid-index" && !store.IsPostgresURL(os.Getenv("MSGVAULT_TEST_DB")) {
				t.Skip("PostgreSQL concurrent builds can leave an invalid index")
			}
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
			source, err := st.GetOrCreateSource("gmail", "index@example.test")
			require.NoError(t, err)
			other, err := st.GetOrCreateSource("gmail", "other-index@example.test")
			require.NoError(t, err)
			conversation, err := st.EnsureConversation(other.ID, "synthetic-thread", "Synthetic upgrade")
			require.NoError(t, err)
			archived, err := st.PersistMessage(&store.MessagePersistData{
				Message: &store.Message{
					SourceID: other.ID, SourceMessageID: "archived",
					ConversationID: conversation, MessageType: "email",
				},
				RawMIME:   []byte("synthetic preserved RAW"),
				RawFormat: "mime",
			})
			require.NoError(t, err)
			run, err := st.StartSync(source.ID, "incremental")
			require.NoError(t, err)
			otherRun, err := st.StartSync(other.ID, "incremental")
			require.NoError(t, err)
			for i := 0; i < 2; i++ {
				require.NoError(t, st.RecordSyncRunItem(store.SyncRunItem{
					SyncRunID: run, SourceMessageID: "same-native", Phase: "fetch",
					Status: store.SyncRunItemStatusSkipped, ErrorKind: store.AcquisitionReplayLocalDeletion,
					ErrorMessage: "synthetic intentional deletion",
				}))
			}
			require.NoError(t, st.RecordSyncRunItem(store.SyncRunItem{
				SyncRunID: otherRun, SourceMessageID: "same-native", Phase: "fetch",
				Status: store.SyncRunItemStatusError, ErrorKind: "fetch_error",
				ErrorMessage: "synthetic original diagnostic",
			}))
			if name != "fresh" {
				// The only v13 schema difference is this index. Preserve journal
				// data across a real close/reopen before running the upgrade.
				_, err = st.DB().Exec("DROP INDEX idx_sync_run_items_local_deletion")
				require.NoError(t, err)
				_, err = st.DB().Exec("UPDATE archive_metadata SET value='13' WHERE key='schema_version'")
				require.NoError(t, err)
				if name == "invalid-index" {
					// Duplicate local decisions make this concurrent UNIQUE build
					// fail after leaving an invalid index with the migration's name.
					_, err = st.DB().Exec(`CREATE UNIQUE INDEX CONCURRENTLY idx_sync_run_items_local_deletion
						ON sync_run_items(source_message_id, sync_run_id)
						WHERE phase='fetch' AND status='skipped' AND error_kind='acquisition_local_deletion'`)
					require.Error(t, err)
					var invalid bool
					require.NoError(t, st.DB().QueryRow(`SELECT NOT indisvalid FROM pg_index
						WHERE indexrelid='idx_sync_run_items_local_deletion'::regclass`).Scan(&invalid))
					require.True(t, invalid)
				}
				require.NoError(t, st.Close())
				st, err = store.Open(path)
				require.NoError(t, err)
				require.Error(t, st.ValidateSchemaContext(t.Context()))
				require.NoError(t, st.InitSchemaContext(t.Context()))
			}
			for reopen := 0; reopen < 2; reopen++ {
				require.NoError(t, st.Close())
				st, err = store.Open(path)
				require.NoError(t, err)
				require.NoError(t, st.ValidateSchemaContext(t.Context()))
				raw, err := st.GetMessageRaw(archived)
				require.NoError(t, err)
				assert.Equal(t, []byte("synthetic preserved RAW"), raw)
				version, err := st.SchemaVersionContext(t.Context())
				require.NoError(t, err)
				assert.Equal(t, store.SchemaVersion, version)
				var indexes int
				query := "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_sync_run_items_local_deletion'"
				if st.IsPostgreSQL() {
					query = `SELECT count(*) FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
						WHERE c.relnamespace=current_schema()::regnamespace
						AND c.relname='idx_sync_run_items_local_deletion' AND i.indisvalid`
				}
				require.NoError(t, st.DB().QueryRow(query).Scan(&indexes))
				assert.Equal(t, 1, indexes)
				pending, err := st.PendingAcquisitionReplayIDs(source.ID)
				require.NoError(t, err)
				assert.Empty(t, pending)
				pending, err = st.PendingAcquisitionReplayIDs(other.ID)
				require.NoError(t, err)
				assert.Equal(t, []string{"same-native"}, pending)
				var diagnostics int
				require.NoError(t, st.DB().QueryRow("SELECT count(*) FROM sync_run_items WHERE error_message IN ('synthetic intentional deletion','synthetic original diagnostic')").Scan(&diagnostics))
				assert.Equal(t, 3, diagnostics)
				_, err = st.ScopedToSync(source.ID, run).PersistMessage(&store.MessagePersistData{
					Message: &store.Message{SourceID: source.ID, SourceMessageID: "same-native", MessageType: "email"},
					RawMIME: []byte("synthetic raw"), RawFormat: "mime",
				})
				assert.ErrorIs(t, err, store.ErrAcquisitionLocallyDeleted)
				_, err = st.ScopedToSync(other.ID, otherRun).PersistMessage(&store.MessagePersistData{
					Message: &store.Message{
						SourceID: other.ID, SourceMessageID: "same-native",
						ConversationID: conversation, MessageType: "email",
					},
					RawMIME: []byte("synthetic raw"), RawFormat: "mime",
				})
				require.NoError(t, err)
				require.NoError(t, st.InitSchemaContext(t.Context()), "repeated setup must retain a valid, nonunique source-aware index")
			}
		})
	}
}
