package store_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

func TestSchemaVersionMigrationFailure(t *testing.T) {
	for _, name := range []string{"v13-seed", "experimental-v14-seed", "official-v14-marker"} {
		t.Run(name, func(t *testing.T) {
			st, path := reopenableAcquisitionStore(t)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			uid, err := st.ArchiveUIDContext(t.Context())
			require.NoError(t, err)
			source, err := st.GetOrCreateSource("gmail", "schema-failure@example.test")
			require.NoError(t, err)
			conversation, err := st.EnsureConversation(source.ID, "fixture", "Synthetic migration")
			require.NoError(t, err)
			message, err := st.PersistMessage(&store.MessagePersistData{
				Message: &store.Message{SourceID: source.ID, SourceMessageID: "preserved", ConversationID: conversation, MessageType: "email"},
				RawMIME: []byte("synthetic migration RAW"), RawFormat: "mime",
			})
			require.NoError(t, err)
			version := 14
			seedFailure := name != "official-v14-marker"
			if name == "v13-seed" {
				version = 13
			}
			if seedFailure {
				_, err = st.DB().Exec(`DELETE FROM applied_migrations WHERE name='communication_services_inline_v1'`)
				require.NoError(t, err)
				_, err = st.DB().Exec(`DELETE FROM communication_services WHERE slug='inline'`)
				require.NoError(t, err)
			}
			if name != "experimental-v14-seed" {
				_, err = st.DB().Exec(`DROP INDEX idx_sync_run_items_local_deletion`)
				require.NoError(t, err)
			}
			_, err = st.DB().Exec(st.Rebind(`UPDATE archive_metadata SET value=? WHERE key='schema_version'`), strconv.Itoa(version))
			require.NoError(t, err)

			// Fail the ledger insert after Inline has been inserted in its
			// transaction, or fail the final completion marker after setup.
			// A retry must preserve the archive, not certify a partial upgrade.
			table, condition := "archive_metadata", "NEW.key='schema_version' AND NEW.value='15'"
			if seedFailure {
				table, condition = "applied_migrations", "NEW.name='communication_services_inline_v1'"
			}
			if st.IsPostgreSQL() {
				_, err = st.DB().Exec(`CREATE FUNCTION fail_schema_upgrade() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF ` + condition + ` THEN RAISE EXCEPTION 'synthetic migration failure'; END IF; RETURN NEW; END $$;
CREATE TRIGGER fail_schema_upgrade BEFORE INSERT OR UPDATE ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION fail_schema_upgrade()`)
			} else {
				event := "UPDATE"
				if seedFailure {
					event = "INSERT"
				}
				_, err = st.DB().Exec(`CREATE TRIGGER fail_schema_upgrade BEFORE ` + event + ` ON ` + table + ` WHEN ` + condition + ` BEGIN SELECT RAISE(ABORT, 'synthetic migration failure'); END`)
			}
			require.NoError(t, err)
			for retry := 0; retry < 2; retry++ {
				require.NoError(t, st.Close())
				st, err = store.Open(path)
				require.NoError(t, err)
				require.ErrorContains(t, st.InitSchemaContext(t.Context()), "synthetic migration failure")
				require.ErrorContains(t, st.ValidateSchemaContext(t.Context()), "run setup")
				got, err := st.SchemaVersionContext(t.Context())
				require.NoError(t, err)
				assert.Equal(t, version, got)
				raw, err := st.GetMessageRaw(message)
				require.NoError(t, err)
				assert.Equal(t, []byte("synthetic migration RAW"), raw)
				if seedFailure {
					var services, ledger int
					require.NoError(t, st.DB().QueryRow(`SELECT count(*) FROM communication_services WHERE slug='inline'`).Scan(&services))
					require.NoError(t, st.DB().QueryRow(`SELECT count(*) FROM applied_migrations WHERE name='communication_services_inline_v1'`).Scan(&ledger))
					assert.Zero(t, services, "service insert must roll back with its failed ledger insert")
					assert.Zero(t, ledger)
				}
			}
			drop := "DROP TRIGGER fail_schema_upgrade"
			if st.IsPostgreSQL() {
				drop += " ON " + table + "; DROP FUNCTION fail_schema_upgrade()"
			}
			_, err = st.DB().Exec(drop)
			require.NoError(t, err)
			require.NoError(t, st.InitSchemaContext(t.Context()))
			require.NoError(t, st.ValidateSchemaContext(t.Context()))
			assertCombinedSchemaObjects(t, st)
			again, err := st.ArchiveUIDContext(t.Context())
			require.NoError(t, err)
			assert.Equal(t, uid, again)
			raw, err := st.GetMessageRaw(message)
			require.NoError(t, err)
			assert.Equal(t, []byte("synthetic migration RAW"), raw)
		})
	}
}

func TestSchemaVersionRejectsUnknownPreservesArchive(t *testing.T) {
	st, path := reopenableAcquisitionStore(t)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	source, err := st.GetOrCreateSource("gmail", "unknown-version@example.test")
	require.NoError(t, err)
	conversation, err := st.EnsureConversation(source.ID, "fixture", "Synthetic archive")
	require.NoError(t, err)
	id, err := st.PersistMessage(&store.MessagePersistData{
		Message: &store.Message{SourceID: source.ID, SourceMessageID: "preserved", ConversationID: conversation, MessageType: "email"},
		RawMIME: []byte("synthetic version RAW"), RawFormat: "mime",
	})
	require.NoError(t, err)
	uid, err := st.ArchiveUIDContext(t.Context())
	require.NoError(t, err)
	digest := func() string {
		if st.IsPostgreSQL() {
			return schemaContractPostgresDigest(t, st)
		}
		return schemaContractSQLiteDigest(t, st)
	}
	before := digest()
	for _, value := range []string{"fourteen", "0", "-1", strconv.Itoa(store.SchemaVersion + 1)} {
		_, err = st.DB().Exec(st.Rebind(`UPDATE archive_metadata SET value=? WHERE key='schema_version'`), value)
		require.NoError(t, err)
		require.NoError(t, st.Close())
		st, err = store.Open(path)
		require.NoError(t, err)
		require.Error(t, st.InitSchemaContext(t.Context()))
		require.Error(t, st.ValidateSchemaContext(t.Context()))
		assert.Equal(t, before, digest(), "refusal must leave SQL objects and migration ledger unchanged")
		raw, err := st.GetMessageRaw(id)
		require.NoError(t, err)
		assert.Equal(t, []byte("synthetic version RAW"), raw)
		again, err := st.ArchiveUIDContext(t.Context())
		require.NoError(t, err)
		assert.Equal(t, uid, again)
		var marker string
		require.NoError(t, st.DB().QueryRow(`SELECT value FROM archive_metadata WHERE key='schema_version'`).Scan(&marker))
		assert.Equal(t, value, marker)
	}
}
