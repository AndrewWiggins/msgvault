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
			must := require.New(t)
			checks := assert.New(t)
			st, path := reopenableAcquisitionStore(t)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			uid, err := st.ArchiveUIDContext(t.Context())
			must.NoError(err)
			source, err := st.GetOrCreateSource("gmail", "schema-failure@example.test")
			must.NoError(err)
			conversation, err := st.EnsureConversation(source.ID, "fixture", "Synthetic migration")
			must.NoError(err)
			message, err := st.PersistMessage(&store.MessagePersistData{
				Message: &store.Message{SourceID: source.ID, SourceMessageID: "preserved", ConversationID: conversation, MessageType: "email"},
				RawMIME: []byte("synthetic migration RAW"), RawFormat: "mime",
			})
			must.NoError(err)
			version := 14
			seedFailure := name != "official-v14-marker"
			if name == "v13-seed" {
				version = 13
			}
			if seedFailure {
				_, err = st.DB().Exec(`DELETE FROM applied_migrations WHERE name='communication_services_inline_v1'`)
				must.NoError(err)
				_, err = st.DB().Exec(`DELETE FROM communication_services WHERE slug='inline'`)
				must.NoError(err)
			}
			if name != "experimental-v14-seed" {
				_, err = st.DB().Exec(`DROP INDEX idx_sync_run_items_local_deletion`)
				must.NoError(err)
			}
			_, err = st.DB().Exec(st.Rebind(`UPDATE archive_metadata SET value=? WHERE key='schema_version'`), strconv.Itoa(version))
			must.NoError(err)

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
			must.NoError(err)
			for range 2 {
				must.NoError(st.Close())
				st, err = store.Open(path)
				must.NoError(err)
				must.ErrorContains(st.InitSchemaContext(t.Context()), "synthetic migration failure")
				must.ErrorContains(st.ValidateSchemaContext(t.Context()), "run setup")
				got, err := st.SchemaVersionContext(t.Context())
				must.NoError(err)
				checks.Equal(version, got)
				raw, err := st.GetMessageRaw(message)
				must.NoError(err)
				checks.Equal([]byte("synthetic migration RAW"), raw)
				if seedFailure {
					var services, ledger int
					must.NoError(st.DB().QueryRow(`SELECT count(*) FROM communication_services WHERE slug='inline'`).Scan(&services))
					must.NoError(st.DB().QueryRow(`SELECT count(*) FROM applied_migrations WHERE name='communication_services_inline_v1'`).Scan(&ledger))
					checks.Zero(services, "service insert must roll back with its failed ledger insert")
					checks.Zero(ledger)
				}
			}
			drop := "DROP TRIGGER fail_schema_upgrade"
			if st.IsPostgreSQL() {
				drop += " ON " + table + "; DROP FUNCTION fail_schema_upgrade()"
			}
			_, err = st.DB().Exec(drop)
			must.NoError(err)
			must.NoError(st.InitSchemaContext(t.Context()))
			must.NoError(st.ValidateSchemaContext(t.Context()))
			assertCombinedSchemaObjects(t, st)
			again, err := st.ArchiveUIDContext(t.Context())
			must.NoError(err)
			checks.Equal(uid, again)
			raw, err := st.GetMessageRaw(message)
			must.NoError(err)
			checks.Equal([]byte("synthetic migration RAW"), raw)
		})
	}
}

func TestSchemaVersionRejectsUnknownPreservesArchive(t *testing.T) {
	must := require.New(t)
	checks := assert.New(t)
	st, path := reopenableAcquisitionStore(t)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	source, err := st.GetOrCreateSource("gmail", "unknown-version@example.test")
	must.NoError(err)
	conversation, err := st.EnsureConversation(source.ID, "fixture", "Synthetic archive")
	must.NoError(err)
	id, err := st.PersistMessage(&store.MessagePersistData{
		Message: &store.Message{SourceID: source.ID, SourceMessageID: "preserved", ConversationID: conversation, MessageType: "email"},
		RawMIME: []byte("synthetic version RAW"), RawFormat: "mime",
	})
	must.NoError(err)
	uid, err := st.ArchiveUIDContext(t.Context())
	must.NoError(err)
	digest := func() string {
		if st.IsPostgreSQL() {
			return schemaContractPostgresDigest(t, st)
		}
		return schemaContractSQLiteDigest(t, st)
	}
	before := digest()
	for _, value := range []string{"fourteen", "0", "-1", strconv.Itoa(store.SchemaVersion + 1)} {
		_, err = st.DB().Exec(st.Rebind(`UPDATE archive_metadata SET value=? WHERE key='schema_version'`), value)
		must.NoError(err)
		must.NoError(st.Close())
		st, err = store.Open(path)
		must.NoError(err)
		must.Error(st.InitSchemaContext(t.Context()))
		must.Error(st.ValidateSchemaContext(t.Context()))
		checks.Equal(before, digest(), "refusal must leave SQL objects and migration ledger unchanged")
		raw, err := st.GetMessageRaw(id)
		must.NoError(err)
		checks.Equal([]byte("synthetic version RAW"), raw)
		again, err := st.ArchiveUIDContext(t.Context())
		must.NoError(err)
		checks.Equal(uid, again)
		var marker string
		must.NoError(st.DB().QueryRow(`SELECT value FROM archive_metadata WHERE key='schema_version'`).Scan(&marker))
		checks.Equal(value, marker)
	}
}
