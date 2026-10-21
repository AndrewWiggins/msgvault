package store_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil/storetest"
)

func TestActivityTriggerMigrationRequeuesProjectedUnsentMessages(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	f, personID := activityProjectionFixture(t)
	occurredAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	messageID := f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
	projection := activityProjectionForMessage(t, f, messageID, personID, occurredAt)
	_, err := f.Store.ProjectActivityBatchContext(t.Context(), []store.ActivityProjection{projection})
	require.NoError(err)
	labels := f.EnsureLabels(map[string]string{"DRAFT": "Drafts"}, "system")
	// Bypass attribution hooks to reproduce a draft projected by an older daemon.
	_, err = f.Store.DB().Exec(f.Store.Rebind(`INSERT INTO message_labels (message_id, label_id) VALUES (?, ?)`), messageID, labels["DRAFT"])
	require.NoError(err)
	queued, err := f.Store.LoadQueuedActivityCandidatesContext(t.Context(), 10)
	require.NoError(err)
	require.Empty(queued)
	_, err = f.Store.DB().Exec(`DELETE FROM applied_migrations WHERE name = 'activity_projection_triggers_v5'`)
	require.NoError(err)
	require.NoError(f.Store.InitSchema())
	queued, err = f.Store.LoadQueuedActivityCandidatesContext(t.Context(), 10)
	require.NoError(err)
	require.Len(queued, 1)
	assert.Equal(messageID, queued[0].MessageID)
	assert.False(queued[0].Eligible)
}

func TestActivityTriggerMigrationRequeuesMergedIMAPDraft(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup []func(*mergedIMAPDraftFixture, *testing.T)
	}{
		{name: "direct", setup: []func(*mergedIMAPDraftFixture, *testing.T){
			(*mergedIMAPDraftFixture).retireOriginalSource,
		}},
		{name: "chained", setup: []func(*mergedIMAPDraftFixture, *testing.T){
			(*mergedIMAPDraftFixture).retireOriginalSource,
			(*mergedIMAPDraftFixture).chainDestination,
		}},
		{name: "survivor", setup: []func(*mergedIMAPDraftFixture, *testing.T){
			(*mergedIMAPDraftFixture).addSurvivor,
			(*mergedIMAPDraftFixture).retireOriginalSource,
		}},
		{name: "chained-survivor", setup: []func(*mergedIMAPDraftFixture, *testing.T){
			(*mergedIMAPDraftFixture).addSurvivor,
			(*mergedIMAPDraftFixture).prepareFinalSurvivor,
			(*mergedIMAPDraftFixture).retireOriginalSource,
			(*mergedIMAPDraftFixture).linkFinalSurvivor,
			(*mergedIMAPDraftFixture).chainDestination,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			f, personID := activityProjectionFixture(t)
			occurredAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
			draft := newMergedIMAPDraftFixture(t, f, occurredAt)
			for _, setup := range tc.setup {
				setup(draft, t)
			}
			destination, messageID := draft.destination, draft.messageID
			_, err := f.Store.EnsureLabelsBatch(destination.ID, map[string]store.LabelInfo{
				"Sent": {Name: "Sent", Type: "system", SystemRole: store.LabelSystemRoleSent},
			})
			require.NoError(err)
			_, err = f.Store.RepairAccountAttributionContext(t.Context(), destination.ID, nil)
			require.NoError(err)
			_, path := attribution(t, f.Store, messageID)
			require.Equal("inbound", path.String)
			// The older daemon already processed the hidden historical rows.
			_, err = activityExec(f.Store, `DELETE FROM activity_projection_queue
		WHERE message_id IN (SELECT id FROM messages WHERE source_id = ? AND deleted_at IS NOT NULL)`, destination.ID)
			require.NoError(err)
			projection := activityProjectionForMessage(t, f, messageID, personID, occurredAt)
			require.False(projection.Token.Unsent, "the old merge hid the only draft evidence")
			_, err = f.Store.ProjectActivityBatchContext(t.Context(), []store.ActivityProjection{projection})
			require.NoError(err)
			queued, err := f.Store.LoadQueuedActivityCandidatesContext(t.Context(), 10)
			require.NoError(err)
			require.Empty(queued)
			_, err = f.Store.DB().Exec(`DELETE FROM applied_migrations WHERE name = 'activity_projection_triggers_v5'`)
			require.NoError(err)
			revisionBefore, err := f.Store.DerivedDataRevisionContext(t.Context())
			require.NoError(err)
			require.NoError(f.Store.InitSchema())
			revisionAfter, err := f.Store.DerivedDataRevisionContext(t.Context())
			require.NoError(err)
			assert.Greater(revisionAfter, revisionBefore, "backfilling merged IMAP draft labels must advance the derived-data revision")
			_, path = attribution(t, f.Store, messageID)
			assert.NotEqual("inbound", path.String, "preserved drafts must leave received-mail attribution")
			queued, err = f.Store.LoadQueuedActivityCandidatesContext(t.Context(), 10)
			require.NoError(err)
			require.Len(queued, 1)
			assert.Equal(messageID, queued[0].MessageID)
			assert.True(queued[0].Unsent)
			assert.False(queued[0].Eligible)
		})
	}
}

type mergedIMAPDraftFixture struct {
	f               *storetest.Fixture
	occurredAt      time.Time
	destination     *store.Source
	messageID       int64
	finalSurvivorID int64
}

func newMergedIMAPDraftFixture(t *testing.T, f *storetest.Fixture, occurredAt time.Time) *mergedIMAPDraftFixture {
	t.Helper()
	history := newAttrFixtureOn(t, f.Store, "imap", "imaps://user%40example.test@mail.example.test:993")
	f.Source = history.source
	f.ConvID = history.conv
	destination, err := f.Store.GetOrCreateSource("imap", "imaps://merged%40example.test@mail.example.test:993")
	require.NoError(t, err)
	messageID := f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
	_, err = activityExec(f.Store, `INSERT INTO imap_message_memberships
		(source_id, mailbox, uidvalidity, uid, message_id, flags)
		VALUES (?, 'Scratch', 7, 1, ?, ?)`, f.Source.ID, messageID, `["\\Draft"]`)
	require.NoError(t, err)
	// Older merges moved messages and kept membership rows on the original source.
	_, err = activityExec(f.Store, `INSERT INTO source_merge_messages
		(message_id, original_source_id, original_message_id)
		SELECT id, source_id, source_message_id FROM messages WHERE id = ?`, messageID)
	require.NoError(t, err)
	_, err = activityExec(f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, destination.ID, messageID)
	require.NoError(t, err)
	return &mergedIMAPDraftFixture{f: f, occurredAt: occurredAt, destination: destination, messageID: messageID}
}

func (d *mergedIMAPDraftFixture) addSurvivor(t *testing.T) {
	t.Helper()
	survivorID := d.f.NewMessage().WithSentAt(d.occurredAt).Create(t, d.f.Store)
	_, err := activityExec(d.f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, d.destination.ID, survivorID)
	require.NoError(t, err, "move survivor to destination")
	_, err = activityExec(d.f.Store, `UPDATE source_merge_messages SET survivor_message_id = ? WHERE message_id = ?`, survivorID, d.messageID)
	require.NoError(t, err, "link historical message to survivor")
	_, err = activityExec(d.f.Store, `UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, d.messageID)
	require.NoError(t, err, "hide historical message")
	d.messageID = survivorID
}

func (d *mergedIMAPDraftFixture) prepareFinalSurvivor(t *testing.T) {
	t.Helper()
	d.finalSurvivorID = d.f.NewMessage().WithSentAt(d.occurredAt).Create(t, d.f.Store)
}

func (d *mergedIMAPDraftFixture) retireOriginalSource(t *testing.T) {
	t.Helper()
	_, err := activityExec(d.f.Store, `INSERT INTO source_settings(source_id, history_only, merged_into_source_id, retired_at)
		VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)`, d.f.Source.ID, d.destination.ID)
	require.NoError(t, err, "retire original draft source")
}

func (d *mergedIMAPDraftFixture) linkFinalSurvivor(t *testing.T) {
	t.Helper()
	final, err := d.f.Store.GetOrCreateSource("imap", "imaps://final%40example.test@mail.example.test:993")
	require.NoError(t, err, "create final survivor source")
	_, err = activityExec(d.f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, final.ID, d.finalSurvivorID)
	require.NoError(t, err, "move final survivor to destination")
	_, err = activityExec(d.f.Store, `INSERT INTO source_merge_messages
		(message_id, original_source_id, original_message_id, survivor_message_id)
		SELECT id, source_id, source_message_id, ? FROM messages WHERE id = ?`, d.finalSurvivorID, d.messageID)
	require.NoError(t, err, "record chained survivor provenance")
	_, err = activityExec(d.f.Store, `UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, d.messageID)
	require.NoError(t, err, "hide intermediate survivor")
	d.messageID = d.finalSurvivorID
}

func (d *mergedIMAPDraftFixture) chainDestination(t *testing.T) {
	t.Helper()
	intermediate := d.destination
	final, err := d.f.Store.GetOrCreateSource("imap", "imaps://final%40example.test@mail.example.test:993")
	require.NoError(t, err, "create final source")
	_, err = activityExec(d.f.Store, `UPDATE messages SET source_id = ? WHERE source_id = ?`, final.ID, intermediate.ID)
	require.NoError(t, err, "move intermediate messages to final source")
	_, err = activityExec(d.f.Store, `INSERT INTO source_settings(source_id, history_only, merged_into_source_id, retired_at)
		VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)`, intermediate.ID, final.ID)
	require.NoError(t, err, "retire intermediate source")
	d.destination = final
}

// The helper initializes an archive through the combined message and attachment
// trigger migration, then this test proves that activity triggers are installed.
func TestPostgresActivityTriggersInstallAfterCombinedTriggerMigration(t *testing.T) {
	dbURL := os.Getenv("MSGVAULT_TEST_DB")
	if !strings.HasPrefix(dbURL, "postgres://") &&
		!strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL trigger migration regression")
	}
	require := require.New(t)
	assert := assert.New(t)
	st, _, _ := newActivityQueryPostgresStores(t, dbURL)

	var watermarkMigrationApplied bool
	err := st.DB().QueryRowContext(t.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM applied_migrations
			WHERE name = 'message_and_attachment_triggers_v12'
		)
	`).Scan(&watermarkMigrationApplied)
	require.NoError(err)
	assert.True(watermarkMigrationApplied)

	source, err := st.GetOrCreateSource("gmail", "activity-trigger@example.invalid")
	require.NoError(err)
	conversationID, err := st.EnsureConversation(
		source.ID, "activity-trigger-thread", "Activity trigger")
	require.NoError(err)
	messageID, err := st.UpsertMessage(&store.Message{
		ConversationID:  conversationID,
		SourceID:        source.ID,
		SourceMessageID: "activity-trigger-message",
		MessageType:     "email",
		SentAt:          sql.NullTime{Valid: false},
	})
	require.NoError(err)

	var revision int64
	//nolint:gosec // The query is a fixed test literal; Rebind only changes placeholders.
	err = st.DB().QueryRowContext(t.Context(), st.Rebind(`
		SELECT revision
		FROM activity_projection_queue
		WHERE message_id = ?
	`), messageID).Scan(&revision)
	require.NoError(err,
		"activity trigger migration must install queue triggers on an upgraded archive")
	assert.Equal(int64(1), revision)
}
