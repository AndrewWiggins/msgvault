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
	for _, name := range []string{"direct", "chained-survivor"} {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			f, _ := activityProjectionFixture(t)
			occurredAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
			history := newAttrFixtureOn(t, f.Store, "imap", "imaps://user%40example.test@mail.example.test:993")
			f.Source = history.source
			f.ConvID = history.conv
			destination, err := f.Store.GetOrCreateSource("imap", "imaps://merged%40example.test@mail.example.test:993")
			require.NoError(err)
			messageID := f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
			_, err = activityExec(f.Store, `INSERT INTO imap_message_memberships
		(source_id, mailbox, uidvalidity, uid, message_id, flags)
		VALUES (?, 'Scratch', 7, 1, ?, ?)`, history.source.ID, messageID, `["\\Draft"]`)
			require.NoError(err)
			// Older merges moved conversations and messages, leaving membership rows on the original source.
			_, err = activityExec(f.Store, `INSERT INTO source_merge_messages
		(message_id, original_source_id, original_message_id)
		SELECT id, source_id, source_message_id FROM messages WHERE id = ?`, messageID)
			require.NoError(err)
			_, err = activityExec(f.Store, `UPDATE conversations SET source_id = ? WHERE id = ?`, destination.ID, f.ConvID)
			require.NoError(err)
			_, err = activityExec(f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, destination.ID, messageID)
			require.NoError(err)
			_, err = activityExec(f.Store, `INSERT INTO source_settings(source_id, history_only, merged_into_source_id, retired_at)
		VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)`, history.source.ID, destination.ID)
			require.NoError(err)
			f.Source = destination
			if name == "chained-survivor" {
				survivorID := f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
				_, err = activityExec(f.Store, `UPDATE source_merge_messages SET survivor_message_id = ? WHERE message_id = ?`, survivorID, messageID)
				require.NoError(err)
				_, err = activityExec(f.Store, `UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, messageID)
				require.NoError(err)
				final, err := f.Store.GetOrCreateSource("imap", "imaps://final%40example.test@mail.example.test:993")
				require.NoError(err)
				finalConversation, err := f.Store.EnsureConversation(final.ID, "final-thread", "Thread")
				require.NoError(err)
				f.Source = final
				f.ConvID = finalConversation
				finalSurvivorID := f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
				_, err = activityExec(f.Store, `INSERT INTO source_merge_messages
		(message_id, original_source_id, original_message_id, survivor_message_id)
		SELECT id, source_id, source_message_id, ? FROM messages WHERE id = ?`, finalSurvivorID, survivorID)
				require.NoError(err)
				_, err = activityExec(f.Store, `UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, survivorID)
				require.NoError(err)
				_, err = activityExec(f.Store, `UPDATE messages SET source_id = ?, conversation_id = ? WHERE source_id = ?`, final.ID, finalConversation, destination.ID)
				require.NoError(err)
				_, err = activityExec(f.Store, `INSERT INTO source_settings(source_id, history_only, merged_into_source_id, retired_at)
		VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)`, destination.ID, final.ID)
				require.NoError(err)
				destination = final
				messageID = finalSurvivorID
			}
			_, err = f.Store.RepairAccountAttributionContext(t.Context(), destination.ID, nil)
			require.NoError(err)
			_, path := attribution(t, f.Store, messageID)
			require.Equal("inbound", path.String)
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
		})
	}
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
