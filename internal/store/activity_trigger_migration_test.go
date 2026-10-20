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
	for _, mergeKind := range []string{"direct", "chained", "survivor"} {
		t.Run(mergeKind, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			f, personID := activityProjectionFixture(t)
			_, err := activityExec(f.Store, `UPDATE sources SET source_type = 'imap' WHERE id = ?`, f.Source.ID)
			require.NoError(err)
			destination, err := f.Store.GetOrCreateSource("imap", "merged@example.invalid")
			require.NoError(err)
			occurredAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
			messageID := f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
			_, err = activityExec(f.Store, `INSERT INTO imap_message_memberships
		(source_id, mailbox, uidvalidity, uid, message_id, flags)
		VALUES (?, 'Scratch', 7, 1, ?, ?)`, f.Source.ID, messageID, `["\\Draft"]`)
			require.NoError(err)
			// Older merges moved messages and kept membership rows on the original source.
			_, err = activityExec(f.Store, `INSERT INTO source_merge_messages
		(message_id, original_source_id, original_message_id)
		SELECT id, source_id, source_message_id FROM messages WHERE id = ?`, messageID)
			require.NoError(err)
			_, err = activityExec(f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, destination.ID, messageID)
			require.NoError(err)
			if mergeKind == "survivor" {
				historicalID := messageID
				messageID = f.NewMessage().WithSentAt(occurredAt).Create(t, f.Store)
				_, err = activityExec(f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, destination.ID, messageID)
				require.NoError(err)
				_, err = activityExec(f.Store, `UPDATE source_merge_messages SET survivor_message_id = ? WHERE message_id = ?`, messageID, historicalID)
				require.NoError(err)
				_, err = activityExec(f.Store, `UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`, historicalID)
				require.NoError(err)
				// The older daemon already processed the hidden historical row.
				_, err = activityExec(f.Store, `DELETE FROM activity_projection_queue WHERE message_id = ?`, historicalID)
				require.NoError(err)
			}
			_, err = activityExec(f.Store, `INSERT INTO source_settings(source_id, history_only, merged_into_source_id, retired_at)
		VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)`, f.Source.ID, destination.ID)
			require.NoError(err)
			if mergeKind == "chained" {
				intermediate := destination
				destination, err = f.Store.GetOrCreateSource("imap", "final@example.invalid")
				require.NoError(err)
				_, err = activityExec(f.Store, `UPDATE messages SET source_id = ? WHERE id = ?`, destination.ID, messageID)
				require.NoError(err)
				_, err = activityExec(f.Store, `INSERT INTO source_settings(source_id, history_only, merged_into_source_id, retired_at)
			VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)`, intermediate.ID, destination.ID)
				require.NoError(err)
			}
			projection := activityProjectionForMessage(t, f, messageID, personID, occurredAt)
			require.False(projection.Token.Unsent, "the old merge hid the only draft evidence")
			_, err = f.Store.ProjectActivityBatchContext(t.Context(), []store.ActivityProjection{projection})
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
			assert.True(queued[0].Unsent)
			assert.False(queued[0].Eligible)
			retraction := activityProjectionFromCandidate(t, queued[0], personID, occurredAt)
			retraction.Event = nil
			_, err = f.Store.ProjectActivityBatchContext(t.Context(), []store.ActivityProjection{retraction})
			require.NoError(err)
			state, err := f.Store.ContactStateContext(t.Context(), personID, occurredAt)
			require.NoError(err)
			assert.Zero(state.InteractionCount)
			assert.Nil(state.LastContactAt)
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
