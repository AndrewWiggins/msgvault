package sync

import (
	"context"
	"errors"
	"fmt"

	"go.kenn.io/msgvault/internal/gmail"
	"go.kenn.io/msgvault/internal/store"
)

const (
	syncItemPhaseFetch  = "fetch"
	syncItemPhaseIngest = "ingest"
	syncItemPhaseDelete = "delete"

	syncItemKindBatchFetchError = "batch_fetch_error"
	syncItemKindFetchError      = "fetch_error"
	syncItemKindGmailNotFound   = "gmail_not_found"
	syncItemKindMessageGone     = "message_gone"
	syncItemKindIngestError     = "ingest_error"
	syncItemKindDeleteError     = "delete_error"
)

var errRawBatchMissing = errors.New("missing raw message in batch result")

type rawBatchWithErrors interface {
	GetMessagesRawBatchWithErrors(ctx context.Context, messageIDs []string) ([]gmail.RawMessageBatchResult, error)
}

type rawBatchWithIdentityValidation interface {
	GetMessagesRawBatchWithIdentityValidation(ctx context.Context, messageIDs []string) ([]gmail.RawMessageBatchResult, error)
}

func (s *Syncer) getMessagesRawBatchWithDiagnostics(ctx context.Context, messageIDs []string) ([]gmail.RawMessageBatchResult, error) {
	if client, ok := s.client.(rawBatchWithErrors); ok {
		return client.GetMessagesRawBatchWithErrors(ctx, messageIDs)
	}

	rawMessages, err := s.client.GetMessagesRawBatch(ctx, messageIDs)
	if err != nil {
		return nil, err
	}

	results := make([]gmail.RawMessageBatchResult, len(messageIDs))
	for i, id := range messageIDs {
		var raw *gmail.RawMessage
		if i < len(rawMessages) {
			raw = rawMessages[i]
		}
		results[i] = gmail.RawMessageBatchResult{
			ID:      id,
			Message: raw,
		}
		if raw == nil {
			results[i].Err = errRawBatchMissing
		}
	}
	return results, nil
}

// getMessagesRawBatchWithIdentityValidation fetches validationIDs first with
// full raw MIME so their Message-IDs turn later ordinary copies into dedup
// stubs. Results keep the order of messageIDs.
func (s *Syncer) getMessagesRawBatchWithIdentityValidation(
	ctx context.Context,
	messageIDs []string,
	validationIDs map[string]inconclusiveLabelRefresh,
) ([]gmail.RawMessageBatchResult, error) {
	client, ok := s.client.(rawBatchWithIdentityValidation)
	if !ok || len(validationIDs) == 0 {
		return s.getMessagesRawBatchWithDiagnostics(ctx, messageIDs)
	}

	var requiredIDs, ordinaryIDs []string
	var requiredIdx, ordinaryIdx []int
	for i, id := range messageIDs {
		if _, needsValidation := validationIDs[id]; needsValidation {
			requiredIDs = append(requiredIDs, id)
			requiredIdx = append(requiredIdx, i)
		} else {
			ordinaryIDs = append(ordinaryIDs, id)
			ordinaryIdx = append(ordinaryIdx, i)
		}
	}

	results := make([]gmail.RawMessageBatchResult, len(messageIDs))
	required, err := client.GetMessagesRawBatchWithIdentityValidation(ctx, requiredIDs)
	if err != nil {
		return nil, err
	}
	for i, result := range required {
		results[requiredIdx[i]] = result
	}
	if len(ordinaryIDs) == 0 {
		return results, nil
	}
	ordinary, err := s.getMessagesRawBatchWithDiagnostics(ctx, ordinaryIDs)
	if err != nil {
		return nil, err
	}
	for i, result := range ordinary {
		results[ordinaryIdx[i]] = result
	}
	return results, nil
}

// pendingFetchReplayIDs includes ingestion and full-run failures. Neither a
// newer run nor a bounded full traversal can erase another run's debt.
func (s *Syncer) pendingFetchReplayIDs(sourceID int64) ([]string, error) {
	return s.store.PendingAcquisitionReplayIDs(sourceID)
}

// requireRecordedSyncItems prevents a lost diagnostic from being passed by a
// durable page checkpoint or source history cursor. The previous traversal can
// then discover the failed acquisition again.
func (s *Syncer) requireRecordedSyncItems(syncID int64) error {
	if s.itemRecordError == nil {
		return nil
	}
	err := fmt.Errorf("persist acquisition diagnostics: %w", s.itemRecordError)
	s.failStoppedSync(syncID, err)
	return err
}

func isMessageGone(err error) bool {
	return errors.Is(err, gmail.ErrMessageGone)
}

// messageGoneKind reports whether the source explicitly confirmed that the
// message disappeared between enumeration and fetch. A generic Gmail 404 only
// proves that the API returned not found; it does not establish permanent
// deletion, so it remains retryable acquisition debt.
func messageGoneKind(err error) (string, bool) {
	switch {
	case errors.Is(err, gmail.ErrMessageGone):
		return syncItemKindMessageGone, true
	default:
		return "", false
	}
}

func syncItemErrorMessage(err error, fallback string) string {
	if err != nil {
		return err.Error()
	}
	return fallback
}

func (s *Syncer) recordSyncItem(syncID int64, sourceMessageID, phase, status, kind string, err error) {
	if sourceMessageID == "" {
		sourceMessageID = "(unknown)"
	}
	errMsg := syncItemErrorMessage(err, kind)
	if recErr := s.store.RecordSyncRunItem(store.SyncRunItem{
		SyncRunID:       syncID,
		SourceMessageID: sourceMessageID,
		Phase:           phase,
		Status:          status,
		ErrorKind:       kind,
		ErrorMessage:    errMsg,
	}); recErr != nil {
		s.itemRecordError = errors.Join(s.itemRecordError, recErr)
		s.logger.Warn("failed to record sync item",
			"sync_id", syncID,
			"source_message_id", sourceMessageID,
			"phase", phase,
			"status", status,
			"error_kind", kind,
			"error", recErr,
		)
	}
}
