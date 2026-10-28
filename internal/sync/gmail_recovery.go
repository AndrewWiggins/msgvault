package sync

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	"go.kenn.io/msgvault/internal/store"
)

const (
	recoveryCheckpointPrefix = "gmail-recovery:v1:"
	recoveryEnumerate        = "enumerate"
	recoveryReconcile        = "reconcile"
	recoveryCatchup          = "catchup"
)

// recoveryCheckpoint is the durable Gmail recovery protocol in sync_runs:
// cursor_before holds this versioned phase envelope, including on first/final
// pages; cursor_after holds H, pinned before enumeration and inherited on every
// retry. Phase, listing token and H are checkpointed atomically.
//
// enumerate resumes messages.list from PageToken. reconcile collects a fresh,
// complete presence inventory, then commits tombstones in one transaction.
// The catchup checkpoint is a separate commit: interruption in that gap repeats
// the idempotent reconciliation. Once catchup is durable, retries skip listings
// and restart History from H. History page tokens stay in memory, so every retry
// replays changes since H through idempotent message and tombstone writes.
//
// Catch-up completion publishes the new source cursor and completes the run
// atomically; publication has no separate resumable phase. Neither enumeration
// nor reconciliation publishes a source cursor. See initHistoryRecoveryState
// for legacy checkpoints that must restart enumeration while preserving H.
type recoveryCheckpoint struct {
	Phase     string `json:"phase"`
	PageToken string `json:"page_token,omitempty"`
}

func (state *syncState) durableCheckpoint() *store.Checkpoint {
	cp := *state.checkpoint
	if state.recoveryPhase != "" {
		encoded, _ := json.Marshal(recoveryCheckpoint{Phase: state.recoveryPhase, PageToken: cp.PageToken})
		cp.PageToken = recoveryCheckpointPrefix + string(encoded)
		cp.HandoffCursor = state.handoffCursor
		cp.InheritFromSyncID = state.predecessorID
	}
	return &cp
}

func (s *Syncer) saveFullCheckpoint(ctx context.Context, state *syncState) error {
	return s.store.UpdateSyncCheckpointContext(ctx, state.syncID, state.durableCheckpoint())
}

func restoreRecoveryCheckpoint(state *syncState, run *store.SyncRun) error {
	state.recoveryPhase = recoveryEnumerate
	if !checkpointMatchesRequest(run, gmailHistoryRecoveryCoverageFingerprint) {
		return nil // The narrower legacy listing must restart, keeping H.
	}
	token := run.CursorBefore.String
	if strings.HasPrefix(token, "gmail-recovery:") && !strings.HasPrefix(token, recoveryCheckpointPrefix) {
		return errors.New("unsupported Gmail recovery checkpoint version")
	}
	if strings.HasPrefix(token, recoveryCheckpointPrefix) {
		var cp recoveryCheckpoint
		if err := json.Unmarshal([]byte(strings.TrimPrefix(token, recoveryCheckpointPrefix)), &cp); err != nil {
			return fmt.Errorf("decode Gmail recovery checkpoint: %w", err)
		}
		switch cp.Phase {
		case recoveryEnumerate, recoveryReconcile, recoveryCatchup:
		default:
			return fmt.Errorf("unknown Gmail recovery phase %q", cp.Phase)
		}
		if cp.Phase != recoveryEnumerate && cp.PageToken != "" {
			return fmt.Errorf("unexpected page token in Gmail recovery phase %q", cp.Phase)
		}
		state.recoveryPhase, token = cp.Phase, cp.PageToken
	}
	state.pageToken = token
	state.checkpoint = &store.Checkpoint{
		PageToken:         token,
		MessagesProcessed: run.MessagesProcessed,
		MessagesAdded:     run.MessagesAdded,
		MessagesUpdated:   run.MessagesUpdated,
		ErrorsCount:       run.ErrorsCount,
	}
	return nil
}
