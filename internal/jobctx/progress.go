package jobctx

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrRunBudgetExceeded is the cause of a scheduled pass's runtime deadline.
var ErrRunBudgetExceeded = errors.New("scheduled job runtime budget exceeded")

type progressKey struct{}

type passProgress struct {
	committed atomic.Bool
	inFlight  atomic.Int64
}

// WithProgress tracks committed checkpoints within one scheduled pass.
func WithProgress(ctx context.Context) context.Context {
	return context.WithValue(ctx, progressKey{}, new(passProgress))
}

// RecordProgress records a committed checkpoint that a later pass can resume.
func RecordProgress(ctx context.Context) {
	if progress, ok := ctx.Value(progressKey{}).(*passProgress); ok {
		progress.committed.Store(true)
	}
}

// HasProgress reports whether this pass committed a checkpoint.
func HasProgress(ctx context.Context) bool {
	progress, ok := ctx.Value(progressKey{}).(*passProgress)
	return ok && progress.committed.Load()
}

// RunBudgetExpired reports either admission-window or hard runtime expiry.
func RunBudgetExpired(ctx context.Context) bool {
	return WorkWindowExpired(ctx) || errors.Is(context.Cause(ctx), ErrRunBudgetExceeded)
}

// BeginAttempt tracks an admitted provider attempt until its response is closed.
func BeginAttempt(ctx context.Context) func() {
	if progress, ok := ctx.Value(progressKey{}).(*passProgress); ok {
		progress.inFlight.Add(1)
		return func() { progress.inFlight.Add(-1) }
	}
	return func() {}
}

// HasInFlightAttempt reports whether this pass is awaiting a provider response.
func HasInFlightAttempt(ctx context.Context) bool {
	progress, ok := ctx.Value(progressKey{}).(*passProgress)
	return ok && progress.inFlight.Load() > 0
}
