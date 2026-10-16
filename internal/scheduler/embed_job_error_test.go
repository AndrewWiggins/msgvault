package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
	"go.kenn.io/msgvault/internal/vector"
)

type convergenceCheckerFunc func(context.Context, vector.GenerationID) (ConvergenceResult, error)

func (f convergenceCheckerFunc) CheckConvergence(ctx context.Context, gen vector.GenerationID) (ConvergenceResult, error) {
	return f(ctx, gen)
}

func TestEmbedJobRunHandlesInterruptedPhases(t *testing.T) {
	failure := errors.New("independent database failure")
	for _, test := range []struct {
		name                      string
		configure                 func(*EmbedJob, *fakeBackend, context.CancelCauseFunc)
		wantErr                   bool
		runs, backstops, coverage int
		activations               []vector.GenerationID
		completeLog               bool
		cooperative               bool
	}{
		{"reclaim", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Worker = &fakeRunner{reclaimErr: failure, yieldReclaim: interrupt}
		}, true, 0, 0, 0, nil, false, false},
		{"backstop", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.BackstopInterval = 0
			job.Worker = &fakeRunner{backstopErr: failure, yieldBackstop: interrupt}
		}, true, 1, 1, 0, nil, true, false},
		{"backstop success", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.BackstopInterval = 0
			job.Worker = &fakeRunner{yieldBackstop: interrupt}
		}, false, 1, 1, 0, nil, true, false},
		{"interrupted RunOnce", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Worker = &fakeRunner{runErr: failure, yieldCancel: interrupt}
		}, true, 1, 0, 0, nil, false, false},
		{"clean RunOnce yield", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Worker = &fakeRunner{yieldCancel: interrupt}
		}, false, 1, 0, 0, nil, false, false},
		{"cooperative RunOnce yield", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Worker = &fakeRunner{yieldCancel: interrupt}
		}, false, 1, 0, 0, nil, false, true},
		{"target building lookup", func(job *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Fingerprint = ""
			backend.buildErr, backend.yieldBuilding = failure, interrupt
		}, true, 0, 0, 0, nil, false, false},
		{"target active lookup", func(job *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Fingerprint = ""
			backend.building = nil
			backend.activeErr, backend.yieldActive = failure, interrupt
		}, true, 0, 0, 0, nil, false, false},
		{"person building lookup", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.buildErr, backend.yieldBuilding = failure, interrupt
		}, true, 0, 0, 0, nil, false, false},
		{"person active lookup", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.activeErr, backend.yieldActive = failure, interrupt
		}, true, 0, 0, 0, nil, false, false},
		{"convergence before backstop", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Convergence = &fakeConvergenceChecker{err: failure, cancel: interrupt}
		}, true, 1, 0, 0, nil, true, false},
		{"convergence after backstop", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.BackstopInterval = 0
			checks := 0
			job.Convergence = convergenceCheckerFunc(func(context.Context, vector.GenerationID) (ConvergenceResult, error) {
				checks++
				if checks == 1 {
					return ConvergenceResult{}, nil
				}
				interrupt(jobctx.ErrYieldedToWaiter)
				return ConvergenceResult{}, failure
			})
		}, true, 1, 1, 0, nil, true, false},
		{"coverage", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Store = &fakeCoverage{err: failure, yieldCancel: interrupt}
		}, true, 1, 0, 1, nil, true, false},
		{"activation", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.activateErr, backend.yieldActivate = failure, interrupt
		}, true, 1, 0, 1, []vector.GenerationID{9}, true, false},
		{"activation success", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.yieldActivate = interrupt
		}, false, 1, 0, 1, []vector.GenerationID{9}, true, false},
		{"sequence-bound activation", func(job *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.activateErr, backend.yieldActivate = failure, interrupt
			job.SequenceBoundActivation = true
			job.Convergence = &fakeConvergenceChecker{result: ConvergenceResult{
				MessageCoverageComplete: true, PersonCoverageComplete: true, ReconciliationComplete: true,
			}}
		}, true, 1, 0, 0, []vector.GenerationID{9}, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			ctx, request := jobctx.WithPreemption(ctx)
			interrupt := cancel
			if test.cooperative {
				interrupt = func(error) { request() }
			}
			backend := &fakeBackend{
				building:  &vector.Generation{ID: 9, State: vector.GenerationBuilding, Fingerprint: "m:768"},
				activeErr: vector.ErrNoActiveGeneration,
			}
			var logs bytes.Buffer
			job := &EmbedJob{
				Log:    slog.New(slog.NewTextHandler(&logs, nil)),
				Worker: &fakeRunner{}, Backend: backend, Store: &fakeCoverage{},
				Fingerprint: "m:768", BackstopInterval: -1,
			}
			test.configure(job, backend, interrupt)
			runner, ok := job.Worker.(*fakeRunner)
			require.True(ok)
			coverage, ok := job.Store.(*fakeCoverage)
			require.True(ok)
			err := job.run(ctx)
			if test.wantErr {
				require.ErrorIs(err, failure)
			} else {
				require.NoError(err)
			}
			_, runs, _ := runner.calls()
			backstops, _ := runner.backstops()
			assert.Equal(test.runs, runs)
			assert.Equal(test.backstops, backstops)
			assert.Equal(test.coverage, coverage.calls)
			assert.Equal(test.activations, backend.activations())
			assert.Equal(test.completeLog, bytes.Contains(logs.Bytes(), []byte("embed run complete")))
			for _, message := range []string{
				"generation lookup failed", "embed reclaim failed", "embed run failed",
				"convergence check after run failed", "embed backstop failed",
				"activation failed", "building generation activated",
			} {
				assert.NotContains(logs.String(), message)
			}
			if test.cooperative {
				assert.True(jobctx.PreemptionRequested(ctx))
				require.NoError(ctx.Err())
			} else {
				require.ErrorIs(context.Cause(ctx), jobctx.ErrYieldedToWaiter)
			}
		})
	}
}
