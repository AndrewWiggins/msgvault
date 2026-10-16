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
		name      string
		configure func(*EmbedJob, *fakeBackend, context.CancelCauseFunc)
	}{
		{"reclaim", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Worker = &fakeRunner{reclaimErr: failure, yieldReclaim: interrupt}
		}},
		{"backstop", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.BackstopInterval = 0
			job.Worker = &fakeRunner{backstopErr: failure, yieldBackstop: interrupt}
		}},
		{"backstop success", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.BackstopInterval = 0
			job.Worker = &fakeRunner{yieldBackstop: interrupt}
		}},
		{"interrupted RunOnce", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Worker = &fakeRunner{runErr: failure, yieldCancel: interrupt}
		}},
		{"target building lookup", func(job *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Fingerprint = ""
			backend.buildErr, backend.yieldBuilding = failure, interrupt
		}},
		{"target active lookup", func(job *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Fingerprint = ""
			backend.building = nil
			backend.activeErr, backend.yieldActive = failure, interrupt
		}},
		{"person building lookup", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.buildErr, backend.yieldBuilding = failure, interrupt
		}},
		{"person active lookup", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.activeErr, backend.yieldActive = failure, interrupt
		}},
		{"convergence before backstop", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Convergence = &fakeConvergenceChecker{err: failure, cancel: interrupt}
		}},
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
		}},
		{"coverage", func(job *EmbedJob, _ *fakeBackend, interrupt context.CancelCauseFunc) {
			job.Store = &fakeCoverage{err: failure, yieldCancel: interrupt}
		}},
		{"activation", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.activateErr, backend.yieldActivate = failure, interrupt
		}},
		{"activation success", func(_ *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.yieldActivate = interrupt
		}},
		{"sequence-bound activation", func(job *EmbedJob, backend *fakeBackend, interrupt context.CancelCauseFunc) {
			backend.activateErr, backend.yieldActivate = failure, interrupt
			job.SequenceBoundActivation = true
			job.Convergence = &fakeConvergenceChecker{result: ConvergenceResult{
				MessageCoverageComplete: true, PersonCoverageComplete: true, ReconciliationComplete: true,
			}}
		}},
	} {
		for _, interruption := range []string{"none", "cooperative", "cancelled"} {
			t.Run(test.name+"/"+interruption, func(t *testing.T) {
				require := require.New(t)
				assert := assert.New(t)
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				ctx, request := jobctx.WithPreemption(ctx)
				interrupt := context.CancelCauseFunc(func(error) {})
				if interruption == "cancelled" {
					interrupt = cancel
				}
				if interruption == "cooperative" {
					interrupt = func(error) { request() }
				}
				backend := &fakeBackend{
					building:  &vector.Generation{ID: 9, State: vector.GenerationBuilding, Fingerprint: "m:768"},
					activeErr: vector.ErrNoActiveGeneration,
				}
				var logs bytes.Buffer
				runner := &fakeRunner{}
				coverage := &fakeCoverage{}
				job := &EmbedJob{
					Log:    slog.New(slog.NewTextHandler(&logs, nil)),
					Worker: runner, Backend: backend, Store: coverage,
					Fingerprint: "m:768", BackstopInterval: -1,
				}
				test.configure(job, backend, interrupt)
				runner, ok := job.Worker.(*fakeRunner)
				require.True(ok)

				err := job.run(ctx)

				if test.name == "activation success" || test.name == "backstop success" {
					require.NoError(err)
				} else {
					require.ErrorIs(err, failure)
					require.ErrorIs(jobctx.ErrorAfterYield(ctx, err), failure)
				}
				if interruption != "none" {
					assert.NotContains(logs.String(), "generation lookup failed")
					for _, message := range []string{"embed reclaim failed", "embed run failed", "convergence check after run failed", "embed backstop failed"} {
						assert.NotContains(logs.String(), message)
					}
					if test.name == "interrupted RunOnce" || test.name == "reclaim" {
						assert.NotContains(logs.String(), "embed run complete")
					}
					if test.name == "interrupted RunOnce" || test.name == "reclaim" || test.name == "backstop" || test.name == "backstop success" || test.name == "convergence before backstop" {
						assert.Zero(coverage.calls)
						assert.Empty(backend.activations())
					}
					assert.NotContains(logs.String(), "activation failed")
					assert.NotContains(logs.String(), "building generation activated")
					if test.name == "coverage" {
						assert.Empty(backend.activations())
					}
					if test.name == "backstop" || test.name == "backstop success" {
						assert.Contains(logs.String(), "embed run complete")
						n, _ := runner.backstops()
						assert.Equal(1, n)
					}
					if test.name == "reclaim" {
						_, runs, _ := runner.calls()
						assert.Zero(runs)
					}
					if test.name == "target building lookup" || test.name == "target active lookup" {
						_, runs, _ := runner.calls()
						assert.Zero(runs)
					}
				}
				switch interruption {
				case "cooperative":
					assert.True(jobctx.PreemptionRequested(ctx))
					require.NoError(ctx.Err())
				case "cancelled":
					require.ErrorIs(context.Cause(ctx), jobctx.ErrYieldedToWaiter)
				}
			})
		}
	}
}
