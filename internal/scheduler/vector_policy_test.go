package scheduler

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/jobctx"
	"go.kenn.io/msgvault/internal/operations"
	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/embed"
)

// policyEmbedRunner exercises EmbedJob's real invocation and phase boundaries.
// The callback models a provider call interrupted after a durable checkpoint.
type policyEmbedRunner struct {
	fakeRunner

	run func(context.Context) error
}

func (r *policyEmbedRunner) RunOnce(ctx context.Context, _ vector.GenerationID, _ operations.PassScope) (embed.RunResult, error) {
	return embed.RunResult{}, r.run(ctx)
}

func registerPolicyVectorJob(t *testing.T, s *Scheduler, name string, run func(context.Context) error) {
	t.Helper()
	require := require.New(t)
	if name == "embed" {
		require.NoError(s.SetEmbedJob(&EmbedJob{
			Worker: &policyEmbedRunner{run: run}, Backend: &fakeBackend{active: vector.Generation{ID: 42}}, BackstopInterval: -1,
		}, "0 0 1 1 *", false))
	} else {
		require.NoError(s.SetDocumentVectorJob(run, "0 0 1 1 *", false))
	}
}

func TestVectorPassBudgetRemainsFinite(t *testing.T) {
	for _, name := range []string{"embed", "document-vector"} {
		for _, test := range []struct {
			name                         string
			timeout, response, admission time.Duration
		}{
			{name: "default budget"},
			{name: "configured budget", timeout: 10 * time.Minute},
			{name: "slow response", timeout: 10 * time.Minute, response: 6 * time.Minute, admission: 4*time.Minute + 59*time.Second},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					assert := assert.New(t)
					require := require.New(t)
					s := New(nil).WithVectorRequestTimeout(test.timeout)
					defer func() { <-s.Stop().Done() }()
					registerPolicyVectorJob(t, s, name, func(ctx context.Context) error {
						if test.response == 0 {
							<-ctx.Done()
							return ctx.Err()
						}
						time.Sleep(test.admission)
						requestCtx, cancel := context.WithTimeout(ctx, test.timeout)
						defer cancel()
						select {
						case <-time.After(test.response):
							jobctx.RecordProgress(ctx)
							return nil
						case <-requestCtx.Done():
							return requestCtx.Err()
						}
					})
					started := time.Now()
					err := s.TriggerJob(name)
					if test.response == 0 {
						require.ErrorIs(err, jobctx.ErrRunBudgetExceeded)
						timeout := test.timeout
						if timeout == 0 {
							timeout = 30 * time.Second
						}
						assert.Equal(5*time.Minute+timeout+5*time.Second, time.Since(started))
					} else {
						require.NoError(err)
						assert.Equal(test.admission+test.response, time.Since(started))
					}
				})
			})
		}
	}
}

func TestAutomaticVectorPassYieldsAndResumes(t *testing.T) {
	for _, name := range []string{"embed", "document-vector"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)
				var events []string
				var started, admitted time.Time
				syncRuns := 0
				syncCompleted := false
				recordWaiter := func(ctx context.Context) error {
					syncRuns++
					admitted = time.Now()
					events = append(events, "sync")
					select {
					case <-time.After(3 * time.Minute):
						syncCompleted = true
						events = append(events, "complete")
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				s := New(func(ctx context.Context, _ string) error { return recordWaiter(ctx) }).WithVectorRequestTimeout(10 * time.Minute).WithWorkTracker(newSerialWorkTracker())
				defer func() { <-s.Stop().Done() }()
				runs := 0
				registerPolicyVectorJob(t, s, name, func(ctx context.Context) error {
					runs++
					if runs == 1 {
						started = time.Now()
						done := jobctx.BeginAttempt(ctx)
						select {
						case <-time.After(90 * time.Second):
						case <-ctx.Done():
						}
						done()
						require.NoError(ctx.Err(), "a first slow attempt finishes before scheduled cancellation")
						events = append(events, "checkpoint")
						jobctx.RecordProgress(ctx)
						return nil
					}
					events = append(events, "resume")
					if !syncCompleted {
						jobctx.RecordProgress(ctx)
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				})
				mustStartJob(t, s, name)
				synctest.Wait()
				require.NoError(s.AddJob(Job{Name: "carddav", Schedule: "0 0 1 1 *", Preemptible: true, Run: recordWaiter}))
				mustStartJob(t, s, "carddav")
				synctest.Wait()
				time.Sleep(90*time.Second + 2*yieldPollInterval + 3*time.Minute)
				synctest.Wait()
				assert.Equal([]string{"checkpoint", "sync", "complete", "resume"}, events)
				assert.Equal(1, syncRuns)
				assert.GreaterOrEqual(admitted.Sub(started), 90*time.Second)
			})
		})
	}
}

func TestAutomaticEmbeddingReportsWorkerError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)
		workerErr := errors.New("embedding provider unavailable")
		s := New(nil)
		defer func() { <-s.Stop().Done() }()
		registerPolicyVectorJob(t, s, "embed", func(context.Context) error { return workerErr })
		err := s.TriggerJob("embed")
		require.ErrorIs(err, workerErr)
		statuses := s.JobStatus()
		require.Len(statuses, 1)
		status := statuses[0]
		assert.Equal(workerErr.Error(), status.LastError)
		assert.True(status.LastRun.IsZero())
	})
}

func TestReservedGenericJobKeepsPolicyOnReplacement(t *testing.T) {
	for _, change := range []string{"replaced", "removed"} {
		t.Run(change, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)
				s := New(nil)
				defer func() { <-s.Stop().Done() }()
				var calls []string
				require.NoError(s.AddJob(Job{Name: "bounded", Schedule: "0 0 1 1 *", Preemptible: true, MaxRuntime: vectorPassBudget, Run: func(ctx context.Context) error {
					calls = append(calls, "original")
					deadline, bounded := ctx.Deadline()
					assert.True(bounded)
					assert.Equal(vectorPassBudget, time.Until(deadline))
					return ErrReschedule
				}}))
				run, disposition, err := s.reserveGenericJob("bounded", false)
				require.NoError(err)
				require.Equal(JobStarted, disposition)
				if change == "removed" {
					s.RemoveJob("bounded")
				} else {
					require.NoError(s.AddJob(Job{Name: "bounded", Schedule: "0 0 1 1 *", MaxRuntime: 30 * time.Second, Run: func(ctx context.Context) error {
						calls = append(calls, "replacement")
						deadline, bounded := ctx.Deadline()
						assert.True(bounded)
						assert.Equal(30*time.Second, time.Until(deadline))
						return nil
					}}))
				}

				require.NoError(s.runJob("bounded", run))
				synctest.Wait()
				if change == "removed" {
					assert.Equal([]string{"original"}, calls)
					assert.Empty(s.JobStatus())
				} else {
					assert.Equal([]string{"original", "replacement"}, calls)
				}
			})
		})
	}
}

func TestAutomaticVectorPeersStopWithoutProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assert := assert.New(t)
		s := New(nil).WithWorkTracker(newSerialWorkTracker())
		defer func() { <-s.Stop().Done() }()
		calls := map[string]int{}
		for _, name := range []string{"embed", "document-vector"} {
			registerPolicyVectorJob(t, s, name, func(ctx context.Context) error {
				calls[name]++
				<-ctx.Done()
				return ctx.Err()
			})
		}
		mustStartJob(t, s, "embed")
		synctest.Wait()
		mustStartJob(t, s, "document-vector")
		synctest.Wait()
		time.Sleep(2 * (vectorPassBudget + 30*time.Second + 5*time.Second))
		synctest.Wait()
		for _, status := range s.JobStatus() {
			assert.Equal(1, calls[status.Name])
			assert.Contains(status.LastError, "runtime budget exceeded before committing progress")
			assert.False(status.Pending)
			assert.False(status.Running)
		}
	})
}
