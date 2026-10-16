package scheduler

import "go.kenn.io/msgvault/internal/vector"

// EmbeddingStatus reports the same reserved pass for cron and post-sync work.
func (s *Scheduler) EmbeddingStatus() vector.EmbeddingStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := vector.EmbeddingStatus{Job: vector.EmbeddingJobState{State: "idle"}, Scheduler: vector.EmbeddingSchedulerState{
		Registered: s.genericFuncs["embed"] != nil, Schedule: s.genericSchedules["embed"], RunAfterSync: s.runEmbedAfterSync,
	}}
	if s.genericQueued["embed"] {
		status.Job.State = "queued"
		queued := s.genericQueuedSince["embed"]
		status.Job.QueuedAt = &queued
	} else if s.genericRunning["embed"] {
		status.Job.State = "running"
		started := s.genericStartedAt["embed"]
		status.Job.StartedAt = &started
	}
	return status
}
