package jobctx

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreemptionRequestVisibleThroughDerivedContexts(t *testing.T) {
	assert := assert.New(t)
	assert.False(PreemptionRequested(context.Background()), "plain context is never preempted")

	ctx, request := WithPreemption(context.Background())
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	assert.False(PreemptionRequested(child))

	request()
	request()
	assert.True(PreemptionRequested(child), "request reaches derived contexts")
	assert.NoError(child.Err(), "preemption does not cancel")
	assert.NoError(context.WithoutCancel(ctx).Err())
	assert.True(PreemptionRequested(context.WithoutCancel(ctx)))
}

func TestBudgetDenialPreservesIndependentError(t *testing.T) {
	require := require.New(t)
	independent := errors.New("publication failed")
	require.NoError(ErrorAfterYield(t.Context(), ErrRunBudgetExceeded))
	filtered := ErrorAfterYield(t.Context(), errors.Join(ErrRunBudgetExceeded, independent))
	require.ErrorIs(filtered, independent)
	require.NotErrorIs(filtered, ErrRunBudgetExceeded)
	require.ErrorIs(ErrorAfterYield(t.Context(), context.DeadlineExceeded), context.DeadlineExceeded)
}
