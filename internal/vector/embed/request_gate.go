package embed

import (
	"context"
	"errors"
	"net/http"
	"time"

	"go.kenn.io/msgvault/internal/jobctx"
)

// ErrEmbeddingProviderRedirect classifies redirects rejected by a gated
// embeddings client. Redirect responses are permanent policy failures and are
// never retried or followed.
var ErrEmbeddingProviderRedirect = errors.New("embedding provider redirects are not allowed")

// BeforeRequestFunc authorizes one concrete HTTP attempt. It runs from the
// transport boundary, so retries and provider-specific request packing each
// receive a fresh check. Clients with a BeforeRequestFunc reject redirects.
type BeforeRequestFunc func(context.Context) error

type beforeRequestTransport struct {
	base   http.RoundTripper
	before BeforeRequestFunc
	cancel context.CancelCauseFunc
}

type beforeRequestError struct{ err error }

func (e *beforeRequestError) Error() string { return e.err.Error() }
func (e *beforeRequestError) Unwrap() error { return e.err }

func (t beforeRequestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var err error
	if jobctx.WorkWindowExpired(request.Context()) {
		err = jobctx.ErrRunBudgetExceeded
	} else if t.before != nil {
		err = t.before(request.Context())
	}
	if err != nil {
		if t.cancel != nil {
			t.cancel(err)
		}
		return nil, &beforeRequestError{err: err}
	}
	return t.base.RoundTrip(request)
}

func newHTTPClient(timeout time.Duration, rejectRedirects bool) *http.Client {
	client := &http.Client{Timeout: timeout, Transport: embeddingMeasuredTransport{base: http.DefaultTransport}}
	if rejectRedirects {
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	return client
}

func beforeRequestCause(err error) error {
	var requestErr *beforeRequestError
	if !errors.As(err, &requestErr) {
		return nil
	}
	return requestErr.err
}
