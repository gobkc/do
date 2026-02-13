package do

import (
	"context"
	"math/rand"
	"strings"
	"time"
)

func BatchCall[D any, P any](ctx context.Context, params []P, limit int, f func(ctx context.Context, param []P) []D) []D {
	var results = make([]D, 0, len(params))
	for i := 0; i < len(params); i += limit {
		if err := ctx.Err(); err != nil {
			return results
		}
		end := min(i+limit, len(params))
		results = append(results, f(ctx, params[i:end])...)
	}
	return results
}

func BatchCallPagination[T any](ctx context.Context, limit int64, f func(ctx context.Context, offset int64) []T) []T {
	var offset int64
	var results = make([]T, 0, limit*2)
	for {
		if err := ctx.Err(); err != nil {
			return results
		}

		items := f(ctx, offset)
		if len(items) == 0 {
			return results
		}

		results = append(results, items...)
		offset += limit
	}
}

func RetryCall[T any](
	ctx context.Context,
	maxAttempts int,
	baseDelay time.Duration,
	fn func(context.Context) (T, error),
) (T, error) {
	var zero T
	delay := baseDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {

		if err := ctx.Err(); err != nil {
			return zero, err
		}

		res, err := fn(ctx)
		if err == nil {
			return res, nil
		}

		if !isRetryable(err) {
			return zero, err
		}

		if attempt == maxAttempts {
			return zero, err
		}

		// ⭐ jitter
		jitter := time.Duration(rand.Int63n(int64(delay / 2)))

		select {
		case <-ctx.Done():
			return zero, ctx.Err()

		case <-time.After(delay + jitter):
		}

		// exponential backoff
		delay *= 2

		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}

	return zero, nil
}

func isRetryable(err error) bool {
	e := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(e, "UNAVAILABLE"),
		strings.Contains(e, "DEADLINE_EXCEEDED"),
		strings.Contains(e, "RESOURCE_EXHAUSTED"),
		strings.Contains(e, "ABORTED"):
		return true
	}
	return false
}
