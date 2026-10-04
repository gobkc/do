package do

import (
	"context"
	"math/rand"
	"strings"
	"time"
)

func BatchCall[D any, P any](ctx context.Context, params []P, limit int, f func(ctx context.Context, param []P) []D) []D {
	var results = make([]D, 0, len(params))
	if len(params) == 0 {
		return results
	}
	// Guard against non-positive limit (previously an infinite loop).
	if limit <= 0 {
		limit = len(params)
	}
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
	// Guard against non-positive limit (previously offset never advanced).
	if limit <= 0 {
		limit = 1
	}
	// Bound the initial capacity: the old limit*2 could over-allocate
	// gigabytes for large page sizes. Capacity is not observable.
	initCap := limit * 2
	if initCap > 1024 {
		initCap = 1024
	}
	var offset int64
	var results = make([]T, 0, initCap)
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

		// ⭐ jitter (guard tiny delays: Int63n(0) would panic)
		var jitter time.Duration
		if delay > 1 {
			jitter = time.Duration(rand.Int63n(int64(delay / 2)))
		}

		// Use NewTimer + Stop instead of time.After to avoid leaking
		// a timer on every retry until it fires.
		timer := time.NewTimer(delay + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()

		case <-timer.C:
		}

		// exponential backoff
		delay *= 2

		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}

	return zero, nil
}

// containsFoldASCII reports whether s contains sub, comparing ASCII
// letters case-insensitively. sub must already be uppercase ASCII.
// Equivalent to strings.Contains(strings.ToUpper(s), sub) for these
// ASCII keywords, but without allocating the uppercased copy.
func containsFoldASCII(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	if len(s) < len(sub) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		matched := true
		for j := 0; j < len(sub); j++ {
			c := s[i+j]
			if c >= 'a' && c <= 'z' {
				c -= 'a' - 'A'
			}
			if c != sub[j] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if msg == "" {
		return false
	}
	// ASCII fast path avoids strings.ToUpper's allocation. Non-ASCII
	// messages fall back to ToUpper so Unicode simple case mappings
	// (e.g. 'ſ' -> 'S') keep behaving exactly as before.
	ascii := true
	for i := 0; i < len(msg); i++ {
		if msg[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if !ascii {
		msg = strings.ToUpper(msg)
	}
	switch {
	case containsFoldASCII(msg, "RESOURCE_EXHAUSTED"),
		containsFoldASCII(msg, "DEADLINE_EXCEEDED"),
		containsFoldASCII(msg, "UNAVAILABLE"),
		containsFoldASCII(msg, "ABORTED"):
		return true
	}
	return false
}
