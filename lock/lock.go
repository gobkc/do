package lock

import (
	"context"
	"log/slog"
	"time"
)

func TryLock(ctx context.Context, store Store, key string, owner string, ttl time.Duration) (bool, error) {
	ok, err := store.SetNx(ctx, key, owner, ttl)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func TryRunOnce(ctx context.Context, store Store, key string, owner string, ttl time.Duration, f func()) {
	ok, err := TryLock(ctx, store, key, owner, ttl)
	if err != nil {
		slog.Error("failed to acquire lock",
			slog.String(`key`, key),
			slog.String(`owner`, owner),
		)
	}
	if err == nil && ok {
		f()
	}
}
