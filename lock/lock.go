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

// Run Always running
func Run(ctx context.Context, store Store, key string, owner string, ttl time.Duration, interval time.Duration, handler func(ctx context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var acquired bool

	for {
		select {
		case <-ctx.Done():
			if acquired {
				_ = store.Delete(context.Background(), key, owner)
			}
			return
		case <-ticker.C:
			if acquired {
				// Permanent lock-up renewal
				_, _ = store.SetNx(ctx, key, owner, ttl)
				continue
			}

			// Try to snatch the lock
			ok, _ := store.SetNx(ctx, key, owner, ttl)
			if ok {
				acquired = true
				go handler(ctx)
			}
		}
	}
}
