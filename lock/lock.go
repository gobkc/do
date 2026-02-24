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

	for {
		select {
		case <-ctx.Done():
			err := store.Delete(context.Background(), key, owner)
			if err != nil {
				slog.Error("failed to release lock",
					slog.String(`key`, key),
					slog.String(`owner`, owner),
				)
			}
			return
		case <-ticker.C:
			ok, err := store.SetNx(ctx, key, owner, ttl)
			if err != nil || !ok {
				continue
			}
			go func() {
				defer func() {
					err := store.Delete(context.Background(), key, owner)
					if err != nil {
						slog.Error("failed to release lock",
							slog.String(`key`, key),
							slog.String(`owner`, owner),
						)
					}
				}()

				handler(ctx)
			}()
		}
	}
}
