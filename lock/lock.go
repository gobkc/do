package lock

import (
	"context"
	"time"
)

func TryLock(ctx context.Context, store Store, key string, owner string, ttl time.Duration) (bool, error) {
	ok, err := store.SetNx(ctx, key, owner, ttl)
	if err != nil {
		return false, err
	}
	return ok, nil
}
