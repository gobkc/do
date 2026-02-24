package lock

import (
	"context"
	"time"
)

type LockMode int

const (
	LockModeOneTime LockMode = iota
	LockModePermanent
)

type Store interface {
	SetNx(
		ctx context.Context,
		key string,
		owner string,
		ttl time.Duration,
	) (bool, error)
	Get(ctx context.Context, key string) (owner string, ttl time.Duration, err error)
	Delete(ctx context.Context, key, owner string) error
}
