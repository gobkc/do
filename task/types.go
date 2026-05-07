package task

import (
	"context"
	"sync"
	"time"
)

type Cache interface {
	Get(ctx context.Context, key string) (string, error)
	SetNx(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
	Del(ctx context.Context, key string) error
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
}

type taskRunner interface {
	start(ctx context.Context, wg *sync.WaitGroup) (context.CancelFunc, error)
	Name() string
}
