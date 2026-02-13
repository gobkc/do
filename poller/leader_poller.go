package poller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type LeaderPoller[T any] struct {
	interval   time.Duration
	lockTTL    time.Duration
	cache      Cacher
	subject    string
	key        string
	delayed    bool
	conditions []func() (T, error)
	mu         sync.Mutex
	cancelFunc context.CancelFunc
}

func NewLeaderPoller[T any](cache Cacher, subject string, interval, lockTTL time.Duration, delayed bool) *LeaderPoller[T] {
	if cache == nil {
		panic("redis client cannot be nil")
	}
	return &LeaderPoller[T]{
		cache:      cache,
		subject:    subject,
		interval:   interval,
		lockTTL:    lockTTL,
		delayed:    delayed,
		conditions: []func() (T, error){},
	}
}

func (lp *LeaderPoller[T]) Conditions(cond ...func() (T, error)) {
	lp.conditions = append(lp.conditions, cond...)
}

func (lp *LeaderPoller[T]) Run(ctx context.Context, task func(T)) {
	ctx, cancel := context.WithCancel(ctx)
	lp.cancelFunc = cancel

	ticker := time.NewTicker(lp.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info(`LeaderPoller stopped`)
			return
		case <-ticker.C:
			lp.tryRunLeaderTask(ctx, task)
		}
	}
}

func (lp *LeaderPoller[T]) Stop() {
	if lp.cancelFunc != nil {
		lp.cancelFunc()
	}
}

func (lp *LeaderPoller[T]) tryRunLeaderTask(ctx context.Context, task func(T)) {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	key := fmt.Sprintf("%d", time.Now().UnixNano())
	acquired, err := lp.cache.SetNX(ctx, lp.subject, key, lp.lockTTL)
	if err != nil {
		slog.Error(`failed to acquire leader lock`, slog.String("error", err.Error()))
		return
	}
	if !acquired {
		slog.Error(`not leader, skipping task`)
		return
	}

	lockCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go lp.keepLock(lockCtx, key)

	var result T
	for _, cond := range lp.conditions {
		result, err = cond()
		if err != nil {
			slog.Error(`condition not met, skipping task`, slog.String("error", err.Error()))
			return
		}
	}

	task(result)

	lp.cache.Del(ctx, lp.subject)
}

// keepLock
func (lp *LeaderPoller[T]) keepLock(ctx context.Context, key string) {
	ticker := time.NewTicker(lp.lockTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current, err := lp.cache.Get(ctx, lp.subject)
			if err != nil {
				slog.Error(`failed to get leader key during renew`, slog.String("error", err.Error()))
				return
			}
			if current != key {
				// lock other Pod
				return
			}
			// delay TTL
			_, err = lp.cache.SetNX(ctx, lp.subject, key, lp.lockTTL)
			if err != nil {
				slog.Error(`failed to renew leader lock`, slog.String("error", err.Error()))
				return
			}
		}
	}
}
