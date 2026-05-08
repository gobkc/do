package task

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Task[T any] struct {
	dep      T
	cache    Cache
	runnerID string
	s        *Scheduler
}

func NewTask[T any](dep T, cache Cache, s *Scheduler) *Task[T] {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := fmt.Sprintf("runner-%d-%d", time.Now().UnixNano(), r.Int63())
	return &Task[T]{
		dep:      dep,
		cache:    cache,
		runnerID: id,
		s:        s,
	}
}

func (t *Task[T]) Route(interval time.Duration, name string, lockTTL time.Duration, fn func(context.Context, T)) {
	if lockTTL <= 0 {
		panic("lockTTL must be > 0")
	}
	task := &genericTask[T]{
		name:     name,
		dep:      t.dep,
		fn:       fn,
		interval: interval,
		lockTTL:  lockTTL,
		cache:    t.cache,
		runnerID: t.runnerID,
	}
	t.s.addTask(task)
}

type genericTask[T any] struct {
	name     string
	dep      T
	fn       func(context.Context, T)
	interval time.Duration
	lockTTL  time.Duration
	cache    Cache
	runnerID string
	running  atomic.Bool
}

func (t *genericTask[T]) Name() string { return t.name }

func (t *genericTask[T]) start(ctx context.Context, wg *sync.WaitGroup) (context.CancelFunc, error) {
	childCtx, cancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.run(childCtx)
	}()
	return cancel, nil
}

func (t *genericTask[T]) run(ctx context.Context) {
	lockKey := "scheduler:lock:" + t.name

	// 改动点：处理 interval 为 0 的情况
	if t.interval <= 0 {
		// 抢锁：如果抢不到说明其他 Pod 正在执行或已执行（在 lockTTL 时间内）
		acquired, err := t.cache.SetNx(ctx, lockKey, t.runnerID, t.lockTTL)
		if err != nil || !acquired {
			return
		}

		jobCtx, jobCancel := context.WithCancel(ctx)
		go t.watchdog(ctx, jobCancel, lockKey)

		func() {
			defer jobCancel()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("once task panic", "name", t.name, "recover", r)
				}
			}()
			t.fn(jobCtx, t.dep)
		}()
		// 改动点：执行完即退出，且不主动调用 releaseLock，利用 lockTTL 确保其他 Pod 不会重复处理
		return
	}

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	var jobWg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			jobWg.Wait()
			return
		case <-ticker.C:
			if t.running.Load() {
				continue
			}

			acquired, err := t.cache.SetNx(ctx, lockKey, t.runnerID, t.lockTTL)
			if err != nil || !acquired {
				continue
			}

			t.running.Store(true)
			jobWg.Add(1)

			jobCtx, jobCancel := context.WithCancel(ctx)
			go t.watchdog(ctx, jobCancel, lockKey)

			go func() {
				defer jobWg.Done()
				defer jobCancel()
				defer t.running.Store(false)
				defer t.releaseLock(lockKey) // 定时任务保留原有的主动释放逻辑

				defer func() {
					if r := recover(); r != nil {
						slog.Error("task panic", "name", t.name, "recover", r)
					}
				}()

				t.fn(jobCtx, t.dep)
			}()
		}
	}
}

func (t *genericTask[T]) watchdog(parentCtx context.Context, jobCancel context.CancelFunc, lockKey string) {
	ticker := time.NewTicker(t.lockTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-parentCtx.Done():
			return
		case <-time.After(t.lockTTL):
			jobCancel()
			return
		case <-ticker.C:
			ok, err := t.cache.Expire(parentCtx, lockKey, t.lockTTL)
			if err != nil || !ok {
				slog.Warn("lock renewal failed, canceling job", "name", t.name)
				jobCancel()
				return
			}
		}
	}
}

func (t *genericTask[T]) releaseLock(lockKey string) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	val, err := t.cache.Get(releaseCtx, lockKey)
	if err == nil && val == t.runnerID {
		_ = t.cache.Del(releaseCtx, lockKey)
	}
}
