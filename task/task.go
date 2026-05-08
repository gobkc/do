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
		t.run(childCtx, wg)
	}()
	return cancel, nil
}

func (t *genericTask[T]) run(ctx context.Context, wg *sync.WaitGroup) {
	lockKey := "scheduler:lock:" + t.name

	// 改动点：Once 任务不续期、不释放，执行完即结束
	if t.interval <= 0 {
		acquired, err := t.cache.SetNx(ctx, lockKey, t.runnerID, t.lockTTL)
		if err != nil || !acquired {
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("once task panic", "name", t.name, "recover", r)
				}
			}()
			t.fn(ctx, t.dep)
		}()
		return
	}

	// 改动点：改用 Timer 模式，确保 interval 是任务结束后的间隔，避免 Ticker 积压
	timer := time.NewTimer(t.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// 改动点：使用 CAS 进行原子状态切换，解决竞争条件
			if !t.running.CompareAndSwap(false, true) {
				timer.Reset(t.interval)
				continue
			}

			acquired, err := t.cache.SetNx(ctx, lockKey, t.runnerID, t.lockTTL)
			if err != nil || !acquired {
				t.running.Store(false)
				timer.Reset(t.interval)
				continue
			}

			jobCtx, jobCancel := context.WithCancel(ctx)
			wg.Add(2)

			// 改动点：Watchdog 失败时调用 jobCancel，确保业务逻辑立即感知锁权丧失
			go func() {
				defer wg.Done()
				t.watchdog(jobCtx, jobCancel, lockKey)
			}()

			go func() {
				defer wg.Done()
				defer jobCancel()
				defer t.running.Store(false)
				defer t.releaseLock(lockKey)
				// 改动点：任务结束后才重置 Timer
				defer timer.Reset(t.interval)

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

func (t *genericTask[T]) watchdog(jobCtx context.Context, jobCancel context.CancelFunc, lockKey string) {
	ticker := time.NewTicker(t.lockTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-jobCtx.Done():
			return
		case <-ticker.C:
			ok, err := t.cache.Expire(jobCtx, lockKey, t.lockTTL)
			if err != nil || !ok {
				slog.Warn("lock renewal failed, canceling job", "name", t.name)
				jobCancel() // 改动点：必须取消任务，防止 Split-brain
				return
			}
		}
	}
}

func (t *genericTask[T]) releaseLock(lockKey string) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	// 改动点：使用新增的 CompareAndDelete 接口，内部通过 Lua 实现原子删除
	_ = t.cache.CompareAndDelete(releaseCtx, lockKey, t.runnerID)
}
