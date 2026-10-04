package task

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// runnerSeq provides process-unique runner suffixes without allocating a
// new math/rand source per Task.
var runnerSeq atomic.Uint64

type Task[T any] struct {
	dep      T
	cache    Cache
	runnerID string
	s        *Scheduler
}

func NewTask[T any](dep T, cache Cache, s *Scheduler) *Task[T] {
	id := fmt.Sprintf("runner-%d-%d", time.Now().UnixNano(), runnerSeq.Add(1))
	return &Task[T]{
		dep:      dep,
		cache:    cache,
		runnerID: id,
		s:        s,
	}
}

func (t *Task[T]) Route(interval time.Duration, name string, lockTTL time.Duration, fn func(context.Context, T)) {
	t.RouteWithDelay(0, interval, name, lockTTL, fn)
}

func (t *Task[T]) RouteWithDelay(delay time.Duration, interval time.Duration, name string, lockTTL time.Duration, fn func(context.Context, T)) {
	if lockTTL <= 0 {
		panic("lockTTL must be > 0")
	}
	if interval <= 0 && delay > 0 {
		panic("delay requires interval > 0")
	}
	task := &genericTask[T]{
		name:     name,
		dep:      t.dep,
		fn:       fn,
		interval: interval,
		delay:    delay,
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
	delay    time.Duration
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

	timer := time.NewTimer(t.delay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !t.running.CompareAndSwap(false, true) {
				timer.Reset(t.interval)
				continue
			}

			acquired, err := t.cache.SetNx(ctx, lockKey, t.runnerID, t.lockTTL)
			if err != nil || !acquired {
				t.running.Store(false)
				// 抢锁失败也需要重置定时器，进入下一个周期的竞争
				timer.Reset(t.interval)
				continue
			}

			jobCtx, jobCancel := context.WithCancel(ctx)
			jobDone := make(chan struct{})
			wg.Add(2)

			go func() {
				defer wg.Done()
				t.watchdog(jobCtx, jobCancel, lockKey)
			}()

			go func() {
				defer wg.Done()
				defer jobCancel()
				defer t.running.Store(false)
				defer t.releaseLock(lockKey)
				defer close(jobDone)

				defer func() {
					if r := recover(); r != nil {
						slog.Error("task panic", "name", t.name, "recover", r)
					}
				}()

				t.fn(jobCtx, t.dep)
			}()

			// The loop goroutine solely owns timer: it resets the timer
			// only after the job signals completion ("执行完后再等 interval").
			// Resetting from the job goroutine raced with the loop's own
			// resets and broke timer guarantees.
			select {
			case <-ctx.Done():
				jobCancel()
				return
			case <-jobDone:
				timer.Reset(t.interval)
			}
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
				jobCancel()
				return
			}
		}
	}
}

func (t *genericTask[T]) releaseLock(lockKey string) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_ = t.cache.CompareAndDelete(releaseCtx, lockKey, t.runnerID)
}
