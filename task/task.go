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
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	lockKey := "scheduler:lock:" + t.name
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

			// 尝试获取分布式锁
			acquired, err := t.cache.SetNx(ctx, lockKey, t.runnerID, t.lockTTL)
			if err != nil {
				slog.Error("lock error", "name", t.name, "err", err)
				continue
			}
			if !acquired {
				continue
			}

			// 抢锁成功
			t.running.Store(true)
			jobWg.Add(1)

			// 为单次执行创建一个可取消的 context
			jobCtx, jobCancel := context.WithCancel(ctx)

			// 1. 启动看门狗：负责续期
			go t.watchdog(ctx, jobCancel, lockKey)

			// 2. 执行任务
			go func() {
				defer jobWg.Done()
				defer jobCancel() // 任务结束通知看门狗停止
				defer t.running.Store(false)
				defer t.releaseLock(lockKey)

				// 异常保护
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

// watchdog 在任务运行期间自动续期，防止锁过期导致多个 Pod 同时运行
func (t *genericTask[T]) watchdog(parentCtx context.Context, jobCancel context.CancelFunc, lockKey string) {
	// 续期频率设为 TTL 的 1/3
	ticker := time.NewTicker(t.lockTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-parentCtx.Done(): // 整个调度停止
			return
		case <-time.After(t.lockTTL): // 安全防范：如果自己挂了，这里会触发
			jobCancel()
			return
		case <-ticker.C:
			// 检查任务是否已经通过 jobCancel 结束了
			// 续期
			ok, err := t.cache.Expire(parentCtx, lockKey, t.lockTTL)
			if err != nil || !ok {
				// 续期失败，可能锁被抢走或 Redis 挂了
				// 为了安全，通知业务逻辑停止执行
				slog.Warn("lock renewal failed, canceling job", "name", t.name)
				jobCancel()
				return
			}
		}
	}
}

func (t *genericTask[T]) releaseLock(lockKey string) {
	// 释放锁时使用 Background，确保即使父级 context 取消了也能尝试删除锁
	// 但设置短超时，防止卡死
	releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	val, err := t.cache.Get(releaseCtx, lockKey)
	if err == nil && val == t.runnerID {
		_ = t.cache.Del(releaseCtx, lockKey)
	}
}
