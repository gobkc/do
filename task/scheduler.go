package task

import (
	"context"
	"log/slog"
	"sync"
)

type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	started bool
	pending []taskRunner
	cancels map[string]context.CancelFunc
}

func NewScheduler() *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		ctx:     ctx,
		cancel:  cancel,
		cancels: make(map[string]context.CancelFunc),
	}
}

func (s *Scheduler) addTask(t taskRunner) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cancel, ok := s.cancels[t.Name()]; ok {
		cancel()
		delete(s.cancels, t.Name())
	}

	if s.started {
		cancel, err := t.start(s.ctx, &s.wg)
		if err != nil {
			slog.Error("start task failed", "name", t.Name(), "error", err)
			return
		}
		s.cancels[t.Name()] = cancel
	} else {
		s.pending = append(s.pending, t)
	}
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	for _, t := range s.pending {
		cancel, err := t.start(s.ctx, &s.wg)
		if err != nil {
			slog.Error("start pending task failed", "name", t.Name(), "error", err)
			continue
		}
		s.cancels[t.Name()] = cancel
	}
	s.pending = nil
}

func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
	slog.Info("all tasks stopped")
}

func (s *Scheduler) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel, ok := s.cancels[name]; ok {
		cancel()
		delete(s.cancels, name)
	}
}
