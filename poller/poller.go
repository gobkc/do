package poller

import (
	"sync"
	"time"
)

type Poller[T any] struct {
	interval time.Duration
	errCh    chan error
	resultCh chan *T
	done     chan struct{}
	mu       sync.Mutex
}

func NewPoller[T any](interval time.Duration) *Poller[T] {
	return &Poller[T]{
		interval: interval,
		errCh:    make(chan error),
		resultCh: make(chan *T),
		done:     make(chan struct{}),
	}
}

func (p *Poller[T]) Start(query func() (*T, error)) {
	go func() {
		for {
			select {
			case <-p.done:
				return
			default:
				result, err := query()
				if err != nil {
					p.errCh <- err
				} else {
					p.resultCh <- result
				}
				time.Sleep(p.interval)
			}
		}
	}()
}

func (p *Poller[T]) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		return
	default:
		close(p.done)
	}
}

func (p *Poller[T]) Then(f func(*T)) {
	go func() {
		for r := range p.resultCh {
			f(r)
		}
	}()
}

func (p *Poller[T]) Catch(f func(error)) {
	go func() {
		for err := range p.errCh {
			f(err)
		}
	}()
}
