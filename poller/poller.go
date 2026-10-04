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
		timer := time.NewTimer(0)
		defer timer.Stop()
		// Drain the initial immediate tick.
		select {
		case <-timer.C:
		default:
		}
		for {
			select {
			case <-p.done:
				return
			default:
				result, err := query()
				if err != nil {
					select {
					case p.errCh <- err:
					case <-p.done:
						return
					}
				} else {
					select {
					case p.resultCh <- result:
					case <-p.done:
						return
					}
				}
				// Sleep that wakes promptly on Stop instead of delaying
				// shutdown by up to a full interval.
				timer.Reset(p.interval)
				select {
				case <-p.done:
					return
				case <-timer.C:
				}
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
		for {
			select {
			case <-p.done:
				return
			case r := <-p.resultCh:
				f(r)
			}
		}
	}()
}

func (p *Poller[T]) Catch(f func(error)) {
	go func() {
		for {
			select {
			case <-p.done:
				return
			case err := <-p.errCh:
				f(err)
			}
		}
	}()
}
