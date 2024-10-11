package do

import (
	"log/slog"
	"reflect"
	"sync"
	"time"
)

func OneOf[T any](condition bool, result1 T, result2 T) T {
	if condition {
		return result1
	}
	return result2
}

func OneOr[T any](result1 T, result2 T) T {
	v1 := reflect.ValueOf(result1)
	if v1.Kind() == reflect.Pointer {
		v1 = v1.Elem()
	}
	if v1.IsZero() {
		return result2
	}
	return result1
}

func ErrorOr(err error, val string) string {
	if err != nil {
		return err.Error()
	}
	return val
}

// AnyTrue AnyTrue(true,false,false) == true
// AnyTrue AnyTrue(false,false,false) == false
func AnyTrue(bs ...bool) bool {
	for _, b := range bs {
		if b == true {
			return true
		}
	}
	return false
}

// AllTrue AllTrue(true,true,true) == true
// AllTrue AllTrue(true,false,true) == false
func AllTrue(bs ...bool) bool {
	for _, b := range bs {
		if b == false {
			return false
		}
	}
	return true
}

// InList InList["a",[]string{"a","b"}] == true
func InList[T comparable](item T, list ...T) bool {
	for _, t := range list {
		if t == item {
			return true
		}
	}
	return false
}

type ReTry[T any] struct {
	sync.Once
	value *T
}

// Keep example
//
//	s := retry.Keep(func(t *Test) error {
//		if err := GenError(); err != nil {
//			return err
//		}
//		t.Name = `abc`
//		return nil
//	})
func (r *ReTry[T]) Keep(action func(t *T) error) *T {
	r.Do(func() {
		r.value = new(T)
		name := GetStructName(r.value)
		for {
			if err := action(r.value); err != nil {
				slog.Default().Error(`Failed to initialize `+name, slog.String(`error`, err.Error()))
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	})
	return r.value
}

func (r *ReTry[T]) Times(times int, action func(t *T) error) *T {
	r.Do(func() {
		r.value = new(T)
		name := GetStructName(r.value)
		for i := 0; i < times; i++ {
			if err := action(r.value); err != nil {
				slog.Default().Error(`Failed to initialize `+name, slog.String(`error`, err.Error()))
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	})
	return r.value
}

func GetStructName(d any) string {
	de := reflect.ValueOf(d)
	if de.Kind() == reflect.Pointer {
		de = de.Elem()
	}
	n := de.Type().Name()
	return n
}

type PollerImpl[T comparable] struct {
	interval *time.Duration
	err      chan error
	result   chan *T
	done     chan struct{}
	stopped  bool
}

type PollerSetting struct {
	Interval time.Duration
}

func (p *PollerImpl[T]) Setting(f func(settings *PollerSetting)) *PollerImpl[T] {
	settings := &PollerSetting{
		Interval: 1 * time.Second,
	}
	f(settings)
	if p.interval != nil {
		*p.interval = settings.Interval
	}
	return p
}

func (p *PollerImpl[T]) Then(f func(result *T)) *PollerImpl[T] {
	go func() {
		for {
			select {
			case result := <-p.result:
				f(result)
			case <-p.done:
				slog.Default().Info(`Stopping Then loop`)
				return
			}
		}
	}()
	return p
}

func (p *PollerImpl[T]) Catch(f func(err error)) {
	go func() {
		for {
			select {
			case err := <-p.err:
				f(err)
			case <-p.done:
				slog.Default().Info(`Stopping Catch loop`)
				return
			}
		}
	}()
}

func (p *PollerImpl[T]) Stop() {
	// close all channel and send some completion signal
	close(p.done)
	close(p.err)
	close(p.result)
	p.stopped = true
}

func Poller[QueryResult comparable](query func(q *QueryResult) error) *PollerImpl[QueryResult] {
	q := new(QueryResult)
	interval := 1 * time.Second
	poller := &PollerImpl[QueryResult]{
		interval: &interval,
		err:      make(chan error),
		result:   make(chan *QueryResult),
		done:     make(chan struct{}),
		stopped:  false,
	}

	go func() {
		for {
			select {
			case <-poller.done: // close poller
				slog.Default().Info(`Stopping Poller loop`)
				return
			default:
				time.Sleep(*poller.interval)
				if poller.stopped {
					slog.Default().Info(`Stopping Poller loop`)
					return
				}
				err := query(q)
				//send queryResult to the then function & error to the catch function
				if err != nil {
					poller.err <- err
					continue
				}
				if q != nil {
					poller.result <- q
				}
			}
		}
	}()
	return poller
}
