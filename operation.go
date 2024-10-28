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

type PollerImpl[T any] struct {
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

func Poller[QueryResult any](query func(q *QueryResult) error) *PollerImpl[QueryResult] {
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
				if poller.stopped {
					slog.Default().Info(`Stopping Poller loop`)
					return
				}
				err := query(q)
				//send queryResult to the then function & error to the catch function
				if err != nil {
					poller.err <- err
					slog.Default().Error(`The current polling failed`, slog.String(`next execution time:`, time.Now().Add(*poller.interval).Format(time.DateTime)))
					time.Sleep(*poller.interval)
					continue
				}
				if q != nil {
					poller.result <- q
				}
				slog.Default().Info(`The current polling succeeded`, slog.String(`next execution time:`, time.Now().Add(*poller.interval).Format(time.DateTime)))
				time.Sleep(*poller.interval)
			}
		}
	}()
	return poller
}

type DiffResp[T comparable] struct {
	Added   []T
	Deleted []T
}

// Diff The diff function is used to analyze the items to be added and deleted between two slices.
// example:	var news = []string{`aa1`, `aa2`, `aa3`}
// var olds = []string{`aa1`, `aa4`}
// diffs := Diff(olds, news)
// result:added: aa2,aa3,  deleted:aa4
func Diff[T comparable](olds, news []T) (resp DiffResp[T]) {
	oldMap := make(map[T]struct{})
	newMap := make(map[T]struct{})

	for _, item := range olds {
		oldMap[item] = struct{}{}
	}

	for _, item := range news {
		newMap[item] = struct{}{}
		if _, ok := oldMap[item]; !ok {
			resp.Added = append(resp.Added, item)
		}
	}

	for _, item := range olds {
		if _, ok := newMap[item]; !ok {
			resp.Deleted = append(resp.Deleted, item)
		}
	}

	return resp
}
