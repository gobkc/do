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
