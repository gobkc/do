package do

import (
	"reflect"
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
