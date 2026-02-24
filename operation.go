package do

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"
)

func OneOf[T any](condition bool, result1 T, result2 T) T {
	if condition {
		return result1
	}
	return result2
}

func OneOr[T any](result1 T, result2 ...T) T {
	isZero := func(v T) bool {
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Pointer {
			if val.IsNil() {
				return true
			}
			val = val.Elem()
		}
		return val.IsZero()
	}

	if !isZero(result1) {
		return result1
	}

	for _, v := range result2 {
		if !isZero(v) {
			return v
		}
	}

	var zero T
	return zero
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

func AnyInList[T comparable]() {

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
		for range times {
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

type DiffResp[T comparable] struct {
	Added   []T
	Deleted []T
	Same    []T
}

// Diff The diff function is used to analyze the items to be added and deleted between two slices.
// example:	var news = []string{`aa1`, `aa2`, `aa3`}
// var olds = []string{`aa1`, `aa4`}
// diffs := Diff(olds, news)
// result:added: aa2,aa3,  deleted:aa4, same: aa1
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
		} else {
			resp.Same = append(resp.Same, item)
		}
	}

	for _, item := range olds {
		if _, ok := newMap[item]; !ok {
			resp.Deleted = append(resp.Deleted, item)
		}
	}

	return resp
}

// GetFieldList is a generic function that takes a slice of items and a fieldGetter function,
// and returns a slice of any field type, such as string, int64, etc.
// examples
//
//	names1 := GetFieldList(items1, func(item Item) string {
//		return item.Name
//	})
//
//	// Retrieve the Age field list from items1 (int64 type)
func GetFieldList[T any, R any](items []T, fieldGetter func(T) R) []R {
	var result []R
	for _, item := range items {
		result = append(result, fieldGetter(item))
	}
	return result
}

func GetFieldMaps[T any, K comparable](items []T, fieldGetter func(T) K) map[K][]T {
	result := make(map[K][]T)
	for _, item := range items {
		key := fieldGetter(item)
		result[key] = append(result[key], item)
	}
	return result
}

func GetFieldMap[T any, K comparable](items []T, fieldGetter func(T) K) map[K]T {
	result := make(map[K]T)
	for _, item := range items {
		key := fieldGetter(item)
		result[key] = item
	}
	return result
}

var marshalFunc = func(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

var escapeFunc = func(v any) string {
	switch val := v.(type) {
	case string:
		return template.HTMLEscapeString(val)
	default:
		return ``
	}
}

func ReplaceMap(s string, replace map[string]string) (result string, err error) {
	result = s
	if replace == nil {
		replace = make(map[string]string)
	}
	tmpl, err := template.New("soapRequest").Funcs(template.FuncMap{
		"marshal": marshalFunc,
		"escape":  escapeFunc,
	}).Parse(s)
	if err != nil {
		return result, err
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, replace)
	if err != nil {
		return result, err
	}
	result = buf.String()
	return result, nil
}

// RegexpCheck Use regular expressions to determine if a string matches
// Example: RegexpCheck(`(?i)^[a-zA-Z]+ (asc|desc)$`,`dafd Asc`) == true
func RegexpCheck(pattern string, str string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		slog.Error(`failed to compile regular expression.`, slog.String(`err`, err.Error()))
		return false
	}
	if re.MatchString(str) {
		return true
	}
	return false
}

// RegexpConvertSnake convert string to snake case
// Example: RegexpConvertSnake(`AbC`) == `ab_c`
func RegexpConvertSnake(s string) string {
	re, err := regexp.Compile(`[A-Z]`)
	if err != nil {
		slog.Error("Error compiling snake case to snake case: %v", slog.String(`err`, err.Error()))
		return s
	}
	return re.ReplaceAllStringFunc(s, func(match string) string {
		if len(s) > 0 && s[0] == match[0] {
			return strings.ToLower(match)
		}
		return "_" + strings.ToLower(match)
	})
}

func OptionDefault[T any](options []T, def T) T {
	if len(options) == 0 {
		return def
	}
	return options[0]
}

type Zeroable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

// Unique
// data := []int{1, 2, 2, 3, 1, 4, 5, 3}
// unique := Unique(data)
// fmt.Println(unique) // [1 2 3 4 5]
func Unique[T Zeroable](items []T, patterns ...string) []T {
	seen := make(map[T]struct{})
	result := make([]T, 0, len(items))
	var zero T
	pattern := OptionDefault(patterns, ``)
	for _, v := range items {
		if v == zero {
			continue
		}
		if pattern != `` {
			if !RegexpCheck(pattern, fmt.Sprintf("%v", v)) {
				continue
			}
		}
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}
