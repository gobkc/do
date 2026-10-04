package do

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
)

// regexpCache caches successfully compiled regular expressions. It is
// bounded so that callers passing unbounded, data-derived patterns cannot
// grow memory without limit. Failed compilations are NOT cached so that
// invalid patterns keep the original behavior (re-attempt compile + log
// on every call).
const maxRegexpCacheEntries = 512

var (
	regexpCacheMu sync.Mutex
	regexpCache   = make(map[string]*regexp.Regexp, maxRegexpCacheEntries)
)

func cachedRegexp(pattern string) (*regexp.Regexp, error) {
	regexpCacheMu.Lock()
	re, ok := regexpCache[pattern]
	regexpCacheMu.Unlock()
	if ok {
		return re, nil
	}
	// Compile outside the lock; concurrent compilation of the same new
	// pattern is harmless (both results are valid).
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexpCacheMu.Lock()
	if len(regexpCache) >= maxRegexpCacheEntries {
		// Evict an arbitrary quarter of the entries to bound memory.
		evict := maxRegexpCacheEntries / 4
		for k := range regexpCache {
			delete(regexpCache, k)
			evict--
			if evict <= 0 {
				break
			}
		}
	}
	if existing, ok := regexpCache[pattern]; ok {
		re = existing
	} else {
		regexpCache[pattern] = re
	}
	regexpCacheMu.Unlock()
	return re, nil
}

// bufPool reuses bytes.Buffer allocations in ReplaceMap.
var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func OneOf[T any](condition bool, result1 T, result2 T) T {
	if condition {
		return result1
	}
	return result2
}

func isZeroValue[T any](v T) bool {
	// Fast paths for common non-pointer types: avoid reflect entirely.
	switch vv := any(v).(type) {
	case string:
		return vv == ""
	case bool:
		return !vv
	case int:
		return vv == 0
	case int8:
		return vv == 0
	case int16:
		return vv == 0
	case int32:
		return vv == 0
	case int64:
		return vv == 0
	case uint:
		return vv == 0
	case uint8:
		return vv == 0
	case uint16:
		return vv == 0
	case uint32:
		return vv == 0
	case uint64:
		return vv == 0
	case uintptr:
		return vv == 0
	case float32:
		return vv == 0
	case float64:
		return vv == 0
	}
	// Fallback preserves the original semantics exactly, including
	// pointer dereference (non-nil pointer to a zero value counts as zero).
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return true
	}
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return true
		}
		val = val.Elem()
	}
	return val.IsZero()
}

func OneOr[T any](result1 T, result2 ...T) T {
	if !isZeroValue(result1) {
		return result1
	}

	for _, v := range result2 {
		if !isZeroValue(v) {
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
		if b {
			return true
		}
	}
	return false
}

// AllTrue AllTrue(true,true,true) == true
// AllTrue AllTrue(true,false,true) == false
func AllTrue(bs ...bool) bool {
	for _, b := range bs {
		if !b {
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
	// Pre-size maps to avoid rehashing. Result slices stay nil when empty
	// to preserve the original nil-vs-empty behavior.
	oldMap := make(map[T]struct{}, len(olds))
	newMap := make(map[T]struct{}, len(news))

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
	if len(items) == 0 {
		return nil
	}
	result := make([]R, 0, len(items))
	for _, item := range items {
		result = append(result, fieldGetter(item))
	}
	return result
}

func GetFieldMaps[T any, K comparable](items []T, fieldGetter func(T) K) map[K][]T {
	result := make(map[K][]T, len(items))
	for _, item := range items {
		key := fieldGetter(item)
		result[key] = append(result[key], item)
	}
	return result
}

func GetFieldMap[T any, K comparable](items []T, fieldGetter func(T) K) map[K]T {
	result := make(map[K]T, len(items))
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

var replaceFuncMap = template.FuncMap{
	"marshal": marshalFunc,
	"escape":  escapeFunc,
}

// templateCache caches parsed templates keyed by source text. It is
// bounded so caller-controlled inputs cannot grow memory without limit.
// Parsed templates are safe for concurrent execution.
const maxTemplateCacheEntries = 128

var (
	templateCacheMu sync.Mutex
	templateCache   = make(map[string]*template.Template, maxTemplateCacheEntries)
)

func cachedTemplate(src string) (*template.Template, error) {
	templateCacheMu.Lock()
	t, ok := templateCache[src]
	templateCacheMu.Unlock()
	if ok {
		return t, nil
	}
	// Parse errors are not cached, preserving the original behavior of
	// re-parsing and returning the same error on every call.
	t, err := template.New("soapRequest").Funcs(replaceFuncMap).Parse(src)
	if err != nil {
		return nil, err
	}
	templateCacheMu.Lock()
	if len(templateCache) >= maxTemplateCacheEntries {
		evict := maxTemplateCacheEntries / 4
		for k := range templateCache {
			delete(templateCache, k)
			evict--
			if evict <= 0 {
				break
			}
		}
	}
	if existing, ok := templateCache[src]; ok {
		t = existing
	} else {
		templateCache[src] = t
	}
	templateCacheMu.Unlock()
	return t, nil
}

func ReplaceMap(s string, replace map[string]string) (result string, err error) {
	result = s
	if replace == nil {
		replace = make(map[string]string)
	}
	tmpl, err := cachedTemplate(s)
	if err != nil {
		return result, err
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if err = tmpl.Execute(buf, replace); err != nil {
		return result, err
	}
	result = buf.String()
	return result, nil
}

// RegexpCheck Use regular expressions to determine if a string matches
// Example: RegexpCheck(`(?i)^[a-zA-Z]+ (asc|desc)$`,`dafd Asc`) == true
func RegexpCheck(pattern string, str string) bool {
	re, err := cachedRegexp(pattern)
	if err != nil {
		slog.Error(`failed to compile regular expression.`, slog.String(`err`, err.Error()))
		return false
	}
	return re.MatchString(str)
}

// RegexpConvertSnake convert string to snake case
// Example: RegexpConvertSnake(`AbC`) == `ab_c`
func RegexpConvertSnake(s string) string {
	// Manual byte scan: identical output to the previous `[A-Z]` regex
	// implementation (including the first-byte rule), without any
	// regexp compilation or ReplaceAllStringFunc closure overhead.
	if s == "" {
		return s
	}
	first := s[0]
	// Fast path: no uppercase letters at all.
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			// Preserve original semantics exactly: the old code compared
			// match[0] against s[0] (not match position), so any uppercase
			// byte equal to the first byte is lowercased without '_' prefix.
			if c == first {
				b.WriteByte(c + 32)
			} else {
				b.WriteByte('_')
				b.WriteByte(c + 32)
			}
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
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

// zeroableToString formats a Zeroable value without fmt.Sprintf reflection
// overhead for common types. Output is identical to fmt.Sprintf("%v", v).
func zeroableToString[T Zeroable](v T) string {
	switch vv := any(v).(type) {
	case string:
		return vv
	case int:
		return strconv.Itoa(vv)
	case int8:
		return strconv.FormatInt(int64(vv), 10)
	case int16:
		return strconv.FormatInt(int64(vv), 10)
	case int32:
		return strconv.FormatInt(int64(vv), 10)
	case int64:
		return strconv.FormatInt(vv, 10)
	case uint:
		return strconv.FormatUint(uint64(vv), 10)
	case uint8:
		return strconv.FormatUint(uint64(vv), 10)
	case uint16:
		return strconv.FormatUint(uint64(vv), 10)
	case uint32:
		return strconv.FormatUint(uint64(vv), 10)
	case uint64:
		return strconv.FormatUint(vv, 10)
	case uintptr:
		return strconv.FormatUint(uint64(vv), 10)
	case float32:
		// 'g' with shortest precision matches fmt's %v for floats.
		return strconv.FormatFloat(float64(vv), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(vv, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Unique
// data := []int{1, 2, 2, 3, 1, 4, 5, 3}
// unique := Unique(data)
// fmt.Println(unique) // [1 2 3 4 5]
func Unique[T Zeroable](items []T, patterns ...string) []T {
	seen := make(map[T]struct{}, len(items))
	result := make([]T, 0, len(items))
	var zero T
	pattern := OptionDefault(patterns, ``)
	if pattern == `` {
		for _, v := range items {
			if v == zero {
				continue
			}
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				result = append(result, v)
			}
		}
		return result
	}
	// Compile the filter pattern once instead of per element.
	// An invalid pattern filters out everything, matching the original
	// per-element RegexpCheck behavior (which returned false on error).
	re, err := cachedRegexp(pattern)
	if err != nil {
		slog.Error(`failed to compile regular expression.`, slog.String(`err`, err.Error()))
		return result
	}
	for _, v := range items {
		if v == zero {
			continue
		}
		if !re.MatchString(zeroableToString(v)) {
			continue
		}
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}
