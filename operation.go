package do

import (
	"bytes"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"reflect"
	"regexp"
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
type LeaderPoller[T any] struct {
	interval    time.Duration
	redisClient *redis.Client
	subject     string
	key         string
	delayed     bool
	conditions  []func() (T, error)
}

type LeaderPollerSetting struct {
	Client   *redis.Client
	Interval time.Duration
	Subject  string
	Key      string
	Delayed  bool
}

func NewLeaderPoller[T any](fs func(setting *LeaderPollerSetting)) *LeaderPoller[T] {
	setting := &LeaderPollerSetting{
		Interval: 10 * time.Second,
		Subject:  "NewLeaderPoller",
	}
	fs(setting)
	if setting.Client == nil {
		panic(`NewLeaderPoller failed: redis client is nil`)
	}
	now := time.Now().Unix()
	key := fmt.Sprintf(`%v`, now)
	setting.Client.Set(context.Background(), setting.Subject, key, 0)
	leader := &LeaderPoller[T]{
		interval:    setting.Interval,
		redisClient: setting.Client,
		subject:     setting.Subject,
		key:         key,
		delayed:     setting.Delayed,
		conditions:  []func() (T, error){},
	}
	leader.intervalResetKey()
	return leader
}

func (lp *LeaderPoller[T]) intervalResetKey() {
	go func() {
		for {
			now := time.Now().Unix()
			key := fmt.Sprintf(`%v`, now)
			lp.redisClient.Set(context.Background(), lp.subject, key, 0)
			lp.key = key
			time.Sleep(10 * time.Minute)
		}
	}()

}

func (lp *LeaderPoller[T]) Conditions(conditionFunctions ...func() (T, error)) {
	lp.conditions = append(lp.conditions, conditionFunctions...)
}

func (lp *LeaderPoller[T]) Run(task func(T)) {
	for {
		if !lp.delayed {
			time.Sleep(lp.interval)
		}
		key, err := lp.redisClient.Get(context.Background(), lp.subject).Result()
		if err != nil || key != lp.key {
			slog.Warn(`the lock has been used`, slog.String(`subject`, lp.subject), slog.String(`current key`, lp.key), slog.String(`using key`, key))
			time.Sleep(lp.interval)
			continue
		}
		// Check conditions
		pollerContext, err := lp.checkConditions()
		if err != nil {
			slog.Error(`conditions not met,retrying...`, slog.String(`error`, err.Error()))
			time.Sleep(lp.interval)
			continue
		}

		// If conditions are met, execute the task
		slog.Info(`conditions met,running tasks...`)
		task(pollerContext)

		// Sleep for the polling interval
		if lp.delayed {
			time.Sleep(lp.interval)
		}
	}
}

func (lp *LeaderPoller[T]) checkConditions() (t T, err error) {
	t = *new(T)
	for _, condition := range lp.conditions {
		t, err = condition()
		if err != nil {
			t = *new(T)
			return t, err
		}
	}

	return t, nil
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

func ReplaceMap(s string, replace map[string]string) (result string, err error) {
	result = s
	if replace == nil {
		replace = make(map[string]string)
	}
	tmpl, err := template.New("soapRequest").Parse(s)
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
