package do

import (
	"errors"
	"log/slog"
	"math/rand"
	"testing"
	"time"
)

func TestOneOr(t *testing.T) {
	v1 := OneOr(222, 111)
	if v1 != 222 {
		t.Errorf(`value error`)
	}
	v2 := OneOr(``, `bbb`)
	if v2 != `bbb` {
		t.Errorf(`value error`)
	}
}

func GenError() error {
	r := rand.Int31n(5)
	if r == 1 || r == 2 || r == 3 || r == 4 {
		return errors.New(`random error`)
	}
	return nil
}

type Test struct {
	Name string
}

func TestReTry_Retry(t *testing.T) {
	retry := ReTry[Test]{}
	s := retry.Keep(func(t *Test) error {
		if err := GenError(); err != nil {
			return err
		}
		t.Name = `abc`
		return nil
	})
	slog.Default().Info(`test success`, slog.String(`value`, s.Name))
}

func TestReTry_Times(t *testing.T) {
	retry := ReTry[Test]{}
	s := retry.Times(2, func(t *Test) error {
		if err := GenError(); err != nil {
			return err
		}
		t.Name = `abc`
		return nil
	})
	slog.Default().Info(`test success`, slog.String(`value`, s.Name))
}

type TestPollerMockData struct {
	User string
	Id   int64
}

func TestPoller(t *testing.T) {
	poller := Poller(func(q *TestPollerMockData) error {
		//Here is a simulation of polling to read the database, which may read data or return an error
		randInt := rand.Int31n(3)
		if randInt == 0 {
			q.User = `user 1`
			q.Id = 1
		}
		if randInt == 1 {
			q.User = `user 2`
			q.Id = 2
		}
		if randInt >= 2 {
			return errors.New(`invalid id`)
		}
		return nil
	})
	poller.Setting(func(settings *PollerSetting) {
		settings.Interval = 1 * time.Second
	})
	poller.Then(func(result *TestPollerMockData) {
		slog.Default().Info(`read success`, slog.Int64(`user id`, result.Id), slog.String(`user name`, result.User))
	}).Catch(func(err error) {
		slog.Default().Error(`some error info`, slog.String(`error`, err.Error()))
	})

	time.Sleep(22 * time.Second)
	poller.Stop()
	time.Sleep(2 * time.Second)
}
