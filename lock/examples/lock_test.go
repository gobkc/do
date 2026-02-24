package examples

import (
	"context"
	"testing"
	"time"

	"github.com/gobkc/do/lock"
	"github.com/gobkc/do/lock/lockstore"
	"github.com/google/uuid"
)

func TestTryLock(t *testing.T) {
	store, err := lockstore.NewRedisStoreByDsn("redis://localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	owner := uuid.New().String()
	ok, err := lock.TryLock(context.Background(), store, "test", owner, time.Second*120)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected lock to be acquired")
	}
}
