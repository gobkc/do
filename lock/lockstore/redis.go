package lockstore

import (
	"context"
	"time"

	"github.com/gobkc/do/lock"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(client *redis.Client) lock.Store {
	return &RedisStore{
		client: client,
	}
}

func NewRedisStoreByDsn(dsn string) (store lock.Store, err error) {
	opt, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opt)
	return NewRedisStore(client), nil
}

func (s *RedisStore) SetNx(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	res, err := s.client.SetArgs(ctx, key, owner, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err != nil {
		return false, err
	}

	return res == "OK", nil
}

func (s *RedisStore) Get(ctx context.Context, key string) (owner string, ttl time.Duration, err error) {
	// Pipeline GET+TTL into a single round trip. Individual command errors
	// are checked in order to preserve the original error precedence
	// (GET failure short-circuits, TTL failure reported next).
	pipe := s.client.Pipeline()
	getCmd := pipe.Get(ctx, key)
	ttlCmd := pipe.TTL(ctx, key)
	_, _ = pipe.Exec(ctx)
	val, err := getCmd.Result()
	if err != nil {
		return "", 0, err
	}
	ttl, err = ttlCmd.Result()
	if err != nil {
		return "", 0, err
	}
	return val, ttl, nil
}

var deleteScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)

func (s *RedisStore) Delete(ctx context.Context, key, owner string) error {
	_, err := deleteScript.Run(ctx, s.client, []string{key}, owner).Result()
	return err
}
