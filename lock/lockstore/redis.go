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
	val, err := s.client.Get(ctx, key).Result()
	if err != nil {
		return "", 0, err
	}
	ttl, err = s.client.TTL(ctx, key).Result()
	if err != nil {
		return "", 0, err
	}
	return val, ttl, nil
}

func (s *RedisStore) Delete(ctx context.Context, key, owner string) error {
	script := redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)
	_, err := script.Run(ctx, s.client, []string{key}, owner).Result()
	return err
}
