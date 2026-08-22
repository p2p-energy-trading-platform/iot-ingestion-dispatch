package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

type Store struct {
	client *goredis.Client
}

type Config struct {
	Address  string
	Password string
	DB       int
}

func New(
	context context.Context,
	config Config,
) (*Store, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:     config.Address,
		Password: config.Password,
		DB:       config.DB,
	})

	if err := client.Ping(context).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	store := Store{
		client: client,
	}

	return &store, nil
}

func (store *Store) Close() error {
	return store.client.Close()
}
