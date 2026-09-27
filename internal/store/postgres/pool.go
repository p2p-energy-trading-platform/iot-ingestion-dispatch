package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(
	context context.Context,
	databaseUrl string,
) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context, config)
	if err != nil {
		return nil, fmt.Errorf("create progress pool: %w", err)
	}

	if err := pool.Ping(context); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store := Store{
		pool: pool,
	}

	return &store, nil
}

func (store *Store) Close() {
	store.pool.Close()
}
