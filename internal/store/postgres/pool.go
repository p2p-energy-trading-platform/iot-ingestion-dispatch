package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

// PoolConfig bounds database use. Zero values keep pgx defaults.
type PoolConfig struct {
	MaxConns         int32
	MinConns         int32
	ConnectTimeout   time.Duration
	StatementTimeout time.Duration
	MaxConnLifetime  time.Duration
}

func New(
	context context.Context,
	databaseUrl string,
) (*Store, error) {
	return NewWithPool(context, databaseUrl, PoolConfig{})
}

// NewWithPool is New with explicit pool bounds and timeouts, so the number of
// simultaneous partition workers can never exhaust PostgreSQL connections.
func NewWithPool(
	context context.Context,
	databaseUrl string,
	pool PoolConfig,
) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	if pool.MaxConns > 0 {
		config.MaxConns = pool.MaxConns
	}
	if pool.MinConns > 0 {
		config.MinConns = pool.MinConns
	}
	if pool.MaxConnLifetime > 0 {
		config.MaxConnLifetime = pool.MaxConnLifetime
	}
	if pool.ConnectTimeout > 0 {
		config.ConnConfig.ConnectTimeout = pool.ConnectTimeout
	}
	if pool.StatementTimeout > 0 {
		if config.ConnConfig.RuntimeParams == nil {
			config.ConnConfig.RuntimeParams = map[string]string{}
		}
		config.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", pool.StatementTimeout.Milliseconds())
	}

	p, err := pgxpool.NewWithConfig(context, config)
	if err != nil {
		return nil, fmt.Errorf("create progress pool: %w", err)
	}

	if err := p.Ping(context); err != nil {
		p.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store := Store{
		pool: p,
	}

	return &store, nil
}

func (store *Store) Close() {
	store.pool.Close()
}

// Ping checks that PostgreSQL answers; used for dependency health reporting.
func (store *Store) Ping(ctx context.Context) error {
	return store.pool.Ping(ctx)
}

// PoolStats exposes pool usage for metrics.
func (store *Store) PoolStats() *pgxpool.Stat {
	return store.pool.Stat()
}
