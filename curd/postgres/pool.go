package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	curd "github.com/gobkc/do/curd"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultMaxIdleConns    = 10
	defaultMaxOpenConns    = 50
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// PoolOption is a functional option for configuring Pool.
type PoolOption func(*poolConfig)

type poolConfig struct {
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func defaultPoolConfig() *poolConfig {
	return &poolConfig{
		maxIdleConns:    defaultMaxIdleConns,
		maxOpenConns:    defaultMaxOpenConns,
		connMaxLifetime: defaultConnMaxLifetime,
		connMaxIdleTime: defaultConnMaxIdleTime,
	}
}

// WithMaxIdleConns sets the maximum number of idle connections in the pool.
func WithMaxIdleConns(n int) PoolOption {
	return func(c *poolConfig) { c.maxIdleConns = n }
}

// WithMaxOpenConns sets the maximum number of open connections in the pool.
func WithMaxOpenConns(n int) PoolOption {
	return func(c *poolConfig) { c.maxOpenConns = n }
}

// WithConnMaxLifetime sets the maximum lifetime of a connection.
func WithConnMaxLifetime(d time.Duration) PoolOption {
	return func(c *poolConfig) { c.connMaxLifetime = d }
}

// WithConnMaxIdleTime sets the maximum idle time of a connection.
func WithConnMaxIdleTime(d time.Duration) PoolOption {
	return func(c *poolConfig) { c.connMaxIdleTime = d }
}

type Pool struct {
	pool *pgxpool.Pool
}

func NewPool(dsn string, opts ...PoolOption) (*Pool, error) {
	cfg := defaultPoolConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	poolCfg.MaxConns = int32(cfg.maxOpenConns)
	poolCfg.MinConns = int32(cfg.maxIdleConns)
	poolCfg.MaxConnLifetime = cfg.connMaxLifetime
	poolCfg.MaxConnIdleTime = cfg.connMaxIdleTime

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	slog.Info("pgx pool created",
		slog.Int("maxConns", cfg.maxOpenConns),
		slog.Int("minConns", cfg.maxIdleConns),
	)
	return &Pool{pool: pool}, nil
}

func (p *Pool) Query(ctx context.Context, sql string, args ...any) (curd.Rows, error) {
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &rowsAdapter{Rows: rows}, nil
}

func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) curd.Row {
	return &rowAdapter{Row: p.pool.QueryRow(ctx, sql, args...)}
}

func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (curd.Result, error) {
	tag, err := p.pool.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &resultAdapter{CommandTag: tag}, nil
}

func (p *Pool) Begin(ctx context.Context) (curd.Tx, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &txAdapter{Tx: tx}, nil
}

// BeginTx starts a transaction with custom options (isolation level, access mode, etc.).
// Use this when you need SERIALIZABLE isolation, READ ONLY mode, or deferrable constraints.
//
// Usage:
//
//	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
//	if err != nil { ... }
//	defer tx.Rollback(ctx)
//	txC := c.WithQuerier(tx)
func (p *Pool) BeginTx(ctx context.Context, opts pgx.TxOptions) (curd.Tx, error) {
	tx, err := p.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &txAdapter{Tx: tx}, nil
}

func (p *Pool) Close() {
	p.pool.Close()
}

type rowsAdapter struct{ pgx.Rows }

type rowAdapter struct{ pgx.Row }

type resultAdapter struct {
	pgconn.CommandTag
}

func (r *resultAdapter) RowsAffected() int64 {
	return r.CommandTag.RowsAffected()
}

type txAdapter struct {
	pgx.Tx
}

func (t *txAdapter) Query(ctx context.Context, sql string, args ...any) (curd.Rows, error) {
	rows, err := t.Tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &rowsAdapter{Rows: rows}, nil
}

func (t *txAdapter) QueryRow(ctx context.Context, sql string, args ...any) curd.Row {
	return &rowAdapter{Row: t.Tx.QueryRow(ctx, sql, args...)}
}

func (t *txAdapter) Exec(ctx context.Context, sql string, args ...any) (curd.Result, error) {
	tag, err := t.Tx.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &resultAdapter{CommandTag: tag}, nil
}
