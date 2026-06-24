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

type Pool struct {
	pool *pgxpool.Pool
}

func NewPool(dsn string, maxIdleConns, maxOpenConns int, connMaxLifetime int) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	cfg.MaxConns = int32(maxOpenConns)
	cfg.MinConns = int32(maxIdleConns)
	cfg.MaxConnLifetime = time.Duration(connMaxLifetime) * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
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
		slog.Int("maxConns", maxOpenConns),
		slog.Int("minConns", maxIdleConns),
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
