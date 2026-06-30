package curd

import "context"

type Rows interface {
	Close()
	Next() bool
	Scan(dest ...any) error
	Err() error
}

type Row interface {
	Scan(dest ...any) error
}

type Result interface {
	RowsAffected() int64
}

type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Exec(ctx context.Context, sql string, args ...any) (Result, error)
}

type Tx interface {
	Querier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type TxBeginner interface {
	Begin(ctx context.Context) (Tx, error)
}
