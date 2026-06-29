package postgres

import (
	"context"
	"fmt"
	"time"
)

type TxFunc func(ctx context.Context, tx Querier) error

type TxFuncResult[T any] func(ctx context.Context, tx Querier) (T, error)

// WithTx executes fn within a transaction. If fn returns an error, the transaction
// is rolled back; otherwise it is committed.
func WithTx(ctx context.Context, b TxBeginner, fn TxFunc) error {
	tx, err := b.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback(ctx)
			panic(r)
		}
	}()
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// WithTxTimeout is like WithTx but applies a timeout to the context.
// The timeout covers the entire transaction: begin, fn execution, and commit.
func WithTxTimeout(ctx context.Context, b TxBeginner, timeout time.Duration, fn TxFunc) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return WithTx(ctx, b, fn)
}

// WithTxResult executes fn within a transaction and returns its result.
func WithTxResult[T any](ctx context.Context, b TxBeginner, fn TxFuncResult[T]) (T, error) {
	var result T
	tx, err := b.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback(ctx)
			panic(r)
		}
	}()
	result, err = fn(ctx, tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit tx: %w", err)
	}
	return result, nil
}

// WithTxResultTimeout is like WithTxResult but applies a timeout to the context.
// The timeout covers the entire transaction: begin, fn execution, and commit.
func WithTxResultTimeout[T any](ctx context.Context, b TxBeginner, timeout time.Duration, fn TxFuncResult[T]) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return WithTxResult[T](ctx, b, fn)
}
