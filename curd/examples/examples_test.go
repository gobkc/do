package examples

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	curd "github.com/gobkc/do/curd"
)

// ============================================
// Mock implementations for demonstration
// ============================================

type mockDialect struct{}

func (mockDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

type mockRows struct {
	records [][]any
	pos     int
	closed  bool
	err     error
}

func (m *mockRows) Close()     { m.closed = true }
func (m *mockRows) Err() error { return m.err }
func (m *mockRows) Next() bool { m.pos++; return m.pos <= len(m.records) }
func (m *mockRows) Scan(dest ...any) error {
	if m.pos < 1 || m.pos > len(m.records) {
		return errors.New("no record")
	}
	for i, d := range dest {
		if ptr, ok := d.(*any); ok {
			*ptr = m.records[m.pos-1][i]
		}
	}
	return nil
}

type mockRow struct {
	record []any
	err    error
}

func (m *mockRow) Scan(dest ...any) error {
	if m.err != nil {
		return m.err
	}
	for i, d := range dest {
		if ptr, ok := d.(*any); ok {
			*ptr = m.record[i]
		}
	}
	return nil
}

type mockResult struct{ rowsAffected int64 }

func (m *mockResult) RowsAffected() int64 { return m.rowsAffected }

type mockQuerier struct {
	queryRows  curd.Rows
	queryRow   curd.Row
	queryErr   error
	execResult curd.Result
	execErr    error
}

func (m *mockQuerier) Query(_ context.Context, _ string, _ ...any) (curd.Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return m.queryRows, nil
}

func (m *mockQuerier) QueryRow(_ context.Context, _ string, _ ...any) curd.Row {
	return m.queryRow
}

func (m *mockQuerier) Exec(_ context.Context, _ string, _ ...any) (curd.Result, error) {
	if m.execErr != nil {
		return nil, m.execErr
	}
	return m.execResult, nil
}

type mockTx struct {
	curd.Querier
	commitErr   error
	rollbackErr error
	committed   bool
	rolledBack  bool
}

func (m *mockTx) Commit(_ context.Context) error {
	m.committed = true
	return m.commitErr
}

func (m *mockTx) Rollback(_ context.Context) error {
	m.rolledBack = true
	return m.rollbackErr
}

type mockTxBeginner struct {
	tx  curd.Tx
	err error
}

func (m *mockTxBeginner) Begin(_ context.Context) (curd.Tx, error) { return m.tx, m.err }

// ============================================
// Example entity types
// ============================================

type User struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Email       string `json:"email"`
	CreatedDate string `json:"created_date"`
	DeletedDate string `json:"deleted_date"`
}

func (User) TableName() string { return "users" }

type Product struct {
	ID       int64  `gorm:"column:product_id;primaryKey"`
	Title    string `gorm:"column:title"`
	Price    int    `gorm:"column:price"`
	Internal string `gorm:"-"`
}

func (Product) TableName() string { return "products" }

// ============================================
// Test: Basic CRUD Operations
// ============================================

func TestExampleBasicCRUD(t *testing.T) {
	ctx := context.Background()

	// --- FindAll ---
	t.Run("FindAll", func(t *testing.T) {
		mock := &mockQuerier{
			queryRows: &mockRows{
				records: [][]any{
					{int64(1), "alice", "alice@example.com", "2024-01-01", ""},
					{int64(2), "bob", "bob@example.com", "2024-02-01", ""},
				},
			},
		}
		c := curd.New[User](mock, nil, mockDialect{})
		results, err := c.FindAll(ctx, nil, "id DESC", 0, 0)
		if err != nil {
			t.Fatalf("FindAll failed: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 users, got %d", len(results))
		}
		t.Logf("Found %d users: %+v, %+v", len(results), results[0], results[1])
	})

	// --- FindOne ---
	t.Run("FindOne", func(t *testing.T) {
		mock := &mockQuerier{
			queryRows: &mockRows{
				records: [][]any{{int64(1), "alice", "alice@example.com", "2024-01-01", ""}},
			},
		}
		c := curd.New[User](mock, nil, mockDialect{})
		user, err := c.FindOne(ctx, curd.Eq("name", "alice"))
		if err != nil {
			t.Fatalf("FindOne failed: %v", err)
		}
		t.Logf("Found user: %+v", user)
	})

	// --- FindByID ---
	t.Run("FindByID", func(t *testing.T) {
		mock := &mockQuerier{
			queryRows: &mockRows{
				records: [][]any{{int64(42), "answer", "42@example.com", "now", ""}},
			},
		}
		c := curd.New[User](mock, nil, mockDialect{})
		user, err := c.FindByID(ctx, 42)
		if err != nil {
			t.Fatalf("FindByID failed: %v", err)
		}
		if user.ID != 42 {
			t.Errorf("expected ID 42, got %d", user.ID)
		}
		t.Logf("Found by ID: %+v", user)
	})

	// --- InsertOne ---
	t.Run("InsertOne", func(t *testing.T) {
		mock := &mockQuerier{
			queryRow:   &mockRow{record: []any{int64(100)}},
			execResult: &mockResult{rowsAffected: 1},
		}
		c := curd.New[User](mock, nil, mockDialect{})
		user := &User{Name: "charlie", Email: "charlie@example.com"}
		if err := c.InsertOne(ctx, user); err != nil {
			t.Fatalf("InsertOne failed: %v", err)
		}
		t.Logf("Inserted user with auto-generated ID: %d", user.ID)
	})

	// --- InsertBatch ---
	t.Run("InsertBatch", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 3}}
		c := curd.New[User](mock, nil, mockDialect{})
		users := []User{
			{Name: "dave", Email: "dave@example.com"},
			{Name: "eve", Email: "eve@example.com"},
			{Name: "frank", Email: "frank@example.com"},
		}
		if err := c.InsertBatch(ctx, users); err != nil {
			t.Fatalf("InsertBatch failed: %v", err)
		}
		t.Logf("Batch inserted %d users", len(users))
	})

	// --- UpdateByID ---
	t.Run("UpdateByID", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
		c := curd.New[User](mock, nil, mockDialect{})
		err := c.UpdateByID(ctx, 1, map[string]any{"name": "alice-updated", "email": "new@example.com"})
		if err != nil {
			t.Fatalf("UpdateByID failed: %v", err)
		}
		t.Log("Updated user by ID successfully")
	})

	// --- UpdateWhere ---
	t.Run("UpdateWhere", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 5}}
		c := curd.New[User](mock, nil, mockDialect{})
		err := c.UpdateWhere(ctx, curd.Eq("email", ""), map[string]any{"email": "unknown@example.com"})
		if err != nil {
			t.Fatalf("UpdateWhere failed: %v", err)
		}
		t.Log("Updated users by condition")
	})

	// --- DeleteByID (soft delete) ---
	t.Run("DeleteByID_Soft", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
		c := curd.New[User](mock, nil, mockDialect{})
		err := c.DeleteByID(ctx, 1, false)
		if err != nil {
			t.Fatalf("DeleteByID (soft) failed: %v", err)
		}
		t.Log("Soft-deleted user by ID")
	})

	// --- DeleteByID (hard delete) ---
	t.Run("DeleteByID_Hard", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
		c := curd.New[User](mock, nil, mockDialect{})
		err := c.DeleteByID(ctx, 99, true)
		if err != nil {
			t.Fatalf("DeleteByID (hard) failed: %v", err)
		}
		t.Log("Hard-deleted user by ID")
	})

	// --- DeleteWhere ---
	t.Run("DeleteWhere", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 3}}
		c := curd.New[User](mock, nil, mockDialect{})
		err := c.DeleteWhere(ctx, curd.Eq("email", ""))
		if err != nil {
			t.Fatalf("DeleteWhere failed: %v", err)
		}
		t.Log("Deleted users by condition")
	})

	// --- Count ---
	t.Run("Count", func(t *testing.T) {
		mock := &mockQuerier{
			queryRow: &mockRow{record: []any{int64(42)}},
		}
		c := curd.New[User](mock, nil, mockDialect{})
		count, err := c.Count(ctx, nil)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		t.Logf("User count: %d", count)
	})

	// --- Exists ---
	t.Run("Exists", func(t *testing.T) {
		mock := &mockQuerier{
			queryRow: &mockRow{record: []any{true}},
		}
		c := curd.New[User](mock, nil, mockDialect{})
		exists, err := c.Exists(ctx, curd.Eq("name", "alice"))
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		t.Logf("User exists: %v", exists)
	})
}

// ============================================
// Test: Transactions
// ============================================

func TestExampleTransactions(t *testing.T) {
	ctx := context.Background()

	// --- WithTx (success) ---
	t.Run("WithTx_Success", func(t *testing.T) {
		q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
		tx := &mockTx{Querier: q}
		tb := &mockTxBeginner{tx: tx}

		err := curd.WithTx(ctx, tb, func(ctx context.Context, tx curd.Querier) error {
			// Use tx.Exec, tx.Query, etc. within the transaction.
			_, err := tx.Exec(ctx, "UPDATE users SET name=$1 WHERE id=$2", "new_name", 1)
			return err
		})
		if err != nil {
			t.Fatalf("WithTx failed: %v", err)
		}
		if !tx.committed {
			t.Error("transaction should be committed")
		}
		t.Log("Transaction committed successfully")
	})

	// --- WithTx (rollback on error) ---
	t.Run("WithTx_Rollback", func(t *testing.T) {
		q := &mockQuerier{execResult: &mockResult{rowsAffected: 0}}
		tx := &mockTx{Querier: q}
		tb := &mockTxBeginner{tx: tx}

		sentinel := errors.New("business error")
		err := curd.WithTx(ctx, tb, func(ctx context.Context, tx curd.Querier) error {
			return sentinel
		})
		if err == nil {
			t.Error("expected error from fn")
		}
		if !tx.rolledBack {
			t.Error("transaction should be rolled back")
		}
		t.Logf("Transaction rolled back (expected): %v", err)
	})

	// --- WithTxResult ---
	t.Run("WithTxResult", func(t *testing.T) {
		q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
		tx := &mockTx{Querier: q}
		tb := &mockTxBeginner{tx: tx}

		result, err := curd.WithTxResult[int](ctx, tb, func(ctx context.Context, tx curd.Querier) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Fatalf("WithTxResult failed: %v", err)
		}
		if result != 42 {
			t.Errorf("expected 42, got %d", result)
		}
		if !tx.committed {
			t.Error("transaction should be committed")
		}
		t.Logf("WithTxResult returned: %d", result)
	})
}

// ============================================
// Test: Field Transformers (JSONB / XML)
// ============================================

type Document struct {
	ID       int64          `json:"id"`
	Metadata map[string]any `json:"metadata"`
	Config   map[string]any `json:"config"`
}

func (Document) TableName() string { return "documents" }

func TestExampleFieldTransformers(t *testing.T) {
	ctx := context.Background()

	// --- JSONBMarshaler ---
	t.Run("JSONBMarshaler", func(t *testing.T) {
		mock := &mockQuerier{
			queryRow:   &mockRow{record: []any{int64(1)}},
			execResult: &mockResult{rowsAffected: 1},
		}
		c := curd.New[Document](mock, nil, mockDialect{}).
			WithTransformer(curd.JSONBMarshaler("metadata"))

		doc := &Document{
			Metadata: map[string]any{"version": 1, "env": "prod"},
		}
		if err := c.InsertOne(ctx, doc); err != nil {
			t.Fatalf("InsertOne with JSONB transformer failed: %v", err)
		}
		t.Logf("Inserted document with JSONB-marshaled metadata (ID=%d)", doc.ID)
	})

	// --- XMLMarshaler ---
	t.Run("XMLMarshaler", func(t *testing.T) {
		mock := &mockQuerier{
			queryRow:   &mockRow{record: []any{int64(2)}},
			execResult: &mockResult{rowsAffected: 1},
		}
		c := curd.New[Document](mock, nil, mockDialect{}).
			WithTransformer(curd.XMLMarshaler("config"))

		doc := &Document{
			Config: map[string]any{"key": "value"},
		}
		if err := c.InsertOne(ctx, doc); err != nil {
			t.Fatalf("InsertOne with XML transformer failed: %v", err)
		}
		t.Logf("Inserted document with XML-marshaled config (ID=%d)", doc.ID)
	})

	// --- ComposeTransformers ---
	t.Run("ComposeTransformers", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}
		composed := curd.ComposeTransformers(
			curd.JSONBMarshaler("metadata"),
			curd.XMLMarshaler("config"),
		)
		c := curd.New[Document](mock, nil, mockDialect{}).
			WithTransformer(composed)

		docs := []Document{
			{Metadata: map[string]any{"a": 1}, Config: map[string]any{"x": true}},
			{Metadata: map[string]any{"b": 2}, Config: map[string]any{"y": false}},
		}
		if err := c.InsertBatch(ctx, docs); err != nil {
			t.Fatalf("InsertBatch with composed transformers failed: %v", err)
		}
		t.Logf("Batch inserted %d documents with composed transformers", len(docs))
	})
}

// ============================================
// Test: Raw Queries
// ============================================

type ReportRow struct {
	Total  int64  `json:"total"`
	Label  string `json:"label"`
	Status string `json:"status"`
}

func TestExampleRawQueries(t *testing.T) {
	ctx := context.Background()

	// --- QueryRaw ---
	t.Run("QueryRaw", func(t *testing.T) {
		mock := &mockQuerier{
			queryRows: &mockRows{
				records: [][]any{
					{int64(100), "sales", "active"},
					{int64(200), "refunds", "inactive"},
				},
			},
		}
		results, err := curd.QueryRaw[ReportRow](ctx, mock,
			"SELECT total, label, status FROM reports WHERE status=$1", "active")
		if err != nil {
			t.Fatalf("QueryRaw failed: %v", err)
		}
		t.Logf("QueryRaw returned %d rows: %+v", len(results), results)
	})

	// --- QueryRowRaw ---
	t.Run("QueryRowRaw", func(t *testing.T) {
		mock := &mockQuerier{
			queryRow: &mockRow{record: []any{int64(42), "answer", "ok"}},
		}
		row, err := curd.QueryRowRaw[ReportRow](ctx, mock,
			"SELECT total, label, status FROM reports LIMIT 1")
		if err != nil {
			t.Fatalf("QueryRowRaw failed: %v", err)
		}
		t.Logf("QueryRowRaw returned: %+v", row)
	})

	// --- ExecRaw ---
	t.Run("ExecRaw", func(t *testing.T) {
		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 7}}
		n, err := curd.ExecRaw(ctx, mock, "UPDATE reports SET status=$1 WHERE total < $2", "archived", 50)
		if err != nil {
			t.Fatalf("ExecRaw failed: %v", err)
		}
		t.Logf("ExecRaw affected %d rows", n)
	})
}

// ============================================
// Test: Custom FieldMapper
// ============================================

// upperFieldMapper implements curd.FieldMapper by returning uppercase Go field names
// as column names. This demonstrates how to supply a fully custom column-naming
// strategy instead of the default json/gorm tag + snake_case mapping.
type upperFieldMapper struct{}

func (upperFieldMapper) ColumnName(f reflect.StructField) string {
	return strings.ToUpper(f.Name)
}

type Item struct {
	ID    int64  `json:"id"`
	Value string `json:"value"`
}

func (Item) TableName() string { return "items" }

func TestExampleCustomFieldMapper(t *testing.T) {
	ctx := context.Background()

	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "hello"}},
		},
	}
	// Use a custom FieldMapper that maps Go field names to uppercase column names.
	c := curd.New[Item](mock, upperFieldMapper{}, mockDialect{})
	results, err := c.FindAll(ctx, nil, "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll failed: %v", err)
	}
	t.Logf("Custom mapper example — found %d items", len(results))
}

// ============================================
// Test: SQL Logging
// ============================================

func TestExampleSQLLogging(t *testing.T) {
	ctx := context.Background()

	// --- WithSQLLogging (instance-level) ---
	t.Run("WithSQLLogging", func(t *testing.T) {
		// Redirect slog output to a buffer for verification.
		var buf strings.Builder
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
		defer slog.SetDefault(prev)

		mock := &mockQuerier{
			queryRow:   &mockRow{record: []any{int64(1)}},
			execResult: &mockResult{rowsAffected: 1},
		}
		c := curd.New[User](mock, nil, mockDialect{}, curd.WithSQLLogging())
		user := &User{Name: "logger", Email: "log@example.com"}
		if err := c.InsertOne(ctx, user); err != nil {
			t.Fatalf("InsertOne failed: %v", err)
		}

		output := buf.String()
		if !strings.Contains(output, "curd sql") {
			t.Error("expected SQL log output containing 'curd sql'")
		}
		if !strings.Contains(output, "INSERT INTO") {
			t.Error("expected SQL log output containing 'INSERT INTO'")
		}
		t.Logf("SQL log output captured (%d bytes)", len(output))
	})

	// --- WithSQLLog (per-operation toggle) ---
	t.Run("WithSQLLog", func(t *testing.T) {
		var buf strings.Builder
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
		defer slog.SetDefault(prev)

		mock := &mockQuerier{
			queryRow:   &mockRow{record: []any{int64(2)}},
			execResult: &mockResult{rowsAffected: 1},
		}
		// Create without logging, then enable per-operation.
		c := curd.New[User](mock, nil, mockDialect{})
		user := &User{Name: "per-op-log", Email: "perop@example.com"}
		if err := c.WithSQLLog(true).InsertOne(ctx, user); err != nil {
			t.Fatalf("InsertOne failed: %v", err)
		}

		output := buf.String()
		if !strings.Contains(output, "curd sql") {
			t.Error("expected SQL log from per-operation toggle")
		}
		t.Logf("Per-operation SQL log captured (%d bytes)", len(output))
	})

	// --- SetGlobalSQLLog (standalone functions) ---
	t.Run("SetGlobalSQLLog", func(t *testing.T) {
		var buf strings.Builder
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
		defer slog.SetDefault(prev)

		curd.SetGlobalSQLLog(true)
		defer curd.SetGlobalSQLLog(false)

		mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
		_, err := curd.ExecRaw(ctx, mock, "DELETE FROM users WHERE id=$1", 999)
		if err != nil {
			t.Fatalf("ExecRaw failed: %v", err)
		}

		output := buf.String()
		if !strings.Contains(output, "curd sql") {
			t.Error("expected SQL log from global toggle")
		}
		t.Logf("Global SQL log captured (%d bytes)", len(output))
	})
}

// ============================================
// Test: WithQuerier (transaction integration)
// ============================================

func TestExampleWithQuerier(t *testing.T) {
	ctx := context.Background()

	pool := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "pool_user", "pool@example.com", "now", ""}},
		},
	}
	txQ := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(99), "tx_user", "tx@example.com", "now", ""}},
		},
	}

	c := curd.New[User](pool, nil, mockDialect{})

	// Query using the original pool-backed Curd.
	results, err := c.FindAll(ctx, nil, "", 0, 0)
	if err != nil {
		t.Fatalf("pool FindAll failed: %v", err)
	}
	t.Logf("Pool query returned user ID=%d", results[0].ID)

	// Switch to a transaction-backed Querier without affecting the original.
	txC := c.WithQuerier(txQ)
	txResults, err := txC.FindAll(ctx, nil, "", 0, 0)
	if err != nil {
		t.Fatalf("tx FindAll failed: %v", err)
	}
	t.Logf("Transaction query returned user ID=%d", txResults[0].ID)

	// Original Curd is unchanged.
	if results[0].ID != 1 {
		t.Errorf("original Curd should be unchanged, got ID=%d", results[0].ID)
	}
}

// ============================================
// Test: GORM-style tag mapping
// ============================================

func TestExampleGormTags(t *testing.T) {
	ctx := context.Background()

	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "Widget", int(99)}},
		},
	}
	// The default FieldMapper understands gorm column: tags.
	c := curd.New[Product](mock, nil, mockDialect{})
	results, err := c.FindAll(ctx, nil, "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll with gorm tags failed: %v", err)
	}
	if results[0].ID != 1 || results[0].Title != "Widget" {
		t.Errorf("unexpected product: %+v", results[0])
	}
	t.Logf("GORM-tagged product: ID=%d, Title=%s, Price=%d", results[0].ID, results[0].Title, results[0].Price)
}
