package postgres

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	curd "github.com/gobkc/do/curd"
)

const testDSN = ""

// --- Test table struct ---

type integrationItem struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	Value       int        `json:"value"`
	Metadata    *string    `json:"metadata"`   // JSONB, nullable
	ConfigXML   *string    `json:"config_xml"` // XML, nullable
	CreatedDate time.Time  `json:"created_date"`
	ChangedDate time.Time  `json:"changed_date"`
	DeletedDate *time.Time `json:"deleted_date"`
}

// integrationItemWithJSONMap uses map[string]any for the JSONB field, meant to
// be used with JSONBMarshaler transformer.
type integrationItemWithJSONMap struct {
	ID          int64          `json:"id"`
	Name        string         `json:"name"`
	Value       int            `json:"value"`
	Metadata    map[string]any `json:"metadata"`
	CreatedDate time.Time      `json:"created_date"`
	ChangedDate time.Time      `json:"changed_date"`
	DeletedDate *time.Time     `json:"deleted_date"`
}

func (integrationItemWithJSONMap) TableName() string { return "curd_test_items" }

// integrationItemWithXMLMap uses a struct for the XML field, meant to be used
// with XMLMarshaler transformer.
type integrationItemWithXMLDoc struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	Value       int        `json:"value"`
	ConfigXML   any        `json:"config_xml"`
	CreatedDate time.Time  `json:"created_date"`
	ChangedDate time.Time  `json:"changed_date"`
	DeletedDate *time.Time `json:"deleted_date"`
}

func (integrationItemWithXMLDoc) TableName() string { return "curd_test_items" }

func (integrationItem) TableName() string { return "curd_test_items" }

// composedInsertItem holds both map and any-typed fields for testing ComposeTransformers.
type composedInsertItem struct {
	ID          int64          `json:"id"`
	Name        string         `json:"name"`
	Value       int            `json:"value"`
	Metadata    map[string]any `json:"metadata"`
	ConfigXML   any            `json:"config_xml"`
	CreatedDate time.Time      `json:"created_date"`
	ChangedDate time.Time      `json:"changed_date"`
	DeletedDate *time.Time     `json:"deleted_date"`
}

func (composedInsertItem) TableName() string { return "curd_test_items" }

// --- Setup / Teardown ---

var testPool *Pool

func TestMain(m *testing.M) {
	if testDSN == `` {
		return
	}
	pool, err := NewPool(testDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to test database: %v\n", err)
		os.Exit(1)
	}
	testPool = pool
	defer testPool.Close()

	if err := setupTestTable(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "cannot setup test table: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	cleanupTestTable(context.Background())
	os.Exit(code)
}

func setupTestTable(ctx context.Context) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS curd_test_items (
		id BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL DEFAULT '',
		value INT NOT NULL DEFAULT 0,
		metadata JSONB,
		config_xml XML,
		created_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		changed_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		deleted_date TIMESTAMPTZ
	);`
	_, err := testPool.Exec(ctx, ddl)
	return err
}

func cleanupTestTable(ctx context.Context) {
	testPool.Exec(ctx, "DROP TABLE IF EXISTS curd_test_items")
}

func truncateTable(t *testing.T) {
	t.Helper()
	_, err := testPool.Exec(context.Background(), "TRUNCATE TABLE curd_test_items RESTART IDENTITY")
	if err != nil {
		t.Fatalf("truncate table: %v", err)
	}
}

func newCurd() *curd.Curd[integrationItem] {
	return curd.New[integrationItem](testPool, nil, Dialect{})
}

// ============================================
// Insert Tests
// ============================================

func TestIntegrationInsertOne(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	row := &integrationItem{Name: "insert-one", Value: 42}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	if row.ID == 0 {
		t.Error("expected non-zero ID after insert")
	}
	if row.CreatedDate.IsZero() {
		t.Error("expected CreatedDate to be set")
	}
	if row.ChangedDate.IsZero() {
		t.Error("expected ChangedDate to be set")
	}

	found, err := c.FindByID(context.Background(), row.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.Name != "insert-one" {
		t.Errorf("expected name 'insert-one', got %q", found.Name)
	}
	if found.Value != 42 {
		t.Errorf("expected value 42, got %d", found.Value)
	}
}

func TestIntegrationInsertBatch(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	rows := []integrationItem{
		{Name: "batch-1", Value: 1},
		{Name: "batch-2", Value: 2},
		{Name: "batch-3", Value: 3},
	}
	err := c.InsertBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestIntegrationInsertBatchPtr(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	rows := []*integrationItem{
		{Name: "ptr-1", Value: 10},
		nil,
		{Name: "ptr-2", Value: 20},
	}
	err := c.InsertBatchPtr(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatchPtr: %v", err)
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows (nil becomes zero value), got %d", count)
	}
}

func TestIntegrationInsertWithJSONBTransformer(t *testing.T) {
	truncateTable(t)
	c := curd.New[integrationItemWithJSONMap](testPool, nil, Dialect{}).
		WithTransformer(curd.JSONBMarshaler("metadata"))

	row := &integrationItemWithJSONMap{
		Name:     "jsonb-test",
		Value:    100,
		Metadata: map[string]any{"env": "prod", "version": 2},
	}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne with JSONB transformer: %v", err)
	}

	// pgx returns JSONB as map[string]any when scanned into any
	results, err := curd.QueryRaw[struct {
		Metadata any `json:"metadata"`
	}](context.Background(), testPool,
		"SELECT metadata FROM curd_test_items WHERE id = $1", row.ID)
	if err != nil {
		t.Fatalf("QueryRaw: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	m, ok := results[0].Metadata.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T: %v", results[0].Metadata, results[0].Metadata)
	}
	if m["env"] != "prod" {
		t.Errorf("expected env=prod, got %v", m["env"])
	}
}

func TestIntegrationInsertWithXMLTransformer(t *testing.T) {
	truncateTable(t)
	c := curd.New[integrationItemWithXMLDoc](testPool, nil, Dialect{}).
		WithTransformer(curd.XMLMarshaler("config_xml"))

	type xmlDoc struct {
		XMLName struct{} `xml:"config"`
		Key     string   `xml:"key"`
		Val     string   `xml:"val"`
	}
	doc := xmlDoc{Key: "timeout", Val: "30s"}

	row := &integrationItemWithXMLDoc{
		Name:      "xml-test",
		Value:     200,
		ConfigXML: doc,
	}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne with XML transformer: %v", err)
	}

	// pgx returns XML as string when scanned into any
	results, err := curd.QueryRaw[struct {
		ConfigXML any `json:"config_xml"`
	}](context.Background(), testPool,
		"SELECT config_xml FROM curd_test_items WHERE id = $1", row.ID)
	if err != nil {
		t.Fatalf("QueryRaw: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	xmlStr := anyToString(results[0].ConfigXML)
	if xmlStr == "" {
		t.Fatalf("expected non-empty XML string, got %T: %v", results[0].ConfigXML, results[0].ConfigXML)
	}
	if !strings.Contains(xmlStr, "config") {
		t.Errorf("expected XML content, got %q", xmlStr)
	}
}

// ============================================
// Query Tests
// ============================================

func TestIntegrationFindAll(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 0; i < 5; i++ {
		err := c.InsertOne(context.Background(), &integrationItem{
			Name:  fmt.Sprintf("findall-%d", i),
			Value: i * 10,
		})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	results, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

func TestIntegrationFindAllWithWhere(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 0; i < 5; i++ {
		c.InsertOne(context.Background(), &integrationItem{
			Name:  fmt.Sprintf("item-%d", i),
			Value: i * 10,
		})
	}

	results, err := c.FindAll(context.Background(),
		curd.Eq("value", 20), "id DESC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll with where: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].Name != "item-2" {
		t.Errorf("expected 'item-2', got %q", results[0].Name)
	}
}

func TestIntegrationFindAllPagination(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 1; i <= 10; i++ {
		c.InsertOne(context.Background(), &integrationItem{
			Name:  fmt.Sprintf("page-%d", i),
			Value: i,
		})
	}

	results, err := c.FindAll(context.Background(), nil, "id ASC", 3, 2)
	if err != nil {
		t.Fatalf("FindAll with pagination: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	if results[0].Name != "page-3" {
		t.Errorf("expected 'page-3', got %q", results[0].Name)
	}
}

func TestIntegrationFindOne(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	c.InsertOne(context.Background(), &integrationItem{Name: "findone", Value: 99})

	result, err := c.FindOne(context.Background(), curd.Eq("name", "findone"))
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if result.Value != 99 {
		t.Errorf("expected value 99, got %d", result.Value)
	}

	_, err = c.FindOne(context.Background(), curd.Eq("name", "nonexistent"))
	if err == nil {
		t.Error("expected error for not found")
	}
}

func TestIntegrationFindByID(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	row := &integrationItem{Name: "byid", Value: 77}
	c.InsertOne(context.Background(), row)

	result, err := c.FindByID(context.Background(), row.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if result.Name != "byid" {
		t.Errorf("expected 'byid', got %q", result.Name)
	}
}

func TestIntegrationCount(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 0; i < 7; i++ {
		c.InsertOne(context.Background(), &integrationItem{Name: "c", Value: i})
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 7 {
		t.Errorf("expected count 7, got %d", count)
	}

	count, err = c.Count(context.Background(), curd.Eq("value", 5))
	if err != nil {
		t.Fatalf("Count with where: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestIntegrationExists(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	c.InsertOne(context.Background(), &integrationItem{Name: "ex", Value: 1})

	exists, err := c.Exists(context.Background(), curd.Eq("name", "ex"))
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected exists=true")
	}

	exists, err = c.Exists(context.Background(), curd.Eq("name", "no"))
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected exists=false")
	}
}

// ============================================
// Update Tests
// ============================================

func TestIntegrationUpdateByID(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	row := &integrationItem{Name: "original", Value: 10}
	c.InsertOne(context.Background(), row)

	err := c.UpdateByID(context.Background(), row.ID, map[string]any{
		"name":  "updated",
		"value": 99,
	})
	if err != nil {
		t.Fatalf("UpdateByID: %v", err)
	}

	found, err := c.FindByID(context.Background(), row.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.Name != "updated" {
		t.Errorf("expected name 'updated', got %q", found.Name)
	}
	if found.Value != 99 {
		t.Errorf("expected value 99, got %d", found.Value)
	}
}

func TestIntegrationUpdateWhere(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 0; i < 3; i++ {
		c.InsertOne(context.Background(), &integrationItem{
			Name:  "batch-update",
			Value: i,
		})
	}

	err := c.UpdateWhere(context.Background(),
		curd.Eq("name", "batch-update"),
		map[string]any{"value": 999},
	)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	results, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	for _, r := range results {
		if r.Value != 999 {
			t.Errorf("expected value 999, got %d", r.Value)
		}
	}
}

// ============================================
// Delete Tests
// ============================================

func TestIntegrationDeleteByIDSoft(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	row := &integrationItem{Name: "soft-del", Value: 1}
	c.InsertOne(context.Background(), row)

	err := c.DeleteByID(context.Background(), row.ID, false)
	if err != nil {
		t.Fatalf("DeleteByID soft: %v", err)
	}

	results, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results (soft deleted filtered), got %d", len(results))
	}
}

func TestIntegrationDeleteByIDHard(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	row := &integrationItem{Name: "hard-del", Value: 1}
	c.InsertOne(context.Background(), row)

	err := c.DeleteByID(context.Background(), row.ID, true)
	if err != nil {
		t.Fatalf("DeleteByID hard: %v", err)
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0 after hard delete, got %d", count)
	}
}

func TestIntegrationDeleteWhere(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 0; i < 5; i++ {
		c.InsertOne(context.Background(), &integrationItem{
			Name:  "del-where",
			Value: i,
		})
	}

	err := c.DeleteWhere(context.Background(), curd.Eq("name", "del-where"))
	if err != nil {
		t.Fatalf("DeleteWhere: %v", err)
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
}

// ============================================
// Transaction Tests
// ============================================

func TestIntegrationWithTxCommit(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	err := curd.WithTx(context.Background(), testPool, func(ctx context.Context, tx curd.Querier) error {
		txC := c.WithQuerier(tx)
		return txC.InsertOne(ctx, &integrationItem{Name: "tx-commit", Value: 1})
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1 after tx commit, got %d", count)
	}
}

func TestIntegrationWithTxRollback(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	testErr := errors.New("rollback me")
	err := curd.WithTx(context.Background(), testPool, func(ctx context.Context, tx curd.Querier) error {
		txC := c.WithQuerier(tx)
		if err := txC.InsertOne(ctx, &integrationItem{Name: "tx-rollback", Value: 1}); err != nil {
			return err
		}
		return testErr
	})
	if !errors.Is(err, testErr) {
		t.Fatalf("expected testErr, got %v", err)
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0 after rollback, got %d", count)
	}
}

func TestIntegrationWithTxMixedOperations(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	err := curd.WithTx(context.Background(), testPool, func(ctx context.Context, tx curd.Querier) error {
		txC := c.WithQuerier(tx)

		row := &integrationItem{Name: "mixed-1", Value: 100}
		if err := txC.InsertOne(ctx, row); err != nil {
			return err
		}

		if err := txC.UpdateByID(ctx, row.ID, map[string]any{"value": 200}); err != nil {
			return err
		}

		if err := txC.InsertOne(ctx, &integrationItem{Name: "mixed-2", Value: 300}); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("WithTx mixed: %v", err)
	}

	results, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Value != 200 {
		t.Errorf("expected value 200 for first row, got %d", results[0].Value)
	}
	if results[1].Value != 300 {
		t.Errorf("expected value 300 for second row, got %d", results[1].Value)
	}
}

func TestIntegrationWithTxResult(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	result, err := curd.WithTxResult[int](context.Background(), testPool, func(ctx context.Context, tx curd.Querier) (int, error) {
		txC := c.WithQuerier(tx)
		for i := 0; i < 3; i++ {
			if err := txC.InsertOne(ctx, &integrationItem{Name: "txr", Value: i}); err != nil {
				return 0, err
			}
		}
		return 3, nil
	})
	if err != nil {
		t.Fatalf("WithTxResult: %v", err)
	}
	if result != 3 {
		t.Errorf("expected result 3, got %d", result)
	}

	count, _ := c.Count(context.Background(), nil)
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

// ============================================
// SQL Logging Tests
// ============================================

func TestIntegrationSQLLogging(t *testing.T) {
	truncateTable(t)

	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	c := curd.New[integrationItem](testPool, nil, Dialect{}, curd.WithSQLLogging())

	row := &integrationItem{Name: "log-test", Value: 42}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "curd sql") {
		t.Error("expected 'curd sql' in log output")
	}
	if !strings.Contains(output, "INSERT INTO") {
		t.Error("expected INSERT INTO in log output")
	}
	t.Logf("log output: %s", output)
}

func TestIntegrationSQLLoggingPerOperation(t *testing.T) {
	truncateTable(t)

	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	c := newCurd()

	// This should NOT log
	err := c.InsertOne(context.Background(), &integrationItem{Name: "no-log", Value: 1})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	beforeOutput := buf.String()

	// This SHOULD log (per-operation toggle)
	err = c.WithSQLLog(true).InsertOne(context.Background(), &integrationItem{Name: "with-log", Value: 2})
	if err != nil {
		t.Fatalf("InsertOne with log: %v", err)
	}
	afterOutput := buf.String()
	if afterOutput == beforeOutput {
		t.Error("expected additional log output after WithSQLLog(true)")
	}
	if !strings.Contains(afterOutput, "curd sql") {
		t.Error("expected 'curd sql' in log output")
	}
}

func TestIntegrationGlobalSQLLogging(t *testing.T) {
	truncateTable(t)

	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	curd.SetGlobalSQLLog(true)
	defer curd.SetGlobalSQLLog(false)

	_, err := curd.ExecRaw(context.Background(), testPool,
		"SELECT 1 FROM curd_test_items LIMIT 0")
	if err != nil {
		t.Fatalf("ExecRaw: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "curd sql") {
		t.Error("expected 'curd sql' in global log output")
	}
}

// ============================================
// Raw Query Tests
// ============================================

func TestIntegrationQueryRaw(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	c.InsertOne(context.Background(), &integrationItem{Name: "raw-1", Value: 10})
	c.InsertOne(context.Background(), &integrationItem{Name: "raw-2", Value: 20})

	type rawResult struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}
	results, err := curd.QueryRaw[rawResult](context.Background(), testPool,
		"SELECT name, value FROM curd_test_items ORDER BY id")
	if err != nil {
		t.Fatalf("QueryRaw: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Name != "raw-1" {
		t.Errorf("expected 'raw-1', got %q", results[0].Name)
	}
}

func TestIntegrationQueryRowRaw(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	c.InsertOne(context.Background(), &integrationItem{Name: "row-raw", Value: 55})

	type rawResult struct {
		Value int `json:"value"`
	}
	result, err := curd.QueryRowRaw[rawResult](context.Background(), testPool,
		"SELECT value FROM curd_test_items WHERE name = $1", "row-raw")
	if err != nil {
		t.Fatalf("QueryRowRaw: %v", err)
	}
	if result.Value != 55 {
		t.Errorf("expected 55, got %d", result.Value)
	}
}

func TestIntegrationExecRaw(t *testing.T) {
	truncateTable(t)

	affected, err := curd.ExecRaw(context.Background(), testPool,
		"INSERT INTO curd_test_items (name, value) VALUES ($1, $2)", "exec-raw", 123)
	if err != nil {
		t.Fatalf("ExecRaw: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}
}

// ============================================
// Sequential Operations Tests
// ============================================

func TestIntegrationSequentialInsertAndQuery(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 1; i <= 20; i++ {
		err := c.InsertOne(context.Background(), &integrationItem{
			Name:  fmt.Sprintf("seq-%03d", i),
			Value: i * 10,
		})
		if err != nil {
			t.Fatalf("InsertOne %d: %v", i, err)
		}
	}

	all, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(all) != 20 {
		t.Errorf("expected 20 rows, got %d", len(all))
	}

	filtered, err := c.FindAll(context.Background(),
		curd.Eq("value", 50), "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll filtered: %v", err)
	}
	if len(filtered) != 1 {
		t.Errorf("expected 1 row with value=50, got %d", len(filtered))
	}
}

func TestIntegrationSequentialUpdateAndDelete(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 1; i <= 10; i++ {
		c.InsertOne(context.Background(), &integrationItem{
			Name:  fmt.Sprintf("sud-%d", i),
			Value: i,
		})
	}

	// Verify sequential IDs are 1..10 (they should be)
	all, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(all) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(all))
	}

	// Update rows with even Value (not even ID, since IDs are sequential)
	for _, r := range all {
		if r.Value%2 == 0 {
			err := c.UpdateByID(context.Background(), r.ID, map[string]any{"value": 999})
			if err != nil {
				t.Fatalf("UpdateByID %d: %v", r.ID, err)
			}
		}
	}

	// Soft delete rows with odd Value
	for _, r := range all {
		if r.Value%2 != 0 {
			err := c.DeleteByID(context.Background(), r.ID, false)
			if err != nil {
				t.Fatalf("DeleteByID %d: %v", r.ID, err)
			}
		}
	}

	// Remaining visible rows should be those with value=999 (originally even values)
	results, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 visible rows, got %d", len(results))
	}
	for _, r := range results {
		if r.Value != 999 {
			t.Errorf("expected value 999, got %d (id=%d)", r.Value, r.ID)
		}
	}
}

// ============================================
// Soft Delete Filtering Tests
// ============================================

func TestIntegrationSoftDeleteFiltering(t *testing.T) {
	truncateTable(t)
	c := newCurd()

	for i := 1; i <= 3; i++ {
		c.InsertOne(context.Background(), &integrationItem{Name: fmt.Sprintf("sf-%d", i), Value: i})
	}

	// Soft delete the middle one (id=2 since IDs are sequential)
	all, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(all) < 3 {
		t.Fatalf("expected at least 3 rows")
	}
	c.DeleteByID(context.Background(), all[1].ID, false)

	results, err := c.FindAll(context.Background(), nil, "id ASC", 0, 0)
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 visible, got %d", len(results))
	}

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	exists, err := c.Exists(context.Background(), curd.Eq("name", "sf-2"))
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("Exists should return false for soft-deleted row")
	}
}

// ============================================
// Composite Transformers Test
// ============================================

func TestIntegrationComposeTransformers(t *testing.T) {
	truncateTable(t)

	composed := curd.ComposeTransformers(
		curd.JSONBMarshaler("metadata"),
		curd.XMLMarshaler("config_xml"),
	)
	c := curd.New[composedInsertItem](testPool, nil, Dialect{}).WithTransformer(composed)

	type xmlDoc struct {
		XMLName struct{} `xml:"root"`
		Item    string   `xml:"item"`
	}

	row := &composedInsertItem{
		Name:      "composed-test",
		Value:     500,
		Metadata:  map[string]any{"count": 42},
		ConfigXML: xmlDoc{Item: "hello"},
	}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne with composed transformers: %v", err)
	}

	type rawResult struct {
		Metadata  any `json:"metadata"`
		ConfigXML any `json:"config_xml"`
	}
	row2, err := curd.QueryRowRaw[rawResult](context.Background(), testPool,
		"SELECT metadata, config_xml FROM curd_test_items WHERE id = $1", row.ID)
	if err != nil {
		t.Fatalf("QueryRowRaw: %v", err)
	}

	// Verify JSONB data
	m, ok := row2.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any for metadata, got %T: %v", row2.Metadata, row2.Metadata)
	}
	if m["count"] != float64(42) {
		t.Errorf("expected count=42, got %v", m["count"])
	}

	// Verify XML data
	xmlStr := anyToString(row2.ConfigXML)
	if xmlStr == "" {
		t.Fatalf("expected non-empty XML, got %T: %v", row2.ConfigXML, row2.ConfigXML)
	}
	if !strings.Contains(xmlStr, "root") && !strings.Contains(xmlStr, "hello") {
		t.Errorf("expected XML content, got %q", xmlStr)
	}
}

// ============================================
// Helpers
// ============================================

func mustXMLMarshal(v any) string {
	data, err := xml.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// anyToString converts an any value that might be string or []byte to a string.
func anyToString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
