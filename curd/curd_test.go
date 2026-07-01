package curd

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// --- Mock types ---

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
	rec := m.records[m.pos-1]
	for i, d := range dest {
		mockScanSet(d, rec[i])
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
		mockScanSet(d, m.record[i])
	}
	return nil
}

func mockScanSet(dest any, val any) {
	if ptr, ok := dest.(*any); ok {
		*ptr = val
		return
	}
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr {
		return
	}
	rv := reflect.ValueOf(val)
	de := dv.Elem()
	if rv.Type().AssignableTo(de.Type()) {
		de.Set(rv)
	} else if rv.Type().ConvertibleTo(de.Type()) {
		de.Set(rv.Convert(de.Type()))
	}
}

type mockResult struct{ rowsAffected int64 }

func (m *mockResult) RowsAffected() int64 { return m.rowsAffected }

type mockQuerier struct {
	queryRows  Rows
	queryRow   Row
	queryErr   error
	execResult Result
	execErr    error
}

func (m *mockQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return m.queryRows, nil
}

func (m *mockQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return m.queryRow
}

func (m *mockQuerier) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	if m.execErr != nil {
		return nil, m.execErr
	}
	return m.execResult, nil
}

// upsertMock is a Querier that returns different QueryRow results on each call,
// supporting the Exists-then-InsertOne or Exists-then-UpdateWhere pattern used by Upsert.
type upsertMock struct {
	existsVal   bool
	insertID    int64
	updateOK    bool
	queryErr    error
	execErr     error
	callCount   int
}

func (m *upsertMock) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return &mockRows{records: [][]any{}}, nil
}

func (m *upsertMock) QueryRow(ctx context.Context, sql string, args ...any) Row {
	if m.queryErr != nil {
		return &mockRow{err: m.queryErr}
	}
	m.callCount++
	if m.callCount == 1 {
		// Exists call
		return &mockRow{record: []any{m.existsVal}}
	}
	// InsertOne RETURNING id call
	return &mockRow{record: []any{m.insertID}}
}

func (m *upsertMock) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	if m.execErr != nil {
		return nil, m.execErr
	}
	return &mockResult{rowsAffected: 1}, nil
}

type mockTxBeginner struct {
	tx  Tx
	err error
}

func (m *mockTxBeginner) Begin(ctx context.Context) (Tx, error) { return m.tx, m.err }

type mockTx struct {
	Querier
	commitErr   error
	rollbackErr error
	committed   bool
	rolledBack  bool
}

func (m *mockTx) Commit(ctx context.Context) error {
	m.committed = true
	return m.commitErr
}

func (m *mockTx) Rollback(ctx context.Context) error {
	m.rolledBack = true
	return m.rollbackErr
}

// --- Test entity types ---

type testTable struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Age         int     `json:"age"`
	CreatedDate string  `json:"created_date"`
	DeletedDate *string `json:"deleted_date"`
}

func (testTable) TableName() string { return "test_table" }

type testTableGorm struct {
	ID      int64  `gorm:"column:gid;primaryKey"`
	Title   string `gorm:"column:title_name"`
	Content string `gorm:"-"`
	Status  int    `gorm:"column:status_code"`
}

func (testTableGorm) TableName() string { return "test_gorm" }

type testTableNoTag struct {
	FullName   string
	EmailAddr  string
	InternalID int
	SkipMe     string `json:"-"`
}

func (testTableNoTag) TableName() string { return "test_no_tag" }

type noIDTable struct {
	Name string `json:"name"`
}

func (noIDTable) TableName() string { return "no_id_table" }

type testTableWithTime struct {
	ID          int64      `json:"id"`
	CreatedDate time.Time  `json:"created_date"`
	ChangedDate time.Time  `json:"changed_date"`
	DeletedDate *time.Time `json:"deleted_date"`
}

func (testTableWithTime) TableName() string { return "test_time" }

type testTableWithJSONB struct {
	ID       int64          `json:"id"`
	Metadata map[string]any `json:"metadata"`
	Config   map[string]any `json:"config"`
}

func (testTableWithJSONB) TableName() string { return "test_jsonb" }

type testTableWithXML struct {
	ID       int64  `json:"id"`
	Document string `json:"document"`
}

func (testTableWithXML) TableName() string { return "test_xml" }

// pointerTable implements Table with a pointer receiver.
type pointerTable struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func (p *pointerTable) TableName() string { return "pointer_table" }

// ============================================
// Predicate / Condition Tests
// ============================================

func TestBuildPredicateNil(t *testing.T) {
	clause, args := buildPredicate(nil, mockDialect{})
	if clause != "" {
		t.Errorf("expected empty clause for nil predicate, got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateEq(t *testing.T) {
	clause, args := buildPredicate(Eq("name", "test"), mockDialect{})
	if clause != "name = $1" {
		t.Errorf("expected 'name = $1', got %q", clause)
	}
	if len(args) != 1 || args[0] != "test" {
		t.Errorf("expected args [test], got %v", args)
	}
}

func TestPredicateNe(t *testing.T) {
	clause, args := buildPredicate(Ne("status", "deleted"), mockDialect{})
	if !strings.Contains(clause, "!=") {
		t.Errorf("expected != in clause, got %q", clause)
	}
	if len(args) != 1 || args[0] != "deleted" {
		t.Errorf("expected args [deleted], got %v", args)
	}
}

func TestPredicateGt(t *testing.T) {
	clause, args := buildPredicate(Gt("age", 18), mockDialect{})
	if !strings.Contains(clause, ">") {
		t.Errorf("expected > in clause, got %q", clause)
	}
	if len(args) != 1 || args[0] != 18 {
		t.Errorf("expected args [18], got %v", args)
	}
}

func TestPredicateGte(t *testing.T) {
	clause, args := buildPredicate(Gte("score", 60), mockDialect{})
	if !strings.Contains(clause, ">=") {
		t.Errorf("expected >= in clause, got %q", clause)
	}
	if len(args) != 1 || args[0] != 60 {
		t.Errorf("expected args [60], got %v", args)
	}
}

func TestPredicateLt(t *testing.T) {
	clause, args := buildPredicate(Lt("count", 100), mockDialect{})
	if !strings.Contains(clause, "<") {
		t.Errorf("expected < in clause, got %q", clause)
	}
	if len(args) != 1 || args[0] != 100 {
		t.Errorf("expected args [100], got %v", args)
	}
}

func TestPredicateLte(t *testing.T) {
	clause, args := buildPredicate(Lte("limit", 50), mockDialect{})
	if !strings.Contains(clause, "<=") {
		t.Errorf("expected <= in clause, got %q", clause)
	}
	if len(args) != 1 || args[0] != 50 {
		t.Errorf("expected args [50], got %v", args)
	}
}

func TestPredicateIn(t *testing.T) {
	clause, args := buildPredicate(In("id", 1, 2, 3), mockDialect{})
	if !strings.Contains(clause, "IN ($1, $2, $3)") {
		t.Errorf("expected IN ($1, $2, $3), got %q", clause)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestPredicateInEmpty(t *testing.T) {
	clause, args := buildPredicate(In("id"), mockDialect{})
	if clause != "FALSE" {
		t.Errorf("expected FALSE for empty IN, got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateNotIn(t *testing.T) {
	clause, args := buildPredicate(NotIn("id", 1, 2), mockDialect{})
	if !strings.Contains(clause, "NOT IN ($1, $2)") {
		t.Errorf("expected NOT IN ($1, $2), got %q", clause)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestPredicateNotInEmpty(t *testing.T) {
	clause, args := buildPredicate(NotIn("id"), mockDialect{})
	if clause != "TRUE" {
		t.Errorf("expected TRUE for empty NOT IN, got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateLike(t *testing.T) {
	clause, args := buildPredicate(Like("name", "%test%"), mockDialect{})
	if !strings.Contains(clause, "LIKE $1") {
		t.Errorf("expected LIKE $1, got %q", clause)
	}
	if len(args) != 1 || args[0] != "%test%" {
		t.Errorf("expected args [%%test%%], got %v", args)
	}
}

func TestPredicateILike(t *testing.T) {
	clause, args := buildPredicate(ILike("name", "%ALICE%"), mockDialect{})
	if !strings.Contains(clause, "ILIKE $1") {
		t.Errorf("expected ILIKE $1, got %q", clause)
	}
	if len(args) != 1 || args[0] != "%ALICE%" {
		t.Errorf("expected args [%%ALICE%%], got %v", args)
	}
}

func TestPredicateIsNull(t *testing.T) {
	clause, args := buildPredicate(IsNull("deleted_at"), mockDialect{})
	if clause != "deleted_at IS NULL" {
		t.Errorf("expected 'deleted_at IS NULL', got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateIsNotNull(t *testing.T) {
	clause, args := buildPredicate(IsNotNull("email"), mockDialect{})
	if clause != "email IS NOT NULL" {
		t.Errorf("expected 'email IS NOT NULL', got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateBetween(t *testing.T) {
	clause, args := buildPredicate(Between("created_at", "2024-01-01", "2024-12-31"), mockDialect{})
	if !strings.Contains(clause, "BETWEEN $1 AND $2") {
		t.Errorf("expected BETWEEN $1 AND $2, got %q", clause)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestPredicateJSONField(t *testing.T) {
	expr := JSONField("payload", "uuid")
	if expr != "payload->>'uuid'" {
		t.Errorf("expected payload->>'uuid', got %q", expr)
	}
}

func TestPredicateJSONContains(t *testing.T) {
	clause, args := buildPredicate(JSONContains("metadata", map[string]any{"key": "val"}), mockDialect{})
	if !strings.Contains(clause, "@>") {
		t.Errorf("expected @> in clause, got %q", clause)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestPredicateAnd(t *testing.T) {
	clause, args := buildPredicate(And(
		Eq("status", "active"),
		Eq("type", "admin"),
	), mockDialect{})
	if !strings.Contains(clause, "AND") {
		t.Errorf("expected AND in clause, got %q", clause)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestPredicateAndEmpty(t *testing.T) {
	clause, args := buildPredicate(And(), mockDialect{})
	if clause != "" {
		t.Errorf("expected empty clause, got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateAndNils(t *testing.T) {
	clause, args := buildPredicate(And(nil, Eq("x", 1), nil), mockDialect{})
	if !strings.Contains(clause, "x = $1") {
		t.Errorf("expected 'x = $1' in clause, got %q", clause)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestPredicateOr(t *testing.T) {
	clause, args := buildPredicate(Or(
		Eq("status", "active"),
		Eq("status", "pending"),
	), mockDialect{})
	if !strings.Contains(clause, "OR") {
		t.Errorf("expected OR in clause, got %q", clause)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestPredicateOrEmpty(t *testing.T) {
	clause, args := buildPredicate(Or(), mockDialect{})
	if clause != "" {
		t.Errorf("expected empty clause, got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateNot(t *testing.T) {
	clause, args := buildPredicate(Not(Eq("deleted", true)), mockDialect{})
	if !strings.Contains(clause, "NOT (") {
		t.Errorf("expected NOT (...) in clause, got %q", clause)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestPredicateNotNil(t *testing.T) {
	clause, args := buildPredicate(Not(nil), mockDialect{})
	if clause != "" {
		t.Errorf("expected empty clause for Not(nil), got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestPredicateComplexOr(t *testing.T) {
	// name ILIKE '%test%' OR label ILIKE '%test%' OR description ILIKE '%test%'
	clause, args := buildPredicate(Or(
		ILike("name", "%test%"),
		ILike("label", "%test%"),
		ILike("description", "%test%"),
	), mockDialect{})
	if !strings.Contains(clause, "OR") {
		t.Errorf("expected OR in clause, got %q", clause)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestPredicateNestedAndOr(t *testing.T) {
	// status = 'active' AND (name ILIKE '%a%' OR name ILIKE '%b%')
	clause, args := buildPredicate(And(
		Eq("status", "active"),
		Or(
			ILike("name", "%a%"),
			ILike("name", "%b%"),
		),
	), mockDialect{})
	if !strings.Contains(clause, "AND") || !strings.Contains(clause, "OR") {
		t.Errorf("expected AND and OR in clause, got %q", clause)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestMapWhere(t *testing.T) {
	clause, args := buildPredicate(MapWhere(map[string]any{
		"name": "test",
		"age":  30,
	}), mockDialect{})
	if !strings.Contains(clause, "name = $") || !strings.Contains(clause, "age = $") {
		t.Errorf("expected name= and age= in clause, got %q", clause)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestMapWhereNilValue(t *testing.T) {
	clause, args := buildPredicate(MapWhere(map[string]any{
		"deleted_date": nil,
	}), mockDialect{})
	if !strings.Contains(clause, "IS NULL") {
		t.Errorf("expected IS NULL, got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestMapWhereSlice(t *testing.T) {
	clause, args := buildPredicate(MapWhere(map[string]any{
		"id": []any{1, 2, 3},
	}), mockDialect{})
	if !strings.Contains(clause, "= ANY") {
		t.Errorf("expected = ANY, got %q", clause)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestMapWhereEmpty(t *testing.T) {
	pred := MapWhere(map[string]any{})
	if pred != nil {
		t.Error("expected nil predicate for empty map")
	}
}

func TestRenumberPlaceholders(t *testing.T) {
	result := renumberPlaceholders("x = $1 AND y = $2", mockDialect{}, 5)
	if !strings.Contains(result, "$5") || !strings.Contains(result, "$6") {
		t.Errorf("expected renumbered placeholders, got %q", result)
	}
}

func TestRenumberPlaceholdersNoChange(t *testing.T) {
	result := renumberPlaceholders("x = $1", mockDialect{}, 0)
	if result != "x = $1" {
		t.Errorf("expected no change, got %q", result)
	}
}

// ============================================
// FieldMapper Tests
// ============================================

func TestFieldMapperJsonTag(t *testing.T) {
	fm := defaultFieldMapper{}
	tp := reflect.TypeOf(testTable{})

	tests := []struct {
		field  string
		expect string
	}{
		{"ID", "id"},
		{"Name", "name"},
		{"Age", "age"},
		{"CreatedDate", "created_date"},
		{"DeletedDate", "deleted_date"},
	}

	for _, tt := range tests {
		f, _ := tp.FieldByName(tt.field)
		got := fm.ColumnName(f)
		if got != tt.expect {
			t.Errorf("FieldMapper.ColumnName(%s) = %q, want %q", tt.field, got, tt.expect)
		}
	}
}

func TestFieldMapperGormTag(t *testing.T) {
	fm := defaultFieldMapper{}
	tp := reflect.TypeOf(testTableGorm{})

	tests := []struct {
		field  string
		expect string
	}{
		{"ID", "gid"},
		{"Title", "title_name"},
		{"Content", ""},
		{"Status", "status_code"},
	}

	for _, tt := range tests {
		f, _ := tp.FieldByName(tt.field)
		got := fm.ColumnName(f)
		if got != tt.expect {
			t.Errorf("FieldMapper.ColumnName(%s) = %q, want %q", tt.field, got, tt.expect)
		}
	}
}

func TestFieldMapperSnakeCaseFallback(t *testing.T) {
	fm := defaultFieldMapper{}
	tp := reflect.TypeOf(testTableNoTag{})

	tests := []struct {
		field  string
		expect string
	}{
		{"FullName", "full_name"},
		{"EmailAddr", "email_addr"},
		{"InternalID", "internal_id"},
		{"SkipMe", ""},
	}

	for _, tt := range tests {
		f, _ := tp.FieldByName(tt.field)
		got := fm.ColumnName(f)
		if got != tt.expect {
			t.Errorf("FieldMapper.ColumnName(%s) = %q, want %q", tt.field, got, tt.expect)
		}
	}
}

func TestFieldMapperJsonDash(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		Hidden string `json:"-"`
		Normal string `json:"name"`
	}
	tp := reflect.TypeOf(s{})

	f, _ := tp.FieldByName("Hidden")
	if got := fm.ColumnName(f); got != "" {
		t.Errorf("json:\"-\" should return empty, got %q", got)
	}
	f, _ = tp.FieldByName("Normal")
	if got := fm.ColumnName(f); got != "name" {
		t.Errorf("json:\"name\" should return name, got %q", got)
	}
}

func TestFieldMapperGormDash(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		Hidden string `gorm:"-"`
		Normal string `gorm:"column:ok_col"`
	}
	tp := reflect.TypeOf(s{})

	f, _ := tp.FieldByName("Hidden")
	if got := fm.ColumnName(f); got != "" {
		t.Errorf("gorm:\"-\" should return empty, got %q", got)
	}
	f, _ = tp.FieldByName("Normal")
	if got := fm.ColumnName(f); got != "ok_col" {
		t.Errorf("gorm column should be ok_col, got %q", got)
	}
}

func TestFieldMapperGormPriorityOverJson(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		Field string `json:"json_name" gorm:"column:gorm_name"`
	}
	tp := reflect.TypeOf(s{})

	f, _ := tp.FieldByName("Field")
	if got := fm.ColumnName(f); got != "gorm_name" {
		t.Errorf("gorm column tag should take priority over json tag, got %q", got)
	}
}

func TestFieldMapperJsonEmptyNameFallsToGorm(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		Field string `json:",omitempty" gorm:"column:gorm_col"`
	}
	tp := reflect.TypeOf(s{})

	f, _ := tp.FieldByName("Field")
	if got := fm.ColumnName(f); got != "gorm_col" {
		t.Errorf("empty json name should fall through to gorm, got %q", got)
	}
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"ID", "id"},
		{"UserID", "user_id"},
		{"CrNumber", "cr_number"},
		{"CreatedDate", "created_date"},
		{"HTTPSConnection", "https_connection"},
		{"Simple", "simple"},
		{"ABC", "abc"},
		{"JSONParser", "json_parser"},
	}

	for _, tt := range tests {
		got := toSnakeCase(tt.input)
		if got != tt.expect {
			t.Errorf("toSnakeCase(%q) = %q, want %q", tt.input, got, tt.expect)
		}
	}
}

func TestColumnsFromType(t *testing.T) {
	fm := defaultFieldMapper{}
	tp := reflect.TypeOf(testTable{})
	cols := columnsFromType(tp, fm)
	expected := []string{"id", "name", "age", "created_date", "deleted_date"}
	if !reflect.DeepEqual(cols, expected) {
		t.Errorf("columnsFromType = %v, want %v", cols, expected)
	}
}

func TestRowValues(t *testing.T) {
	fm := defaultFieldMapper{}
	row := testTable{ID: 1, Name: "test", Age: 30}
	v := reflect.ValueOf(row)
	cols, vals := rowValues(v, fm)

	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" || cols[2] != "age" {
		t.Errorf("unexpected columns: %v", cols)
	}
	if vals[0].(int64) != 1 || vals[1].(string) != "test" || vals[2].(int) != 30 {
		t.Errorf("unexpected values: %v", vals)
	}
}

func TestScanTargets(t *testing.T) {
	fm := defaultFieldMapper{}
	row := testTable{}
	v := reflect.ValueOf(&row).Elem()
	targets, fields := scanTargets(v, fm)

	if len(targets) != 5 {
		t.Fatalf("expected 5 targets, got %d", len(targets))
	}
	if len(fields) != 5 {
		t.Fatalf("expected 5 fields, got %d", len(fields))
	}
	for i, target := range targets {
		if _, ok := target.(*any); !ok {
			t.Errorf("target[%d] should be *any", i)
		}
	}
}

func TestNullSafeCopyString(t *testing.T) {
	type s struct{ Name string }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = "hello"
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Name").String() != "hello" {
		t.Errorf("expected 'hello', got %q", v.FieldByName("Name").String())
	}
}

func TestNullSafeCopyNil(t *testing.T) {
	type s struct{ Name string }
	v := reflect.ValueOf(&s{Name: "original"}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = nil
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Name").String() != "" {
		t.Errorf("expected zero value, got %q", v.FieldByName("Name").String())
	}
}

func TestNullSafeCopyInt(t *testing.T) {
	type s struct{ Age int }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = float64(42)
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Age").Int() != 42 {
		t.Errorf("expected 42, got %d", v.FieldByName("Age").Int())
	}
}

func TestNullSafeCopyFloat(t *testing.T) {
	type s struct{ Score float64 }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = int64(95)
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Score").Float() != 95.0 {
		t.Errorf("expected 95.0, got %f", v.FieldByName("Score").Float())
	}
}

func TestNullSafeCopyBool(t *testing.T) {
	type s struct{ Active bool }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = true
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if !v.FieldByName("Active").Bool() {
		t.Error("expected true")
	}
}

func TestHasField(t *testing.T) {
	if !hasField(testTable{}, "ID") {
		t.Error("testTable should have ID field")
	}
	if !hasField(testTable{}, "DeletedDate") {
		t.Error("testTable should have DeletedDate field")
	}
	if hasField(testTable{}, "NonExistent") {
		t.Error("testTable should not have NonExistent field")
	}
}

func TestHasFieldWithType(t *testing.T) {
	tp := reflect.TypeOf(testTable{})
	if !hasField(tp, "Name") {
		t.Error("should find Name field via reflect.Type")
	}
}

func TestSetField(t *testing.T) {
	row := testTable{}
	v := reflect.ValueOf(&row).Elem()
	setField(v, "ID", int64(100))
	setField(v, "Name", "updated")

	if row.ID != 100 {
		t.Errorf("expected ID 100, got %d", row.ID)
	}
	if row.Name != "updated" {
		t.Errorf("expected Name 'updated', got %q", row.Name)
	}
}

func TestSetNow(t *testing.T) {
	type s struct{ UpdatedAt string }
	v := reflect.ValueOf(&s{}).Elem()
	setNow(v, "UpdatedAt")
	if v.FieldByName("UpdatedAt").String() != "" {
		t.Error("setNow should not set non-time.Time fields")
	}
}

// ============================================
// Curd CRUD Tests
// ============================================

func TestCurdFindAll(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{int64(1), "alice", int(25), "2024-01-01", nil},
				{int64(2), "bob", int(30), "2024-02-01", nil},
			},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	results, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != 1 || results[0].Name != "alice" {
		t.Errorf("unexpected result[0]: %+v", results[0])
	}
	if results[1].ID != 2 || results[1].Name != "bob" {
		t.Errorf("unexpected result[1]: %+v", results[1])
	}
}

func TestCurdFindAllWithWhere(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "alice", int(25), "2024-01-01", nil}},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	results, err := c.FindAll(context.Background(), Eq("name", "alice"), "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestCurdFindAllWithLimitOffset(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(2), "bob", int(30), "2024-02-01", nil}},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	results, err := c.FindAll(context.Background(), nil, "id DESC", 1, 1)
	if err != nil {
		t.Fatalf("FindAll error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestCurdFindAllQueryError(t *testing.T) {
	mock := &mockQuerier{queryErr: errors.New("db down")}
	c := New[testTable](mock, nil, mockDialect{})
	_, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err == nil {
		t.Error("expected error from failed query")
	}
}

func TestCurdFindOne(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "alice", int(25), "2024-01-01", nil}},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	result, err := c.FindOne(context.Background(), Eq("id", 1))
	if err != nil {
		t.Fatalf("FindOne error: %v", err)
	}
	if result.ID != 1 {
		t.Errorf("expected ID 1, got %d", result.ID)
	}
}

func TestCurdFindOneNotFound(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{}},
	}

	c := New[testTable](mock, nil, mockDialect{})
	_, err := c.FindOne(context.Background(), Eq("id", 999))
	if err == nil {
		t.Error("expected not found error")
	}
}

func TestCurdFindByID(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(42), "answer", int(0), "now", nil}},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	result, err := c.FindByID(context.Background(), 42)
	if err != nil {
		t.Fatalf("FindByID error: %v", err)
	}
	if result.ID != 42 {
		t.Errorf("expected ID 42, got %d", result.ID)
	}
}

func TestCurdInsertOne(t *testing.T) {
	mock := &mockQuerier{
		queryRow:   &mockRow{record: []any{int64(10)}},
		execResult: &mockResult{rowsAffected: 1},
	}

	c := New[testTable](mock, nil, mockDialect{})
	row := &testTable{Name: "inserted", Age: 20}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}
	if row.ID != 10 {
		t.Errorf("expected ID 10, got %d", row.ID)
	}
}

func TestCurdInsertOneExecError(t *testing.T) {
	mock := &mockQuerier{execErr: errors.New("constraint violation")}
	c := New[noIDTable](mock, nil, mockDialect{})
	row := &noIDTable{Name: "bad"}
	err := c.InsertOne(context.Background(), row)
	if err == nil {
		t.Error("expected exec error")
	}
}

func TestCurdInsertBatch(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 3}}
	c := New[testTable](mock, nil, mockDialect{})

	rows := []testTable{
		{Name: "a", Age: 1},
		{Name: "b", Age: 2},
		{Name: "c", Age: 3},
	}
	err := c.InsertBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatch error: %v", err)
	}
}

func TestCurdInsertBatchEmpty(t *testing.T) {
	mock := &mockQuerier{}
	c := New[testTable](mock, nil, mockDialect{})
	err := c.InsertBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("InsertBatch with nil should return nil, got %v", err)
	}
	err = c.InsertBatch(context.Background(), []testTable{})
	if err != nil {
		t.Errorf("InsertBatch with empty should return nil, got %v", err)
	}
}

func TestCurdInsertBatchPtr(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}
	c := New[testTable](mock, nil, mockDialect{})

	rows := []*testTable{
		{Name: "x", Age: 1},
		nil,
		{Name: "y", Age: 2},
	}
	err := c.InsertBatchPtr(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatchPtr error: %v", err)
	}
}

func TestCurdUpdateByID(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	c := New[testTable](mock, nil, mockDialect{})

	err := c.UpdateByID(context.Background(), 5, map[string]any{"name": "updated", "age": 99})
	if err != nil {
		t.Fatalf("UpdateByID error: %v", err)
	}
}

func TestCurdUpdateByIDError(t *testing.T) {
	mock := &mockQuerier{execErr: errors.New("update failed")}
	c := New[testTable](mock, nil, mockDialect{})

	err := c.UpdateByID(context.Background(), 5, map[string]any{"name": "x"})
	if err == nil {
		t.Error("expected update error")
	}
}

func TestCurdUpdateWhere(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}
	c := New[testTable](mock, nil, mockDialect{})

	err := c.UpdateWhere(context.Background(), Eq("status", "old"), map[string]any{"status": "new"})
	if err != nil {
		t.Fatalf("UpdateWhere error: %v", err)
	}
}

func TestCurdDeleteByIDSoft(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	c := New[testTable](mock, nil, mockDialect{})

	err := c.DeleteByID(context.Background(), 42, false)
	if err != nil {
		t.Fatalf("DeleteByID soft error: %v", err)
	}
}

func TestCurdDeleteByIDHard(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	c := New[testTable](mock, nil, mockDialect{})

	err := c.DeleteByID(context.Background(), 42, true)
	if err != nil {
		t.Fatalf("DeleteByID hard error: %v", err)
	}
}

func TestCurdDeleteWhere(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 5}}
	c := New[testTable](mock, nil, mockDialect{})

	err := c.DeleteWhere(context.Background(), Eq("status", "expired"))
	if err != nil {
		t.Fatalf("DeleteWhere error: %v", err)
	}
}

func TestCurdCount(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(42)}},
	}
	c := New[testTable](mock, nil, mockDialect{})

	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 42 {
		t.Errorf("expected count 42, got %d", count)
	}
}

func TestCurdCountWithWhere(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(1)}},
	}
	c := New[testTable](mock, nil, mockDialect{})

	count, err := c.Count(context.Background(), Eq("status", "active"))
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestCurdExists(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{true}},
	}
	c := New[testTable](mock, nil, mockDialect{})

	exists, err := c.Exists(context.Background(), Eq("id", 1))
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if !exists {
		t.Error("expected exists=true")
	}
}

func TestCurdExistsFalse(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{false}},
	}
	c := New[testTable](mock, nil, mockDialect{})

	exists, err := c.Exists(context.Background(), Eq("id", 999))
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if exists {
		t.Error("expected exists=false")
	}
}

func TestCurdWithQuerier(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{{int64(1), "test", int(0), "now", nil}}},
	}
	c := New[testTable](mock, nil, mockDialect{})

	txMock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{{int64(99), "tx_row", int(0), "now", nil}}},
	}

	txCurd := c.WithQuerier(txMock)
	results, err := txCurd.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("WithQuerier FindAll error: %v", err)
	}
	if results[0].ID != 99 {
		t.Errorf("expected ID 99 from tx, got %d", results[0].ID)
	}

	results2, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("original FindAll error: %v", err)
	}
	if results2[0].ID != 1 {
		t.Errorf("original Curd should be unchanged, expected ID 1, got %d", results2[0].ID)
	}
}

// ============================================
// Raw Query Tests
// ============================================

func TestQueryRaw(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{"result1"},
				{"result2"},
			},
		},
	}

	type row struct {
		Value string `json:"value"`
	}
	results, err := QueryRaw[row](context.Background(), mock, "SELECT value FROM t")
	if err != nil {
		t.Fatalf("QueryRaw error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Value != "result1" || results[1].Value != "result2" {
		t.Errorf("unexpected results: %+v", results)
	}
}

func TestQueryRawError(t *testing.T) {
	mock := &mockQuerier{queryErr: errors.New("syntax error")}
	type row struct{ X string }
	_, err := QueryRaw[row](context.Background(), mock, "BAD SQL")
	if err == nil {
		t.Error("expected query error")
	}
}

func TestQueryRowRaw(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{"single_result"}},
	}

	type row struct {
		Value string `json:"value"`
	}
	result, err := QueryRowRaw[row](context.Background(), mock, "SELECT value FROM t LIMIT 1")
	if err != nil {
		t.Fatalf("QueryRowRaw error: %v", err)
	}
	if result.Value != "single_result" {
		t.Errorf("expected 'single_result', got %q", result.Value)
	}
}

func TestQueryRowRawError(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{err: errors.New("no rows")},
	}
	type row struct{ X string }
	_, err := QueryRowRaw[row](context.Background(), mock, "SELECT 1")
	if err == nil {
		t.Error("expected scan error")
	}
}

func TestExecRaw(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 7}}
	n, err := ExecRaw(context.Background(), mock, "UPDATE t SET x=1")
	if err != nil {
		t.Fatalf("ExecRaw error: %v", err)
	}
	if n != 7 {
		t.Errorf("expected 7 rows affected, got %d", n)
	}
}

func TestExecRawError(t *testing.T) {
	mock := &mockQuerier{execErr: errors.New("permission denied")}
	_, err := ExecRaw(context.Background(), mock, "DROP TABLE t")
	if err == nil {
		t.Error("expected exec error")
	}
}

func TestRawFieldMapper(t *testing.T) {
	fm := rawFieldMapper{}
	type s struct {
		Visible string `json:"visible"`
		Hidden  string `json:"-"`
		GormHid string `gorm:"-"`
		Normal  string
	}
	tp := reflect.TypeOf(s{})

	f, _ := tp.FieldByName("Visible")
	if fm.ColumnName(f) == "" {
		t.Error("Visible should not be excluded")
	}
	f, _ = tp.FieldByName("Hidden")
	if fm.ColumnName(f) != "" {
		t.Error("json:\"-\" should be excluded")
	}
	f, _ = tp.FieldByName("GormHid")
	if fm.ColumnName(f) != "" {
		t.Error("gorm:\"-\" should be excluded")
	}
	f, _ = tp.FieldByName("Normal")
	if fm.ColumnName(f) == "" {
		t.Error("Normal should not be excluded")
	}
}

// ============================================
// Additional error path tests
// ============================================

func TestCurdInsertOneQueryRowError(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{err: errors.New("scan error")},
	}
	c := New[testTable](mock, nil, mockDialect{})
	row := &testTable{Name: "fail"}
	err := c.InsertOne(context.Background(), row)
	if err == nil {
		t.Error("expected query row error")
	}
}

func TestCurdInsertOneNoID(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	c := New[noIDTable](mock, nil, mockDialect{})
	row := &noIDTable{Name: "test"}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne without ID error: %v", err)
	}
}

func TestCurdNewFallsBackToDefaultMapper(t *testing.T) {
	c := New[testTable](&mockQuerier{}, nil, mockDialect{})
	if c.fm == nil {
		t.Error("FieldMapper should not be nil after New with nil fm")
	}
	_, ok := c.fm.(defaultFieldMapper)
	if !ok {
		t.Errorf("expected defaultFieldMapper, got %T", c.fm)
	}
}

func TestCurdNewWithCustomMapper(t *testing.T) {
	fm := rawFieldMapper{}
	c := New[testTable](&mockQuerier{}, fm, mockDialect{})
	if c.fm == nil {
		t.Error("custom FieldMapper should be used")
	}
	_, ok := c.fm.(rawFieldMapper)
	if !ok {
		t.Errorf("expected rawFieldMapper, got %T", c.fm)
	}
}

// ============================================
// Transaction Tests
// ============================================

func TestWithTxSuccess(t *testing.T) {
	q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	tx := &mockTx{Querier: q}
	tb := &mockTxBeginner{tx: tx}

	called := false
	err := WithTx(context.Background(), tb, func(ctx context.Context, q Querier) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx error: %v", err)
	}
	if !called {
		t.Error("fn should have been called")
	}
	if !tx.committed {
		t.Error("tx should be committed")
	}
	if tx.rolledBack {
		t.Error("tx should not be rolled back")
	}
}

func TestWithTxFnError(t *testing.T) {
	q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	tx := &mockTx{Querier: q}
	tb := &mockTxBeginner{tx: tx}

	testErr := errors.New("fn error")
	err := WithTx(context.Background(), tb, func(ctx context.Context, q Querier) error {
		return testErr
	})
	if err == nil {
		t.Error("expected error from fn")
	}
	if tx.committed {
		t.Error("tx should not be committed on error")
	}
	if !tx.rolledBack {
		t.Error("tx should be rolled back on error")
	}
}

func TestWithTxBeginError(t *testing.T) {
	tb := &mockTxBeginner{err: errors.New("begin failed")}

	err := WithTx(context.Background(), tb, func(ctx context.Context, q Querier) error {
		return nil
	})
	if err == nil {
		t.Error("expected begin error")
	}
}

func TestWithTxCommitError(t *testing.T) {
	q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	tx := &mockTx{Querier: q, commitErr: errors.New("commit failed")}
	tb := &mockTxBeginner{tx: tx}

	err := WithTx(context.Background(), tb, func(ctx context.Context, q Querier) error {
		return nil
	})
	if err == nil {
		t.Error("expected commit error")
	}
	if !tx.committed {
		t.Error("commit should have been attempted")
	}
}

func TestWithTxResultSuccess(t *testing.T) {
	q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	tx := &mockTx{Querier: q}
	tb := &mockTxBeginner{tx: tx}

	result, err := WithTxResult[int](context.Background(), tb, func(ctx context.Context, q Querier) (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("WithTxResult error: %v", err)
	}
	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
	if !tx.committed {
		t.Error("tx should be committed")
	}
}

func TestWithTxResultFnError(t *testing.T) {
	q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	tx := &mockTx{Querier: q}
	tb := &mockTxBeginner{tx: tx}

	_, err := WithTxResult[int](context.Background(), tb, func(ctx context.Context, q Querier) (int, error) {
		return 0, errors.New("fn error")
	})
	if err == nil {
		t.Error("expected fn error")
	}
	if !tx.rolledBack {
		t.Error("tx should be rolled back on error")
	}
}

func TestWithTxResultBeginError(t *testing.T) {
	tb := &mockTxBeginner{err: errors.New("begin failed")}
	_, err := WithTxResult[int](context.Background(), tb, func(ctx context.Context, q Querier) (int, error) {
		return 0, nil
	})
	if err == nil {
		t.Error("expected begin error")
	}
}

func TestWithTxResultCommitError(t *testing.T) {
	q := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	tx := &mockTx{Querier: q, commitErr: errors.New("commit failed")}
	tb := &mockTxBeginner{tx: tx}

	_, err := WithTxResult[int](context.Background(), tb, func(ctx context.Context, q Querier) (int, error) {
		return 42, nil
	})
	if err == nil {
		t.Error("expected commit error")
	}
	if !tx.committed {
		t.Error("commit should have been attempted")
	}
}

// ============================================
// Reflect edge case tests
// ============================================

func TestHasFieldPtr(t *testing.T) {
	row := &testTable{}
	if !hasField(row, "Name") {
		t.Error("should find Name on *testTable")
	}
}

func TestHasFieldNonStruct(t *testing.T) {
	if hasField(42, "anything") {
		t.Error("should return false for non-struct")
	}
}

func TestHasFieldValue(t *testing.T) {
	v := reflect.ValueOf(testTable{})
	if !hasField(v, "Name") {
		t.Error("should find Name via reflect.Value")
	}
}

func TestHasFieldDoublePointer(t *testing.T) {
	row := &testTable{}
	ptr := &row
	if !hasField(ptr, "Name") {
		t.Error("should find Name on **testTable")
	}
	if !hasField(ptr, "ID") {
		t.Error("should find ID on **testTable")
	}
}

func TestNullSafeCopyIntDefault(t *testing.T) {
	type s struct{ Age int }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = "not_a_number"
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Age").Int() != 0 {
		t.Errorf("expected zero for unparseable value, got %d", v.FieldByName("Age").Int())
	}
}

func TestNullSafeCopyFloatDefault(t *testing.T) {
	type s struct{ Score float64 }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = "not_a_float"
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Score").Float() != 0.0 {
		t.Errorf("expected zero for unparseable value, got %f", v.FieldByName("Score").Float())
	}
}

func TestNullSafeCopyBoolDefault(t *testing.T) {
	type s struct{ Active bool }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = "not_a_bool"
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Active").Bool() {
		t.Error("expected false for unparseable bool value")
	}
}

func TestNullSafeCopyAssignable(t *testing.T) {
	type s struct{ Name string }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = "direct"
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Name").String() != "direct" {
		t.Errorf("expected 'direct', got %q", v.FieldByName("Name").String())
	}
}

func TestNullSafeCopyIntFromInt(t *testing.T) {
	type s struct{ Count int }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = int64(100)
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Count").Int() != 100 {
		t.Errorf("expected 100, got %d", v.FieldByName("Count").Int())
	}
}

func TestNullSafeCopyFloatFromFloat(t *testing.T) {
	type s struct{ Rate float64 }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})

	var dest any = float64(3.14)
	targets := []any{&dest}
	nullSafeCopy(fields, targets)

	if v.FieldByName("Rate").Float() != 3.14 {
		t.Errorf("expected 3.14, got %f", v.FieldByName("Rate").Float())
	}
}

func TestNullSafeCopyUnsettableField(t *testing.T) {
	type s struct{ name string }
	v := reflect.ValueOf(&s{}).Elem()
	_, fields := scanTargets(v, defaultFieldMapper{})
	if len(fields) > 0 && !fields[0].CanSet() {
		var dest any = "hello"
		targets := []any{&dest}
		nullSafeCopy(fields, targets)
	}
}

func TestSetNowWithTimeField(t *testing.T) {
	type s struct{ UpdatedAt time.Time }
	v := reflect.ValueOf(&s{}).Elem()
	setNow(v, "UpdatedAt")
	if v.FieldByName("UpdatedAt").Interface().(time.Time).IsZero() {
		t.Error("setNow should set time.Time to now")
	}
}

func TestSetNowNonExistentField(t *testing.T) {
	type s struct{ X string }
	v := reflect.ValueOf(&s{}).Elem()
	setNow(v, "NonExistent")
}

func TestSetNowPointer(t *testing.T) {
	type s struct{ UpdatedAt time.Time }
	v := reflect.ValueOf(&s{})
	setNow(v, "UpdatedAt")
	if v.Elem().FieldByName("UpdatedAt").Interface().(time.Time).IsZero() {
		t.Error("setNow should set time.Time through pointer")
	}
}

func TestSetFieldPointer(t *testing.T) {
	type s struct{ Name string }
	v := reflect.ValueOf(&s{})
	setField(v, "Name", "hello")
	if v.Elem().FieldByName("Name").String() != "hello" {
		t.Errorf("expected 'hello', got %q", v.Elem().FieldByName("Name").String())
	}
}

func TestScanTargetsUnaddressable(t *testing.T) {
	type s struct{ Name string }
	v := reflect.ValueOf(s{})
	_, fields := scanTargets(v, defaultFieldMapper{})
	if len(fields) != 0 {
		t.Error("unaddressable struct should produce no fields")
	}
}

// ============================================
// Mock helpers edge case tests
// ============================================

func TestMockScanSetAssignable(t *testing.T) {
	var dest int64
	mockScanSet(&dest, int64(42))
	if dest != 42 {
		t.Errorf("expected 42, got %d", dest)
	}
}

func TestMockScanSetConvertible(t *testing.T) {
	var dest int64
	mockScanSet(&dest, int(42))
	if dest != 42 {
		t.Errorf("expected 42, got %d", dest)
	}
}

func TestMockScanSetNonPointer(t *testing.T) {
	var dest int64
	mockScanSet(dest, int64(42))
	if dest != 0 {
		t.Error("non-pointer dest should not be modified")
	}
}

func TestMockRowsScanOutOfBounds(t *testing.T) {
	m := &mockRows{records: [][]any{}, pos: 5}
	err := m.Scan()
	if err == nil {
		t.Error("expected out of bounds error")
	}
}

// ============================================
// Curd edge case tests
// ============================================

func TestCurdFindAllScanError(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{},
			err:     errors.New("rows iteration error"),
		},
	}
	c := New[testTable](mock, nil, mockDialect{})
	_, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err == nil {
		t.Error("expected scan error")
	}
}

func TestCurdInsertOneNoIDExecError(t *testing.T) {
	mock := &mockQuerier{execErr: errors.New("fk violation")}
	c := New[noIDTable](mock, nil, mockDialect{})
	row := &noIDTable{Name: "x"}
	err := c.InsertOne(context.Background(), row)
	if err == nil {
		t.Error("expected exec error for no-ID insert")
	}
}

func TestCurdNewWithNilDialect(t *testing.T) {
	c := New[testTable](&mockQuerier{}, nil, nil)
	if c.dialect != nil {
		t.Log("dialect is nil - callers must provide valid dialect")
	}
}

func TestFieldMapperGormColumnOnly(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		X string `gorm:"primaryKey;column:pk_col"`
	}
	tp := reflect.TypeOf(s{})
	f, _ := tp.FieldByName("X")
	if got := fm.ColumnName(f); got != "pk_col" {
		t.Errorf("expected pk_col, got %q", got)
	}
}

func TestFieldMapperGormColumnWithSpaces(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		X string `gorm:"column: my_col ;type:varchar"`
	}
	tp := reflect.TypeOf(s{})
	f, _ := tp.FieldByName("X")
	if got := fm.ColumnName(f); got != "my_col" {
		t.Errorf("expected my_col, got %q", got)
	}
}

// ============================================
// Auto Timestamp Tests
// ============================================

func TestCurdInsertOneAutoTimestamps(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(1)}},
	}
	c := New[testTableWithTime](mock, nil, mockDialect{})
	row := &testTableWithTime{}
	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}
	if row.CreatedDate.IsZero() {
		t.Error("CreatedDate should be set")
	}
	if row.ChangedDate.IsZero() {
		t.Error("ChangedDate should be set")
	}
}

func TestCurdFindAllHidesDeleted(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "2024-01-01", "2024-01-01", nil}},
		},
	}
	c := New[testTableWithTime](mock, nil, mockDialect{})
	results, err := c.FindAll(context.Background(), Eq("id", 1), "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestCurdInsertBatchAutoTimestamps(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}
	c := New[testTableWithTime](mock, nil, mockDialect{})
	rows := []testTableWithTime{{}, {}}
	err := c.InsertBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatch error: %v", err)
	}
	for i := range rows {
		if rows[i].CreatedDate.IsZero() {
			t.Errorf("row[%d].CreatedDate should be set", i)
		}
	}
}

// ============================================
// Field Transformer Tests
// ============================================

func TestJSONBMarshaler(t *testing.T) {
	transformer := JSONBMarshaler("metadata")

	result := transformer("metadata", map[string]any{"key": "value"})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if str != `{"key":"value"}` {
		t.Errorf("expected JSON string, got %q", str)
	}

	result = transformer("other", 42)
	if result != 42 {
		t.Errorf("expected 42, got %v", result)
	}

	result = transformer("metadata", nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestJSONBMarshalerComplexStruct(t *testing.T) {
	transformer := JSONBMarshaler("config")

	type nested struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	result := transformer("config", nested{A: "hello", B: 123})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if str != `{"a":"hello","b":123}` {
		t.Errorf("expected JSON, got %q", str)
	}
}

func TestXMLMarshaler(t *testing.T) {
	transformer := XMLMarshaler("document")

	type doc struct {
		XMLName struct{} `xml:"root"`
		Title   string   `xml:"title"`
		Body    string   `xml:"body"`
	}

	result := transformer("document", doc{Title: "Test", Body: "Content"})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if str != `<root><title>Test</title><body>Content</body></root>` {
		t.Errorf("expected XML string, got %q", str)
	}

	result = transformer("other_field", 100)
	if result != 100 {
		t.Errorf("expected 100, got %v", result)
	}

	result = transformer("document", nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestComposeTransformers(t *testing.T) {
	composed := ComposeTransformers(
		JSONBMarshaler("metadata"),
		XMLMarshaler("document"),
	)

	result := composed("metadata", map[string]any{"a": 1})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string from JSONB marshaler, got %T", result)
	}
	if str != `{"a":1}` {
		t.Errorf("expected JSON, got %q", str)
	}

	type doc struct {
		XMLName struct{} `xml:"node"`
		Val     string   `xml:"val"`
	}
	result = composed("document", doc{Val: "x"})
	str, ok = result.(string)
	if !ok {
		t.Fatalf("expected string from XML marshaler, got %T", result)
	}
	if str != `<node><val>x</val></node>` {
		t.Errorf("expected XML, got %q", str)
	}

	result = composed("name", "john")
	if result != "john" {
		t.Errorf("expected 'john', got %v", result)
	}
}

func TestComposeTransformersOverride(t *testing.T) {
	composed := ComposeTransformers(
		JSONBMarshaler("data"),
		XMLMarshaler("data"),
	)

	type doc struct {
		XMLName struct{} `xml:"item"`
		Value   string   `xml:"value"`
	}
	result := composed("data", doc{Value: "test"})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if str == "" {
		t.Error("expected non-empty result")
	}
	if str[:1] != "<" {
		t.Error("expected XML-encoded output to start with '<'")
	}
}

func TestCurdWithTransformerInsertOne(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(1)}},
	}

	c := New[testTableWithJSONB](mock, nil, mockDialect{}).
		WithTransformer(JSONBMarshaler("metadata"))

	row := &testTableWithJSONB{
		Metadata: map[string]any{"version": 1, "env": "prod"},
	}

	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne with transformer error: %v", err)
	}
	if row.ID != 1 {
		t.Errorf("expected ID 1, got %d", row.ID)
	}
}

func TestCurdWithTransformerInsertBatch(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}

	c := New[testTableWithJSONB](mock, nil, mockDialect{}).
		WithTransformer(JSONBMarshaler("metadata", "config"))

	rows := []testTableWithJSONB{
		{Metadata: map[string]any{"a": 1}},
		{Metadata: map[string]any{"b": 2}, Config: map[string]any{"c": 3}},
	}

	err := c.InsertBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatch with transformer error: %v", err)
	}
}

func TestCurdWithTransformerPreservesOtherFields(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(10)}},
	}

	c := New[testTableWithJSONB](mock, nil, mockDialect{}).
		WithTransformer(JSONBMarshaler("metadata"))

	row := &testTableWithJSONB{
		Metadata: map[string]any{"key": "value"},
		Config:   map[string]any{"other": true},
	}

	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}
	if _, ok := row.Config["other"]; !ok {
		t.Error("Config should be preserved as map")
	}
}

func TestCurdChainedWithTransformer(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 3}}

	c := New[testTableWithJSONB](mock, nil, mockDialect{}).
		WithTransformer(JSONBMarshaler("metadata")).
		WithTransformer(JSONBMarshaler("config"))

	if len(c.transforms) != 2 {
		t.Errorf("expected 2 transformers, got %d", len(c.transforms))
	}

	rows := []testTableWithJSONB{
		{Metadata: map[string]any{"a": 1}, Config: map[string]any{"x": 2}},
	}
	err := c.InsertBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatch error: %v", err)
	}
}

func TestCurdWithXMLTransformerInsertOne(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(5)}},
	}

	c := New[testTableWithXML](mock, nil, mockDialect{}).
		WithTransformer(XMLMarshaler("document"))

	type xmlDoc struct {
		XMLName struct{} `xml:"report"`
		Title   string   `xml:"title"`
	}
	row := &testTableWithXML{
		Document: "placeholder",
	}

	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne with XML transformer error: %v", err)
	}
	if row.ID != 5 {
		t.Errorf("expected ID 5, got %d", row.ID)
	}

	tf := XMLMarshaler("document")
	result := tf("document", xmlDoc{Title: "Hello"})
	if str, ok := result.(string); !ok || str != `<report><title>Hello</title></report>` {
		t.Errorf("XML transformer did not produce expected output: %v", result)
	}
}

func TestRowValuesWithTransforms(t *testing.T) {
	fm := defaultFieldMapper{}
	row := testTableWithJSONB{
		ID:       1,
		Metadata: map[string]any{"key": "val"},
		Config:   map[string]any{"debug": true},
	}
	v := reflect.ValueOf(row)

	_, vals := rowValues(v, fm)
	if _, ok := vals[1].(map[string]any); !ok {
		t.Error("Metadata should be map without transformer")
	}

	_, vals = rowValues(v, fm, JSONBMarshaler("metadata"))
	if str, ok := vals[1].(string); !ok || str != `{"key":"val"}` {
		t.Errorf("Metadata should be JSON string with transformer, got %v", vals[1])
	}
	if _, ok := vals[2].(map[string]any); !ok {
		t.Error("Config should remain map (not in transformer list)")
	}
}

func TestCurdWithQuerierPreservesTransforms(t *testing.T) {
	c := New[testTableWithJSONB](&mockQuerier{}, nil, mockDialect{}).
		WithTransformer(JSONBMarshaler("metadata"))

	c2 := c.WithQuerier(&mockQuerier{})
	if len(c2.transforms) != 1 {
		t.Errorf("WithQuerier should preserve transforms, got %d", len(c2.transforms))
	}
}

// ============================================
// Pointer type T tests (Curd[*T])
// ============================================

func TestCurdPointerFindAll(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{int64(1), "first"},
				{int64(2), "second"},
			},
		},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})
	results, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != 1 || results[0].Name != "first" {
		t.Errorf("unexpected result[0]: %+v", results[0])
	}
	if results[1].ID != 2 || results[1].Name != "second" {
		t.Errorf("unexpected result[1]: %+v", results[1])
	}
}

func TestCurdPointerFindOne(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(42), "answer"}},
		},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})
	result, err := c.FindOne(context.Background(), Eq("id", 42))
	if err != nil {
		t.Fatalf("FindOne error: %v", err)
	}
	if result.ID != 42 {
		t.Errorf("expected ID 42, got %d", result.ID)
	}
	if result.Name != "answer" {
		t.Errorf("expected Name 'answer', got %q", result.Name)
	}
}

func TestCurdPointerFindOneNotFound(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{}},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})
	_, err := c.FindOne(context.Background(), Eq("id", 999))
	if err == nil {
		t.Error("expected not found error")
	}
}

func TestCurdPointerCount(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(7)}},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})
	count, err := c.Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 7 {
		t.Errorf("expected count 7, got %d", count)
	}
}

func TestCurdPointerExists(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{true}},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})
	exists, err := c.Exists(context.Background(), Eq("id", 1))
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if !exists {
		t.Error("expected exists=true")
	}
}

func TestCurdPointerUpdateByID(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	c := New[*pointerTable](mock, nil, mockDialect{})
	err := c.UpdateByID(context.Background(), 5, map[string]any{"name": "updated"})
	if err != nil {
		t.Fatalf("UpdateByID error: %v", err)
	}
}

func TestCurdPointerUpdateWhere(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 3}}
	c := New[*pointerTable](mock, nil, mockDialect{})
	err := c.UpdateWhere(context.Background(), Eq("name", "old"), map[string]any{"name": "new"})
	if err != nil {
		t.Fatalf("UpdateWhere error: %v", err)
	}
}

func TestCurdPointerDeleteByID(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 1}}
	c := New[*pointerTable](mock, nil, mockDialect{})
	err := c.DeleteByID(context.Background(), 42, false)
	if err != nil {
		t.Fatalf("DeleteByID error: %v", err)
	}
}

func TestCurdPointerDeleteWhere(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}
	c := New[*pointerTable](mock, nil, mockDialect{})
	err := c.DeleteWhere(context.Background(), Eq("name", "stale"))
	if err != nil {
		t.Fatalf("DeleteWhere error: %v", err)
	}
}

func TestCurdPointerInsertBatch(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 2}}
	c := New[*pointerTable](mock, nil, mockDialect{})

	rows := []*pointerTable{
		{Name: "x"},
		{Name: "y"},
	}
	err := c.InsertBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("InsertBatch error: %v", err)
	}
}

func TestCurdPointerInsertOne(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(100)}},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})
	row := &pointerTable{Name: "inserted"}
	err := c.InsertOne(context.Background(), &row)
	if err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}
	if row.ID != 100 {
		t.Errorf("expected ID 100, got %d", row.ID)
	}
}

func TestCurdPointerWithQuerier(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{{int64(1), "original"}}},
	}
	c := New[*pointerTable](mock, nil, mockDialect{})

	txMock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{{int64(99), "tx_result"}}},
	}
	txC := c.WithQuerier(txMock)
	results, err := txC.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("FindAll error: %v", err)
	}
	if results[0].ID != 99 || results[0].Name != "tx_result" {
		t.Errorf("unexpected result: %+v", results[0])
	}

	origResults, err := c.FindAll(context.Background(), nil, "", 0, 0)
	if err != nil {
		t.Fatalf("original FindAll error: %v", err)
	}
	if origResults[0].ID != 1 {
		t.Errorf("original ID should be 1, got %d", origResults[0].ID)
	}
}

func TestQueryRawPointer(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{int64(10), "ten"},
				{int64(20), "twenty"},
			},
		},
	}
	results, err := QueryRaw[*pointerTable](context.Background(), mock, "SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("QueryRaw error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != 10 || results[0].Name != "ten" {
		t.Errorf("unexpected results[0]: %+v", results[0])
	}
}

func TestQueryRowRawPointer(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{int64(55), "test"}},
	}
	result, err := QueryRowRaw[*pointerTable](context.Background(), mock, "SELECT id, name FROM t LIMIT 1")
	if err != nil {
		t.Fatalf("QueryRowRaw error: %v", err)
	}
	if result.ID != 55 || result.Name != "test" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestColumnsFromTypePointer(t *testing.T) {
	fm := defaultFieldMapper{}
	tp := reflect.TypeOf(&testTable{})
	cols := columnsFromType(tp, fm)
	expected := []string{"id", "name", "age", "created_date", "deleted_date"}
	if !reflect.DeepEqual(cols, expected) {
		t.Errorf("columnsFromType with pointer type = %v, want %v", cols, expected)
	}
}

func TestRowValuesPointer(t *testing.T) {
	fm := defaultFieldMapper{}
	row := &testTable{ID: 1, Name: "test", Age: 30}
	v := reflect.ValueOf(row)
	cols, vals := rowValues(v, fm)

	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" || cols[2] != "age" {
		t.Errorf("unexpected columns: %v", cols)
	}
	if vals[0].(int64) != 1 || vals[1].(string) != "test" || vals[2].(int) != 30 {
		t.Errorf("unexpected values: %v", vals)
	}
}

func TestScanTargetsPointer(t *testing.T) {
	fm := defaultFieldMapper{}
	row := &testTable{}
	v := reflect.ValueOf(row)
	targets, fields := scanTargets(v, fm)

	if len(targets) != 5 {
		t.Fatalf("expected 5 targets, got %d", len(targets))
	}
	if len(fields) != 5 {
		t.Fatalf("expected 5 fields, got %d", len(fields))
	}
}

// ============================================
// New Methods Tests (Find, FindPaginated, Upsert, Save, Pluck, Each)
// ============================================

func TestCurdFind(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{int64(1), "alice", int(25), "2024-01-01", nil},
			},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	results, err := c.Find(context.Background(),
		WithWhere(Eq("name", "alice")),
		WithOrderBy("id DESC"),
		WithLimit(1),
	)
	if err != nil {
		t.Fatalf("Find error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Name != "alice" {
		t.Errorf("expected 'alice', got %q", results[0].Name)
	}
}

func TestCurdFindWithColumns(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{{int64(1), "bob", int(30), "2024-01-01", nil}},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	// WithColumns with table-qualified column names (full set of struct columns)
	results, err := c.Find(context.Background(),
		WithColumns("t.id", "t.name", "t.age", "t.created_date", "t.deleted_date"),
		WithLimit(10),
	)
	if err != nil {
		t.Fatalf("Find error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Name != "bob" {
		t.Errorf("expected 'bob', got %q", results[0].Name)
	}
}

func TestCurdFindPaginated(t *testing.T) {
	callCount := 0
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{int64(1), "alice", int(25), "2024-01-01", nil},
				{int64(2), "bob", int(30), "2024-02-01", nil},
			},
		},
		queryRow: &mockRow{record: []any{int64(25)}},
	}
	_ = callCount

	c := New[testTable](mock, nil, mockDialect{})
	result, err := c.FindPaginated(context.Background(),
		WithWhere(Eq("status", "active")),
		WithOrderBy("id ASC"),
		WithLimit(10),
		WithOffset(0),
	)
	if err != nil {
		t.Fatalf("FindPaginated error: %v", err)
	}
	if result.Total != 25 {
		t.Errorf("expected total 25, got %d", result.Total)
	}
	if len(result.List) != 2 {
		t.Fatalf("expected 2 list items, got %d", len(result.List))
	}
}

func TestCurdFindPaginatedCountError(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{err: errors.New("count failed")},
	}
	c := New[testTable](mock, nil, mockDialect{})
	_, err := c.FindPaginated(context.Background(),
		WithLimit(10),
	)
	if err == nil {
		t.Error("expected count error")
	}
}

func TestCurdUpsertInsert(t *testing.T) {
	// No matching row → INSERT (Exists returns false, then InsertOne returns id 100)
	c := New[testTable](&upsertMock{
		existsVal:   false,
		insertID:    int64(100),
		updateOK:    true,
	}, nil, mockDialect{})
	row := &testTable{Name: "upserted", Age: 42}
	err := c.Upsert(context.Background(), Eq("name", "upserted"), row)
	if err != nil {
		t.Fatalf("Upsert (insert) error: %v", err)
	}
	if row.ID != 100 {
		t.Errorf("expected ID 100 from insert, got %d", row.ID)
	}
}

func TestCurdUpsertUpdate(t *testing.T) {
	// Matching row exists → UPDATE
	c := New[testTable](&upsertMock{
		existsVal: true,
		updateOK:  true,
	}, nil, mockDialect{})
	row := &testTable{Name: "updated-name", Age: 99}
	err := c.Upsert(context.Background(), Eq("name", "updated-name"), row)
	if err != nil {
		t.Fatalf("Upsert (update) error: %v", err)
	}
}

func TestCurdUpsertExistsError(t *testing.T) {
	c := New[testTable](&upsertMock{
		queryErr: errors.New("db error"),
	}, nil, mockDialect{})
	row := &testTable{Name: "fail"}
	err := c.Upsert(context.Background(), Eq("name", "fail"), row)
	if err == nil {
		t.Error("expected error from failed exists check")
	}
}

func TestCurdUpsertWithCompositeCondition(t *testing.T) {
	c := New[testTable](&upsertMock{
		existsVal: true,
		updateOK:  true,
	}, nil, mockDialect{})
	row := &testTable{Name: "test", Age: 30}
	err := c.Upsert(context.Background(),
		And(Eq("name", "test"), Eq("status", "active")),
		row,
	)
	if err != nil {
		t.Fatalf("Upsert with composite condition error: %v", err)
	}
}

func TestCurdSaveInsert(t *testing.T) {
	// ID is zero → direct INSERT (bypasses Upsert)
	mock := &mockQuerier{
		queryRow:   &mockRow{record: []any{int64(50)}},
		execResult: &mockResult{rowsAffected: 1},
	}
	c := New[testTable](mock, nil, mockDialect{})
	row := &testTable{Name: "saved", Age: 10}
	err := c.Save(context.Background(), row)
	if err != nil {
		t.Fatalf("Save (insert) error: %v", err)
	}
	if row.ID != 50 {
		t.Errorf("expected ID 50 from insert, got %d", row.ID)
	}
}

func TestCurdSaveUpdate(t *testing.T) {
	// ID is non-zero → Upsert by id (exists → update)
	c := New[testTable](&upsertMock{
		existsVal: true,
		updateOK:  true,
	}, nil, mockDialect{})
	row := &testTable{ID: 42, Name: "updated-via-save", Age: 99}
	err := c.Save(context.Background(), row)
	if err != nil {
		t.Fatalf("Save (update) error: %v", err)
	}
}

func TestCurdSaveInsertWhenNotExists(t *testing.T) {
	// ID non-zero but no matching row → INSERT
	c := New[testTable](&upsertMock{
		existsVal: false,
		insertID:  int64(99),
	}, nil, mockDialect{})
	row := &testTable{ID: 99, Name: "new-with-id", Age: 10}
	err := c.Save(context.Background(), row)
	if err != nil {
		t.Fatalf("Save (insert when not exists) error: %v", err)
	}
	if row.ID != 99 {
		t.Errorf("expected ID 99, got %d", row.ID)
	}
}

func TestStructToUpdates(t *testing.T) {
	row := &testTable{ID: 1, Name: "test", Age: 30}
	v := reflect.ValueOf(row).Elem()
	updates := structToUpdates(v, defaultFieldMapper{}, nil)
	if len(updates) != 4 {
		t.Errorf("expected 4 columns (excl. id), got %d: %v", len(updates), updates)
	}
	if updates["name"] != "test" {
		t.Errorf("expected name='test', got %v", updates["name"])
	}
	if updates["age"] != 30 {
		t.Errorf("expected age=30, got %v", updates["age"])
	}
}

func TestCurdPluck(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{"alice"},
				{"bob"},
				{"charlie"},
			},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	vals, err := c.Pluck(context.Background(), "name", Eq("status", "active"))
	if err != nil {
		t.Fatalf("Pluck error: %v", err)
	}
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	if vals[0] != "alice" {
		t.Errorf("expected 'alice', got %v", vals[0])
	}
}

func TestCurdPluckEmpty(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{records: [][]any{}},
	}

	c := New[testTable](mock, nil, mockDialect{})
	vals, err := c.Pluck(context.Background(), "name", nil)
	if err != nil {
		t.Fatalf("Pluck error: %v", err)
	}
	if len(vals) != 0 {
		t.Errorf("expected 0 values, got %d", len(vals))
	}
}

func TestCurdPluckError(t *testing.T) {
	mock := &mockQuerier{queryErr: errors.New("pluck failed")}
	c := New[testTable](mock, nil, mockDialect{})
	_, err := c.Pluck(context.Background(), "name", nil)
	if err == nil {
		t.Error("expected pluck error")
	}
}

func TestCurdEach(t *testing.T) {
	batch1 := &mockRows{
		records: [][]any{
			{int64(1), "a", int(1), "now", nil},
			{int64(2), "b", int(2), "now", nil},
		},
	}
	batch2 := &mockRows{
		records: [][]any{
			{int64(3), "c", int(3), "now", nil},
		},
	}

	// cyclingQuerier returns a different Rows on each Query call
	type cyclingQuerier struct {
		batches  []*mockRows
		idx      int
		queryRow Row
	}
	cq := &cyclingQuerier{
		batches:  []*mockRows{batch1, batch2},
		queryRow: &mockRow{record: []any{int64(0)}},
	}

	mock := &mockQuerier{
		queryRows: batch1,
	}

	c := New[testTable](mock, nil, mockDialect{})
	var collected []string
	called := 0

	// Use a custom querier that cycles through batches
	cycleQ := &eachQuerier{batches: []*mockRows{batch1, batch2}}
	c2 := c.WithQuerier(cycleQ)

	err := c2.Each(context.Background(), Gt("id", 0), 2, func(batch []testTable) error {
		called++
		for _, r := range batch {
			collected = append(collected, r.Name)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Each error: %v", err)
	}
	if called != 2 {
		t.Fatalf("expected 2 batches, got %d", called)
	}
	if len(collected) != 3 {
		t.Fatalf("expected 3 items, got %d: %v", len(collected), collected)
	}
	if collected[0] != "a" || collected[1] != "b" || collected[2] != "c" {
		t.Errorf("unexpected collected: %v", collected)
	}
	_ = cq
}

// eachQuerier cycles through batches on each Query call
type eachQuerier struct {
	batches []*mockRows
	idx     int
}

func (e *eachQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if e.idx >= len(e.batches) {
		return &mockRows{records: [][]any{}}, nil
	}
	r := e.batches[e.idx]
	e.idx++
	return r, nil
}

func (e *eachQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return &mockRow{record: []any{int64(0)}}
}

func (e *eachQuerier) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	return &mockResult{rowsAffected: 0}, nil
}

func TestCurdEachFnError(t *testing.T) {
	mock := &mockQuerier{
		queryRows: &mockRows{
			records: [][]any{
				{int64(1), "x", int(1), "now", nil},
			},
		},
	}

	c := New[testTable](mock, nil, mockDialect{})
	testErr := errors.New("stop")
	err := c.Each(context.Background(), nil, 100, func(batch []testTable) error {
		return testErr
	})
	if !errors.Is(err, testErr) {
		t.Errorf("expected testErr, got %v", err)
	}
}

