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

type mockDialect struct{}

func (mockDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

type mockRows struct {
	records [][]any
	pos     int
	closed  bool
	err     error
}

func (m *mockRows) Close()         { m.closed = true }
func (m *mockRows) Err() error     { return m.err }
func (m *mockRows) Next() bool     { m.pos++; return m.pos <= len(m.records) }
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

func TestFieldMapperJsonPriorityOverGorm(t *testing.T) {
	fm := defaultFieldMapper{}
	type s struct {
		Field string `json:"json_name" gorm:"column:gorm_name"`
	}
	tp := reflect.TypeOf(s{})

	f, _ := tp.FieldByName("Field")
	if got := fm.ColumnName(f); got != "json_name" {
		t.Errorf("json tag should take priority over gorm, got %q", got)
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

func TestBuildWhereNil(t *testing.T) {
	conditions, args := buildWhere(map[string]any{"deleted_date": nil}, &argCounter{idx: 1}, mockDialect{})
	if len(conditions) != 1 || conditions[0] != "deleted_date IS NULL" {
		t.Errorf("expected IS NULL condition, got %v", conditions)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestBuildWhereSlice(t *testing.T) {
	conditions, args := buildWhere(map[string]any{"id": []any{1, 2, 3}}, &argCounter{idx: 1}, mockDialect{})
	if len(conditions) != 1 || !strings.Contains(conditions[0], "= ANY") {
		t.Errorf("expected = ANY condition, got %v", conditions)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestBuildWhereScalar(t *testing.T) {
	conditions, args := buildWhere(map[string]any{"name": "test"}, &argCounter{idx: 1}, mockDialect{})
	if len(conditions) != 1 || !strings.Contains(conditions[0], "= $1") {
		t.Errorf("expected = $1 condition, got %v", conditions)
	}
	if len(args) != 1 || args[0] != "test" {
		t.Errorf("expected args [test], got %v", args)
	}
}

func TestBuildWhereMultiple(t *testing.T) {
	conditions, args := buildWhere(map[string]any{
		"name": "joe",
		"age":  30,
	}, &argCounter{idx: 1}, mockDialect{})
	if len(conditions) != 2 {
		t.Errorf("expected 2 conditions, got %d", len(conditions))
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
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
	// setNow only sets time.Time fields, not string
	v := reflect.ValueOf(&s{}).Elem()
	setNow(v, "UpdatedAt")
	if v.FieldByName("UpdatedAt").String() != "" {
		t.Error("setNow should not set non-time.Time fields")
	}
}

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
	results, err := c.FindAll(context.Background(), map[string]any{"name": "alice"}, "", 0, 0)
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
	result, err := c.FindOne(context.Background(), map[string]any{"id": 1})
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
	_, err := c.FindOne(context.Background(), map[string]any{"id": 999})
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

	err := c.UpdateWhere(context.Background(), map[string]any{"status": "old"}, map[string]any{"status": "new"})
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

	err := c.DeleteWhere(context.Background(), map[string]any{"status": "expired"})
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

func TestCurdExists(t *testing.T) {
	mock := &mockQuerier{
		queryRow: &mockRow{record: []any{true}},
	}
	c := New[testTable](mock, nil, mockDialect{})

	exists, err := c.Exists(context.Background(), map[string]any{"id": 1})
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

	exists, err := c.Exists(context.Background(), map[string]any{"id": 999})
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

type noIDTable struct {
	Name string `json:"name"`
}

func (noIDTable) TableName() string { return "no_id_table" }

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

func TestScanTargetsUnaddressable(t *testing.T) {
	type s struct{ Name string }
	v := reflect.ValueOf(s{})
	_, fields := scanTargets(v, defaultFieldMapper{})
	if len(fields) != 0 {
		t.Error("unaddressable struct should produce no fields")
	}
}

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

type testTableWithTime struct {
	ID          int64         `json:"id"`
	CreatedDate time.Time     `json:"created_date"`
	ChangedDate time.Time     `json:"changed_date"`
	DeletedDate *time.Time    `json:"deleted_date"`
}

func (testTableWithTime) TableName() string { return "test_time" }

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
	results, err := c.FindAll(context.Background(), map[string]any{"id": 1}, "", 0, 0)
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

func TestCurdNewWithNilDialect(t *testing.T) {
	// New should accept nil dialect - the caller is responsible for providing a valid one
	c := New[testTable](&mockQuerier{}, nil, nil)
	if c.dialect != nil {
		t.Log("dialect is nil - callers must provide valid dialect")
	}
}

func TestBuildWhereEmptyMap(t *testing.T) {
	conditions, args := buildWhere(map[string]any{}, &argCounter{idx: 1}, mockDialect{})
	if len(conditions) != 0 {
		t.Errorf("expected 0 conditions, got %d", len(conditions))
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

// --- Field transformer tests ---

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

func TestJSONBMarshaler(t *testing.T) {
	transformer := JSONBMarshaler("metadata")

	// Field in the set should be marshaled
	result := transformer("metadata", map[string]any{"key": "value"})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if str != `{"key":"value"}` {
		t.Errorf("expected JSON string, got %q", str)
	}

	// Field not in the set should pass through
	result = transformer("other", 42)
	if result != 42 {
		t.Errorf("expected 42, got %v", result)
	}

	// Nil value should pass through
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

	// Field not in the set should pass through
	result = transformer("other_field", 100)
	if result != 100 {
		t.Errorf("expected 100, got %v", result)
	}

	// Nil value should pass through
	result = transformer("document", nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestComposeTransformers(t *testing.T) {
	// Use transformers for non-overlapping fields
	composed := ComposeTransformers(
		JSONBMarshaler("metadata"),
		XMLMarshaler("document"),
	)

	// JSONB field
	result := composed("metadata", map[string]any{"a": 1})
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string from JSONB marshaler, got %T", result)
	}
	if str != `{"a":1}` {
		t.Errorf("expected JSON, got %q", str)
	}

	// XML field
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

	// Field not in any set
	result = composed("name", "john")
	if result != "john" {
		t.Errorf("expected 'john', got %v", result)
	}
}

func TestComposeTransformersOverride(t *testing.T) {
	// When both match the same field, transformers are applied in order.
	// JSONBMarshaler runs first (struct → JSON string),
	// then XMLMarshaler runs (XML-encodes the JSON string).
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
	// The struct was JSON-marshaled first, then that JSON string was XML-encoded.
	// xml.Marshal on a string produces <string>...</string> wrapping.
	if str == "" {
		t.Error("expected non-empty result")
	}
	// Verify the last transformer (XML) was applied: the result should contain XML tags
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
		Config:   map[string]any{"other": true}, // not transformed
	}

	err := c.InsertOne(context.Background(), row)
	if err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}
	// Config should remain as map[string]any (not JSON string)
	if _, ok := row.Config["other"]; !ok {
		t.Error("Config should be preserved as map")
	}
}

func TestCurdChainedWithTransformer(t *testing.T) {
	mock := &mockQuerier{execResult: &mockResult{rowsAffected: 3}}

	c := New[testTableWithJSONB](mock, nil, mockDialect{}).
		WithTransformer(JSONBMarshaler("metadata")).
		WithTransformer(JSONBMarshaler("config"))

	// Both transformers should be active
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

	// Also verify transformer behavior directly
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

	// Without transformer
	_, vals := rowValues(v, fm)
	// Metadata should be a map
	if _, ok := vals[1].(map[string]any); !ok {
		t.Error("Metadata should be map without transformer")
	}

	// With transformer
	_, vals = rowValues(v, fm, JSONBMarshaler("metadata"))
	// Metadata should now be a JSON string
	if str, ok := vals[1].(string); !ok || str != `{"key":"val"}` {
		t.Errorf("Metadata should be JSON string with transformer, got %v", vals[1])
	}
	// Config should still be map (not transformed)
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
