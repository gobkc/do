package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"
)

// globalSQLLog controls SQL logging for standalone functions (QueryRaw, ExecRaw, etc.).
var globalSQLLog bool

// SetGlobalSQLLog enables or disables SQL logging for standalone functions.
func SetGlobalSQLLog(enabled bool) {
	globalSQLLog = enabled
}

// CurdOption is a functional option for configuring Curd.
type CurdOption func(*curdConfig)

type curdConfig struct {
	sqlLogEnabled bool
}

// WithSQLLogging enables SQL logging for all operations on this Curd instance.
// When enabled, every CRUD operation will log the generated SQL and arguments
// via slog at Info level.
func WithSQLLogging() CurdOption {
	return func(c *curdConfig) { c.sqlLogEnabled = true }
}

type Table interface {
	TableName() string
}

type Curd[T Table] struct {
	q          Querier
	fm         FieldMapper
	dialect    Dialect
	transforms []FieldTransformer
	sqlLog     bool
}

func New[T Table](q Querier, fm FieldMapper, d Dialect, opts ...CurdOption) *Curd[T] {
	if fm == nil {
		fm = defaultFieldMapper{}
	}
	cfg := &curdConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return &Curd[T]{q: q, fm: fm, dialect: d, sqlLog: cfg.sqlLogEnabled}
}

func (c *Curd[T]) WithQuerier(q Querier) *Curd[T] {
	return &Curd[T]{q: q, fm: c.fm, dialect: c.dialect, transforms: c.transforms, sqlLog: c.sqlLog}
}

// WithSQLLog returns a new Curd with SQL logging enabled or disabled for
// subsequent operations. This allows per-operation control over logging.
//
// Usage:
//
//	// Log only this specific insert:
//	c.WithSQLLog(true).InsertOne(ctx, row)
//	// Subsequent operations on c are unaffected.
func (c *Curd[T]) WithSQLLog(enabled bool) *Curd[T] {
	return &Curd[T]{q: c.q, fm: c.fm, dialect: c.dialect, transforms: c.transforms, sqlLog: enabled}
}

// WithTransformer returns a new Curd that applies the given FieldTransformer
// to field values during insert operations. Multiple transformers can be
// composed with ComposeTransformers or by chaining WithTransformer calls.
//
// Usage:
//
//	c := New[MyTable](pool, nil, postgres.Dialect{}).
//	    WithTransformer(JSONBMarshaler("metadata", "config"))
func (c *Curd[T]) WithTransformer(t FieldTransformer) *Curd[T] {
	transforms := make([]FieldTransformer, len(c.transforms), len(c.transforms)+1)
	copy(transforms, c.transforms)
	transforms = append(transforms, t)
	return &Curd[T]{q: c.q, fm: c.fm, dialect: c.dialect, transforms: transforms, sqlLog: c.sqlLog}
}

func (c *Curd[T]) logSQL(ctx context.Context, query string, args ...any) {
	if !c.sqlLog {
		return
	}
	slog.InfoContext(ctx, "curd sql",
		slog.String("query", query),
		slog.Any("args", args),
	)
}

func logSQLGlobal(ctx context.Context, query string, args ...any) {
	if !globalSQLLog {
		return
	}
	slog.InfoContext(ctx, "curd sql",
		slog.String("query", query),
		slog.Any("args", args),
	)
}

func (c *Curd[T]) FindAll(ctx context.Context, where map[string]any, orderBy string, limit, offset int) ([]T, error) {
	var t T
	tableName := t.TableName()
	cols := columnsFromType(reflect.TypeOf(t), c.fm)

	conditions, args := buildWhere(where, &argCounter{idx: 1}, c.dialect)
	if hasField(t, "DeletedDate") {
		conditions = append(conditions, "deleted_date IS NULL")
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(cols, ","), tableName, whereClause)
	if orderBy != "" {
		query += " ORDER BY " + orderBy
	}
	a := &argCounter{idx: len(args) + 1}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %s", c.dialect.Placeholder(a.next()))
		args = append(args, limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %s", c.dialect.Placeholder(a.next()))
		args = append(args, offset)
	}

	c.logSQL(ctx, query, args...)
	rows, err := c.q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("findAll %s: %w", tableName, err)
	}
	defer rows.Close()
	return scanAllWithMapper[T](rows, c.fm)
}

func (c *Curd[T]) FindOne(ctx context.Context, where map[string]any) (T, error) {
	var zero T
	results, err := c.FindAll(ctx, where, "", 1, 0)
	if err != nil {
		return zero, err
	}
	if len(results) == 0 {
		return zero, fmt.Errorf("%T not found", zero)
	}
	return results[0], nil
}

func (c *Curd[T]) FindByID(ctx context.Context, id any) (T, error) {
	return c.FindOne(ctx, map[string]any{"id": id})
}

func (c *Curd[T]) InsertOne(ctx context.Context, row *T) error {
	v := reflect.ValueOf(row).Elem()
	t := v.Type()
	tableName := (*row).TableName()

	setNow(v, "CreatedDate")
	setNow(v, "ChangedDate")

	cols, vals := rowValues(v, c.fm, c.transforms...)
	placeholders := make([]string, len(vals))
	args := make([]any, len(vals))
	for i := range vals {
		placeholders[i] = c.dialect.Placeholder(i + 1)
		args[i] = vals[i]
	}

	returningClause := ""
	if hasField(t, "ID") {
		returningClause = " RETURNING id"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s",
		tableName, strings.Join(cols, ","), strings.Join(placeholders, ","), returningClause)

	if returningClause != "" {
		c.logSQL(ctx, query, args...)
		var id int64
		if err := c.q.QueryRow(ctx, query, args...).Scan(&id); err != nil {
			return fmt.Errorf("insert %s: %w", tableName, err)
		}
		setField(v, "ID", id)
		return nil
	}
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert %s: %w", tableName, err)
	}
	return nil
}

func (c *Curd[T]) InsertBatch(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	tableName := rows[0].TableName()

	// Derive the column list from the first row's rowValues so that
	// columns skipped by rowValues (e.g. zero-value ID) are consistent.
	pv0 := reflect.ValueOf(&rows[0])
	setNow(pv0, "CreatedDate")
	setNow(pv0, "ChangedDate")
	cols, _ := rowValues(pv0.Elem(), c.fm, c.transforms...)

	placeholders := make([]string, len(rows))
	args := []any{}
	argIdx := 1
	for i := range rows {
		pv := reflect.ValueOf(&rows[i])
		if i > 0 {
			setNow(pv, "CreatedDate")
			setNow(pv, "ChangedDate")
		}
		_, vals := rowValues(pv.Elem(), c.fm, c.transforms...)
		ph := make([]string, len(vals))
		for j := range vals {
			ph[j] = c.dialect.Placeholder(argIdx)
			argIdx++
		}
		placeholders[i] = "(" + strings.Join(ph, ",") + ")"
		args = append(args, vals...)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, strings.Join(cols, ","), strings.Join(placeholders, ","))
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert batch %s: %w", tableName, err)
	}
	return nil
}

func (c *Curd[T]) InsertBatchPtr(ctx context.Context, rows []*T) error {
	vals := make([]T, len(rows))
	for i, r := range rows {
		if r != nil {
			vals[i] = *r
		}
	}
	return c.InsertBatch(ctx, vals)
}

func (c *Curd[T]) UpdateByID(ctx context.Context, id any, updates map[string]any) error {
	var t T
	tableName := t.TableName()
	setClauses := []string{}
	args := []any{id}
	argIdx := 2
	for col, val := range updates {
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, c.dialect.Placeholder(argIdx)))
		args = append(args, val)
		argIdx++
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = %s", tableName, strings.Join(setClauses, ","), c.dialect.Placeholder(1))
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update %s: %w", tableName, err)
	}
	return nil
}

func (c *Curd[T]) UpdateWhere(ctx context.Context, where map[string]any, updates map[string]any) error {
	var t T
	tableName := t.TableName()
	setClauses := []string{}
	whereClauses := []string{}
	args := []any{}
	argIdx := 1
	for col, val := range updates {
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, c.dialect.Placeholder(argIdx)))
		args = append(args, val)
		argIdx++
	}
	for col, val := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", col, c.dialect.Placeholder(argIdx)))
		args = append(args, val)
		argIdx++
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(setClauses, ","), strings.Join(whereClauses, " AND "))
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update where %s: %w", tableName, err)
	}
	return nil
}

func (c *Curd[T]) DeleteByID(ctx context.Context, id any, hard bool) error {
	var t T
	tableName := t.TableName()
	if hard {
		query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", tableName, c.dialect.Placeholder(1))
		c.logSQL(ctx, query, id)
		_, err := c.q.Exec(ctx, query, id)
		return err
	}
	query := fmt.Sprintf("UPDATE %s SET deleted_date = %s WHERE id = %s", tableName, c.dialect.Placeholder(1), c.dialect.Placeholder(2))
	args := []any{time.Now(), id}
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	return err
}

func (c *Curd[T]) DeleteWhere(ctx context.Context, where map[string]any) error {
	var t T
	tableName := t.TableName()
	conditions, args := buildWhere(where, &argCounter{idx: 1}, c.dialect)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, strings.Join(conditions, " AND "))
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	return err
}

func (c *Curd[T]) Count(ctx context.Context, where map[string]any) (int64, error) {
	var t T
	tableName := t.TableName()
	conditions, args := buildWhere(where, &argCounter{idx: 1}, c.dialect)
	if hasField(t, "DeletedDate") {
		conditions = append(conditions, "deleted_date IS NULL")
	}
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", tableName, whereClause)
	var count int64
	c.logSQL(ctx, query, args...)
	err := c.q.QueryRow(ctx, query, args...).Scan(&count)
	return count, err
}

func (c *Curd[T]) Exists(ctx context.Context, where map[string]any) (bool, error) {
	var t T
	tableName := t.TableName()
	conditions, args := buildWhere(where, &argCounter{idx: 1}, c.dialect)
	if hasField(t, "DeletedDate") {
		conditions = append(conditions, "deleted_date IS NULL")
	}
	var exists bool
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s)", tableName, strings.Join(conditions, " AND "))
	c.logSQL(ctx, query, args...)
	err := c.q.QueryRow(ctx, query, args...).Scan(&exists)
	return exists, err
}

func QueryRaw[T any](ctx context.Context, q Querier, query string, args ...any) ([]T, error) {
	logSQLGlobal(ctx, query, args...)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query raw: %w", err)
	}
	defer rows.Close()
	return scanAllWithMapper[T](rows, rawFieldMapper{})
}

func QueryRowRaw[T any](ctx context.Context, q Querier, query string, args ...any) (T, error) {
	var zero T
	logSQLGlobal(ctx, query, args...)
	row := q.QueryRow(ctx, query, args...)
	result, err := scanRowWithMapper[T](row, rawFieldMapper{})
	if err != nil {
		return zero, err
	}
	return result, nil
}

func ExecRaw(ctx context.Context, q Querier, sql string, args ...any) (int64, error) {
	logSQLGlobal(ctx, sql, args...)
	tag, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("exec raw: %w", err)
	}
	return tag.RowsAffected(), nil
}

type rawFieldMapper struct{}

func (rawFieldMapper) ColumnName(f reflect.StructField) string {
	if f.Tag.Get("json") == "-" {
		return ""
	}
	if f.Tag.Get("gorm") == "-" {
		return ""
	}
	return f.Name
}

type argCounter struct{ idx int }

func (a *argCounter) next() int {
	n := a.idx
	a.idx++
	return n
}

func buildWhere(where map[string]any, a *argCounter, d Dialect) (conditions []string, args []any) {
	for col, val := range where {
		if val == nil {
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", col))
		} else if sl, ok := val.([]any); ok {
			conditions = append(conditions, fmt.Sprintf("%s = ANY(%s)", col, d.Placeholder(a.next())))
			args = append(args, sl)
		} else {
			conditions = append(conditions, fmt.Sprintf("%s = %s", col, d.Placeholder(a.next())))
			args = append(args, val)
		}
	}
	return
}

func nullSafeCopy(fields []reflect.Value, targets []any) {
	for i, f := range fields {
		if !f.CanSet() {
			continue
		}
		anyPtr, ok := targets[i].(*any)
		if !ok || anyPtr == nil || *anyPtr == nil {
			f.Set(reflect.Zero(f.Type()))
			continue
		}

		src := reflect.ValueOf(*anyPtr)
		if src.Type().AssignableTo(f.Type()) {
			f.Set(src)
			continue
		}
		if src.Type().ConvertibleTo(f.Type()) {
			f.Set(src.Convert(f.Type()))
			continue
		}

		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch src.Kind() {
			case reflect.Float64:
				f.SetInt(int64(src.Float()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				f.SetInt(src.Int())
			default:
				f.Set(reflect.Zero(f.Type()))
			}
		case reflect.Float32, reflect.Float64:
			switch src.Kind() {
			case reflect.Float64:
				f.SetFloat(src.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				f.SetFloat(float64(src.Int()))
			default:
				f.Set(reflect.Zero(f.Type()))
			}
		case reflect.String:
			f.SetString(fmt.Sprintf("%v", *anyPtr))
		case reflect.Bool:
			switch src.Kind() {
			case reflect.Bool:
				f.SetBool(src.Bool())
			default:
				f.Set(reflect.Zero(f.Type()))
			}
		default:
			f.Set(reflect.Zero(f.Type()))
		}
	}
}

func scanAllWithMapper[T any](rows Rows, fm FieldMapper) ([]T, error) {
	var results []T
	for rows.Next() {
		elem := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Elem()
		targets, fields := scanTargets(elem, fm)
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		nullSafeCopy(fields, targets)
		results = append(results, elem.Interface().(T))
	}
	return results, rows.Err()
}

func scanRowWithMapper[T any](row Row, fm FieldMapper) (T, error) {
	var zero T
	elem := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Elem()
	targets, fields := scanTargets(elem, fm)
	if err := row.Scan(targets...); err != nil {
		return zero, fmt.Errorf("scan row: %w", err)
	}
	nullSafeCopy(fields, targets)
	return elem.Interface().(T), nil
}

func hasField(v any, name string) bool {
	var t reflect.Type
	switch val := v.(type) {
	case reflect.Type:
		t = val
	case reflect.Value:
		t = val.Type()
	default:
		t = reflect.TypeOf(v)
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	_, ok := t.FieldByName(name)
	return ok
}

func setField(v reflect.Value, name string, val any) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	f := v.FieldByName(name)
	if f.IsValid() && f.CanSet() {
		rv := reflect.ValueOf(val)
		if rv.Type().AssignableTo(f.Type()) {
			f.Set(rv)
		}
	}
}

func setNow(v reflect.Value, name string) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	f := v.FieldByName(name)
	if f.IsValid() && f.CanSet() {
		switch f.Interface().(type) {
		case time.Time:
			f.Set(reflect.ValueOf(time.Now()))
		}
	}
}