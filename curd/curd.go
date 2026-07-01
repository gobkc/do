package curd

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
func WithSQLLogging() CurdOption {
	return func(c *curdConfig) { c.sqlLogEnabled = true }
}

// Table is the interface that entity types must implement.
type Table interface {
	TableName() string
}

// Curd is a type-safe CRUD operator for table T.
// All dependencies (Querier, Dialect, FieldMapper, FieldTransformer) are interfaces,
// enabling maximum decoupling. Create instances via New[T].
type Curd[T Table] struct {
	q          Querier
	fm         FieldMapper
	dialect    Dialect
	transforms []FieldTransformer
	sqlLog     bool
}

// New creates a Curd[T] instance. fm can be nil to use the default mapper
// (json/gorm tag + snake_case fallback). d supplies SQL dialect placeholders.
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

// WithQuerier returns a new Curd that uses the given Querier (e.g. a transaction)
// while sharing all other configuration. The original Curd is unchanged.
func (c *Curd[T]) WithQuerier(q Querier) *Curd[T] {
	return &Curd[T]{q: q, fm: c.fm, dialect: c.dialect, transforms: c.transforms, sqlLog: c.sqlLog}
}

// WithSQLLog returns a new Curd with SQL logging enabled or disabled for
// subsequent operations. This allows per-operation control over logging.
func (c *Curd[T]) WithSQLLog(enabled bool) *Curd[T] {
	return &Curd[T]{q: c.q, fm: c.fm, dialect: c.dialect, transforms: c.transforms, sqlLog: enabled}
}

// WithTransformer returns a new Curd that applies the given FieldTransformer
// to field values during insert operations. Multiple transformers compose
// via chaining or ComposeTransformers.
func (c *Curd[T]) WithTransformer(t FieldTransformer) *Curd[T] {
	transforms := make([]FieldTransformer, len(c.transforms), len(c.transforms)+1)
	copy(transforms, c.transforms)
	transforms = append(transforms, t)
	return &Curd[T]{q: c.q, fm: c.fm, dialect: c.dialect, transforms: transforms, sqlLog: c.sqlLog}
}

// --- Logging ---

func (c *Curd[T]) logSQL(ctx context.Context, query string, args ...any) {
	if !c.sqlLog {
		return
	}
	slog.InfoContext(ctx, "curd sql", slog.String("sql", formatSQL(query, args...)))
}

func logSQLGlobal(ctx context.Context, query string, args ...any) {
	if !globalSQLLog {
		return
	}
	slog.InfoContext(ctx, "curd sql", slog.String("sql", formatSQL(query, args...)))
}

// formatSQL interpolates parameter values into a SQL query string,
// replacing $1, $2, ... placeholders with formatted values.
// The result is for logging/display only — never use it to execute queries.
func formatSQL(query string, args ...any) string {
	if len(args) == 0 {
		return query
	}
	var buf strings.Builder
	i := 0
	for i < len(query) {
		if query[i] == '$' && i+1 < len(query) && isDigit(query[i+1]) {
			j := i + 1
			for j < len(query) && isDigit(query[j]) {
				j++
			}
			num := 0
			for k := i + 1; k < j; k++ {
				num = num*10 + int(query[k]-'0')
			}
			if num > 0 && num <= len(args) {
				buf.WriteString(formatArg(args[num-1]))
			} else {
				buf.WriteString(query[i:j])
			}
			i = j
		} else {
			buf.WriteByte(query[i])
			i++
		}
	}
	return buf.String()
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// formatArg formats a single argument value for display in SQL log output.
func formatArg(arg any) string {
	if arg == nil {
		return "NULL"
	}
	switch v := arg.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case time.Time:
		return "'" + v.Format(time.RFC3339Nano) + "'"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", v)
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
	default:
		// Handle slices (e.g. for = ANY($1) or IN clauses)
		rv := reflect.ValueOf(arg)
		if rv.Kind() == reflect.Slice {
			var parts []string
			for k := 0; k < rv.Len(); k++ {
				parts = append(parts, formatArg(rv.Index(k).Interface()))
			}
			return strings.Join(parts, ", ")
		}
		return fmt.Sprintf("'%v'", v)
	}
}

// --- Internal helpers ---

// tableName safely extracts the table name from type parameter T.
// It handles both value types (e.g. VPaymentOrder) and pointer types
// (e.g. *VPaymentOrder). When T is a pointer, var t T would be nil and
// calling a value-receiver TableName() on a nil pointer would panic.
func tableName[T Table]() string {
	var zero T
	rv := reflect.ValueOf(&zero).Elem()
	if rv.Kind() == reflect.Ptr {
		return reflect.New(rv.Type().Elem()).Interface().(Table).TableName()
	}
	return zero.TableName()
}

// buildWhereClause evaluates a Predicate and combines it with the soft-delete
// filter (deleted_date IS NULL) when the entity has a DeletedDate field.
// Returns the complete " WHERE ..." clause and collected arguments.
func (c *Curd[T]) buildWhereClause(where Predicate) (clause string, args []any) {
	var t T

	userClause, userArgs := buildPredicate(where, c.dialect)

	var parts []string
	if userClause != "" {
		parts = append(parts, userClause)
	}
	if hasField(t, "DeletedDate") {
		parts = append(parts, "deleted_date IS NULL")
	}

	if len(parts) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(parts, " AND "), userArgs
}

// --- Query methods ---

// FindAll returns all rows matching the predicate, ordered and paginated.
// Pass nil for where to include all rows. orderBy can be empty.
func (c *Curd[T]) FindAll(ctx context.Context, where Predicate, orderBy string, limit, offset int) ([]T, error) {
	var t T
	name := tableName[T]()
	cols := columnsFromType(reflect.TypeOf(t), c.fm)

	whereClause, args := c.buildWhereClause(where)

	query := fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(cols, ","), name, whereClause)
	if orderBy != "" {
		query += " ORDER BY " + orderBy
	}
	nextIdx := len(args) + 1
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %s", c.dialect.Placeholder(nextIdx))
		args = append(args, limit)
		nextIdx++
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %s", c.dialect.Placeholder(nextIdx))
		args = append(args, offset)
	}

	c.logSQL(ctx, query, args...)
	rows, err := c.q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("findAll %s: %w", name, err)
	}
	defer rows.Close()
	return scanAllWithMapper[T](rows, c.fm)
}

// FindOne returns a single row matching the predicate, or an error if not found.
func (c *Curd[T]) FindOne(ctx context.Context, where Predicate) (T, error) {
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

// FindByID returns a single row by its primary key "id".
func (c *Curd[T]) FindByID(ctx context.Context, id any) (T, error) {
	return c.FindOne(ctx, Eq("id", id))
}

// Find is a general-purpose query method driven by functional options.
// It supports JOINs, column selection, filtering, ordering, and pagination.
//
// Usage:
//
//	results, err := c.Find(ctx,
//	    curd.WithJoins(curd.JoinClause{Type: curd.LeftJoin, Table: "role AS r", On: "r.name = t.role_name"}),
//	    curd.WithColumns("t.id", "t.name", "r.label"),
//	    curd.WithWhere(curd.Eq("t.status", "active")),
//	    curd.WithOrderBy("t.id ASC"),
//	    curd.WithLimit(10),
//	)
func (c *Curd[T]) Find(ctx context.Context, opts ...FindOption) ([]T, error) {
	cfg := resolveFindConfig(opts)

	var t T
	name := tableName[T]()

	cols := cfg.columns
	if len(cols) == 0 {
		cols = columnsFromType(reflect.TypeOf(t), c.fm)
	}

	fromClause := name
	for _, j := range cfg.joins {
		fromClause += fmt.Sprintf(" %s JOIN %s ON %s", j.Type, j.Table, j.On)
	}

	whereClause, args := c.buildWhereClause(cfg.where)

	query := fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(cols, ","), fromClause, whereClause)
	if cfg.orderBy != "" {
		query += " ORDER BY " + cfg.orderBy
	}
	nextIdx := len(args) + 1
	if cfg.limit > 0 {
		query += fmt.Sprintf(" LIMIT %s", c.dialect.Placeholder(nextIdx))
		args = append(args, cfg.limit)
		nextIdx++
	}
	if cfg.offset > 0 {
		query += fmt.Sprintf(" OFFSET %s", c.dialect.Placeholder(nextIdx))
		args = append(args, cfg.offset)
	}

	c.logSQL(ctx, query, args...)
	rows, err := c.q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find %s: %w", name, err)
	}
	defer rows.Close()
	return scanAllWithMapper[T](rows, c.fm)
}

// FindPaginated returns a page of results together with the total count.
// The count query wraps the same FROM/JOIN/WHERE in a subquery to correctly
// handle JOINs.
func (c *Curd[T]) FindPaginated(ctx context.Context, opts ...FindOption) (*PaginatedResult[T], error) {
	cfg := resolveFindConfig(opts)
	// Don't allow limit/offset on the count query
	countCfg := *cfg
	countCfg.limit = 0
	countCfg.offset = 0
	countCfg.orderBy = ""
	countCfg.columns = []string{"1"}

	name := tableName[T]()

	fromClause := name
	for _, j := range cfg.joins {
		fromClause += fmt.Sprintf(" %s JOIN %s ON %s", j.Type, j.Table, j.On)
	}

	whereClause, whereArgs := c.buildWhereClause(cfg.where)

	// COUNT uses a subquery to handle JOINs correctly
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s%s) AS _curd_count", fromClause, whereClause)
	var total int64
	c.logSQL(ctx, countQuery, whereArgs...)
	if err := c.q.QueryRow(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		return nil, fmt.Errorf("findPaginated count %s: %w", name, err)
	}

	list, err := c.Find(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &PaginatedResult[T]{List: list, Total: total}, nil
}

// --- Insert methods ---

// InsertOne inserts a single row. If the entity has an ID field, the generated
// id is set back on the row via RETURNING. CreatedDate and ChangedDate are
// auto-set to time.Now().
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

// InsertBatch inserts multiple rows in a single statement.
// CreatedDate and ChangedDate are auto-set on each row.
func (c *Curd[T]) InsertBatch(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	tableName := rows[0].TableName()

	pv0 := reflect.ValueOf(&rows[0])
	setNow(pv0, "CreatedDate")
	setNow(pv0, "ChangedDate")
	cols, _ := rowValues(pv0.Elem(), c.fm, c.transforms...)

	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(cols))
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

// InsertBatchPtr inserts multiple rows (given as pointers) in a single statement.
// nil elements become zero-value rows.
func (c *Curd[T]) InsertBatchPtr(ctx context.Context, rows []*T) error {
	vals := make([]T, len(rows))
	for i, r := range rows {
		if r != nil {
			vals[i] = *r
		}
	}
	return c.InsertBatch(ctx, vals)
}

// --- Update methods ---

// UpdateByID updates a row identified by its primary key "id".
func (c *Curd[T]) UpdateByID(ctx context.Context, id any, updates map[string]any) error {
	tableName := tableName[T]()
	setClauses := make([]string, 0, len(updates))
	args := make([]any, 1, 1+len(updates))
	args[0] = id
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

// UpdateWhere updates rows matching the predicate.
func (c *Curd[T]) UpdateWhere(ctx context.Context, where Predicate, updates map[string]any) error {
	tableName := tableName[T]()

	// Build SET clause (starts at $1)
	setClauses := make([]string, 0, len(updates))
	args := make([]any, 0, len(updates)+4) // +4 for typical WHERE args
	argIdx := 1
	for col, val := range updates {
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, c.dialect.Placeholder(argIdx)))
		args = append(args, val)
		argIdx++
	}

	// Build WHERE from predicate
	whereClause, whereArgs := buildPredicate(where, c.dialect)
	whereSQL := ""
	if whereClause != "" {
		// Re-number where placeholders to continue after SET args
		renumbered := renumberPlaceholders(whereClause, c.dialect, argIdx)
		whereSQL = " WHERE " + renumbered
		args = append(args, whereArgs...)
	}

	query := fmt.Sprintf("UPDATE %s SET %s%s", tableName, strings.Join(setClauses, ","), whereSQL)
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update where %s: %w", tableName, err)
	}
	return nil
}

// --- Delete methods ---

// DeleteByID deletes a row by its primary key. If hard is false, performs a
// soft delete by setting deleted_date. If hard is true, performs a hard DELETE.
func (c *Curd[T]) DeleteByID(ctx context.Context, id any, hard bool) error {
	tableName := tableName[T]()
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

// DeleteWhere hard-deletes rows matching the predicate.
func (c *Curd[T]) DeleteWhere(ctx context.Context, where Predicate) error {
	tableName := tableName[T]()
	whereClause, args := buildPredicate(where, c.dialect)
	whereSQL := ""
	if whereClause != "" {
		whereSQL = " WHERE " + whereClause
	}
	query := fmt.Sprintf("DELETE FROM %s%s", tableName, whereSQL)
	c.logSQL(ctx, query, args...)
	_, err := c.q.Exec(ctx, query, args...)
	return err
}

// --- Aggregate methods ---

// Count returns the number of rows matching the predicate.
// Soft-deleted rows (deleted_date IS NOT NULL) are automatically excluded.
func (c *Curd[T]) Count(ctx context.Context, where Predicate) (int64, error) {
	tableName := tableName[T]()
	whereClause, args := c.buildWhereClause(where)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", tableName, whereClause)
	var count int64
	c.logSQL(ctx, query, args...)
	err := c.q.QueryRow(ctx, query, args...).Scan(&count)
	return count, err
}

// Exists returns true if at least one row matches the predicate.
// Soft-deleted rows are automatically excluded.
func (c *Curd[T]) Exists(ctx context.Context, where Predicate) (bool, error) {
	tableName := tableName[T]()
	whereClause, args := c.buildWhereClause(where)
	whereSQL := ""
	if whereClause != "" {
		whereSQL = whereClause
	}
	var exists bool
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s%s)", tableName, whereSQL)
	c.logSQL(ctx, query, args...)
	err := c.q.QueryRow(ctx, query, args...).Scan(&exists)
	return exists, err
}

// --- Upsert / Save ---

// Upsert inserts the row if no record matches the predicate, or updates
// matching records with the row's values if one exists.
//
// The where predicate identifies existing records. Every column from row
// (including zero values) is applied during update, matching the behaviour
// of GORM's Where(...).Assign(...). Use Save for the simpler "upsert by id"
// case.
//
// This is NOT an atomic operation — it runs a SELECT followed by INSERT
// or UPDATE. It does not require database constraints.
//
// Usage:
//
//	row := &MyTable{Name: "test", Status: "active", Value: 100}
//	err := c.Upsert(ctx,
//	    curd.And(curd.Eq("name", "test"), curd.Eq("source", "api")),
//	    row,
//	)
func (c *Curd[T]) Upsert(ctx context.Context, where Predicate, row *T) error {
	v := reflect.ValueOf(row).Elem()

	exists, err := c.Exists(ctx, where)
	if err != nil {
		return fmt.Errorf("upsert exists: %w", err)
	}

	if exists {
		setNow(v, "ChangedDate")
		updates := structToUpdates(v, c.fm, c.transforms)
		return c.UpdateWhere(ctx, where, updates)
	}

	setNow(v, "CreatedDate")
	setNow(v, "ChangedDate")
	return c.InsertOne(ctx, row)
}

// Save upserts by primary key "id":
//   - If row.ID is zero → INSERT
//   - If row.ID is non-zero → check if exists; UPDATE all row fields if yes,
//     INSERT if no
//
// This is a convenience wrapper around Upsert with an id-based predicate.
func (c *Curd[T]) Save(ctx context.Context, row *T) error {
	v := reflect.ValueOf(row).Elem()

	idField := v.FieldByName("ID")
	if !idField.IsValid() || idField.IsZero() {
		return c.InsertOne(ctx, row)
	}

	return c.Upsert(ctx, Eq("id", idField.Interface()), row)
}

// structToUpdates converts a struct value to a map[string]any suitable for
// UpdateWhere. All mapped columns are included (zero values included).
// Field transformers are applied.
func structToUpdates(v reflect.Value, fm FieldMapper, transforms []FieldTransformer) map[string]any {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	t := v.Type()
	updates := make(map[string]any)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		col := fm.ColumnName(f)
		if col == "" || col == "id" {
			continue
		}
		val := v.Field(i).Interface()
		for _, tr := range transforms {
			val = tr(col, val)
		}
		updates[col] = val
	}
	return updates
}

// --- Utility methods ---

// Pluck extracts values of a single column into a slice.
//
// Usage:
//
//	names, err := c.Pluck(ctx, "name", curd.Eq("status", "active"))
func (c *Curd[T]) Pluck(ctx context.Context, column string, where Predicate) ([]any, error) {
	tableName := tableName[T]()
	whereClause, args := c.buildWhereClause(where)
	query := fmt.Sprintf("SELECT %s FROM %s%s", column, tableName, whereClause)
	c.logSQL(ctx, query, args...)
	rows, err := c.q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pluck %s: %w", tableName, err)
	}
	defer rows.Close()

	var results []any
	for rows.Next() {
		var val any
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("pluck scan: %w", err)
		}
		results = append(results, val)
	}
	return results, rows.Err()
}

// Each iterates over rows matching the predicate in batches of batchSize,
// calling fn for each batch. Iteration stops when fn returns an error or
// all rows have been consumed.
//
// Usage:
//
//	err := c.Each(ctx, curd.Gt("id", 0), 500, func(batch []T) error {
//	    for _, row := range batch { process(row) }
//	    return nil
//	})
func (c *Curd[T]) Each(ctx context.Context, where Predicate, batchSize int, fn func([]T) error) error {
	if batchSize <= 0 {
		batchSize = 500
	}
	offset := 0
	for {
		results, err := c.FindAll(ctx, where, "id ASC", batchSize, offset)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			return nil
		}
		if err := fn(results); err != nil {
			return err
		}
		if len(results) < batchSize {
			return nil
		}
		offset += batchSize
	}
}

// --- FindOption types ---

// FindOption is a functional option for the Find and FindPaginated methods.
type FindOption func(*findConfig)

type findConfig struct {
	where   Predicate
	joins   []JoinClause
	columns []string
	orderBy string
	limit   int
	offset  int
}

// JoinType represents a SQL JOIN type.
type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
)

// JoinClause describes a SQL JOIN.
type JoinClause struct {
	Type  JoinType // "INNER", "LEFT", "RIGHT"
	Table string   // table name and optional alias, e.g. "role AS r"
	On    string   // JOIN condition, e.g. "r.name = t.role_name"
}

// PaginatedResult holds a page of results together with the total count.
type PaginatedResult[T any] struct {
	List  []T
	Total int64
}

// WithWhere sets the WHERE predicate for Find.
func WithWhere(p Predicate) FindOption {
	return func(c *findConfig) { c.where = p }
}

// WithJoins adds JOIN clauses to the query.
func WithJoins(joins ...JoinClause) FindOption {
	return func(c *findConfig) { c.joins = append(c.joins, joins...) }
}

// WithColumns specifies which columns to SELECT. If empty, all columns
// are selected using the FieldMapper.
func WithColumns(cols ...string) FindOption {
	return func(c *findConfig) { c.columns = append(c.columns, cols...) }
}

// WithOrderBy sets the ORDER BY clause.
func WithOrderBy(orderBy string) FindOption {
	return func(c *findConfig) { c.orderBy = orderBy }
}

// WithLimit sets the LIMIT clause.
func WithLimit(n int) FindOption {
	return func(c *findConfig) { c.limit = n }
}

// WithOffset sets the OFFSET clause.
func WithOffset(n int) FindOption {
	return func(c *findConfig) { c.offset = n }
}

func resolveFindConfig(opts []FindOption) *findConfig {
	cfg := &findConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// renumberPlaceholders rewrites placeholder numbers in a SQL fragment
// by adding an offset. For example, with offset=3, "$1 AND $2" becomes "$4 AND $5".
// This is used when combining independently-built SQL fragments.
func renumberPlaceholders(sql string, d Dialect, offset int) string {
	if offset <= 1 {
		return sql
	}
	delta := offset - 1
	var buf strings.Builder
	buf.Grow(len(sql) + 4) // extra space for longer numbers
	i := 0
	for i < len(sql) {
		if sql[i] == '$' && i+1 < len(sql) && isDigit(sql[i+1]) {
			j := i + 1
			for j < len(sql) && isDigit(sql[j]) {
				j++
			}
			num := 0
			for k := i + 1; k < j; k++ {
				num = num*10 + int(sql[k]-'0')
			}
			buf.WriteString(d.Placeholder(num + delta))
			i = j
		} else {
			buf.WriteByte(sql[i])
			i++
		}
	}
	return buf.String()
}

// --- Standalone raw query functions ---

// QueryRaw executes a raw SQL query and scans results into []T.
// Column mapping uses Go field names directly (no json/gorm tag processing).
func QueryRaw[T any](ctx context.Context, q Querier, query string, args ...any) ([]T, error) {
	logSQLGlobal(ctx, query, args...)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query raw: %w", err)
	}
	defer rows.Close()
	return scanAllWithMapper[T](rows, rawFieldMapper{})
}

// QueryRowRaw executes a raw SQL query and scans a single row into T.
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

// ExecRaw executes a raw SQL statement and returns the number of rows affected.
func ExecRaw(ctx context.Context, q Querier, sql string, args ...any) (int64, error) {
	logSQLGlobal(ctx, sql, args...)
	tag, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("exec raw: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --- Field mapper for raw queries ---

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

// --- Scan utilities ---

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
		elem := newT[T]()
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
	elem := newT[T]()
	targets, fields := scanTargets(elem, fm)
	if err := row.Scan(targets...); err != nil {
		return zero, fmt.Errorf("scan row: %w", err)
	}
	nullSafeCopy(fields, targets)
	return elem.Interface().(T), nil
}

// newT creates a new zero value of type T and returns it as a reflect.Value.
func newT[T any]() reflect.Value {
	var zero T
	typ := reflect.TypeOf(zero)
	if typ != nil && typ.Kind() == reflect.Ptr {
		return reflect.New(typ.Elem())
	}
	return reflect.New(typ).Elem()
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
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	_, ok := t.FieldByName(name)
	return ok
}

func setField(v reflect.Value, name string, val any) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
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
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
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
