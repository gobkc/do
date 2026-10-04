package curd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// typeMetaCache caches per-entity-type metadata derived from reflection:
// table names and soft-delete (DeletedDate) support. Keys are the
// reflect.Type of T (deref not needed — each T maps to exactly one entry).
// hasDeletedCache caches whether entity types support soft-delete
// (DeletedDate field). Unlike TableName, this is purely type-derived and
// stable for the lifetime of the process.
var hasDeletedCache sync.Map // map[reflect.Type]bool

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

func (c *Curd[T]) logSQL(ctx context.Context, query string, args ...any) func() {
	if !c.sqlLog {
		return func() {}
	}
	start := time.Now().UTC()
	return func() {
		slog.InfoContext(ctx, "curd sql",
			slog.String("sql", formatSQL(query, args...)),
			slog.Duration("cost", time.Since(start)),
		)
	}
}

func logSQLGlobal(ctx context.Context, query string, args ...any) func() {
	if !globalSQLLog {
		return func() {}
	}
	start := time.Now().UTC()
	return func() {
		slog.InfoContext(ctx, "curd sql",
			slog.String("sql", formatSQL(query, args...)),
			slog.Duration("cost", time.Since(start)),
		)
	}
}

// formatSQL interpolates parameter values into a SQL query string,
// replacing $1, $2, ... placeholders with formatted values.
// The result is for logging/display only — never use it to execute queries.
func formatSQL(query string, args ...any) string {
	if len(args) == 0 {
		return query
	}
	var buf strings.Builder
	buf.Grow(len(query) + len(args)*8)
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
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
	default:
		// Handle slices (e.g. for = ANY($1) or IN clauses)
		rv := reflect.ValueOf(arg)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			n := rv.Len()
			parts := make([]string, 0, n)
			for k := 0; k < n; k++ {
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
// Not cached: TableName() may legitimately depend on runtime state, so it
// is evaluated on every call exactly as before.
func tableName[T Table]() string {
	var zero T
	rv := reflect.ValueOf(&zero).Elem()
	if rv.Kind() == reflect.Ptr {
		return reflect.New(rv.Type().Elem()).Interface().(Table).TableName()
	}
	return zero.TableName()
}

// typeHasDeletedDate reports whether entities of type T carry a DeletedDate
// field (soft-delete support). The reflection result is cached per T.
func typeHasDeletedDate[T Table]() bool {
	key := reflect.TypeOf((*T)(nil)).Elem()
	if v, ok := hasDeletedCache.Load(key); ok {
		return v.(bool)
	}
	var t T
	ok := hasFieldUncached(t, "DeletedDate")
	actual, loaded := hasDeletedCache.LoadOrStore(key, ok)
	if loaded {
		return actual.(bool)
	}
	return ok
}

// buildWhereClause evaluates a Predicate and combines it with the soft-delete
// filter (deleted_date IS NULL) when the entity has a DeletedDate field.
// Returns the complete " WHERE ..." clause and collected arguments.
func (c *Curd[T]) buildWhereClause(where Predicate) (clause string, args []any) {
	userClause, userArgs := buildPredicate(where, c.dialect)
	hasDeleted := typeHasDeletedDate[T]()

	// Avoid slice allocation + Join for the common cases.
	if userClause == "" {
		if !hasDeleted {
			return "", nil
		}
		return " WHERE deleted_date IS NULL", userArgs
	}
	if !hasDeleted {
		return " WHERE " + userClause, userArgs
	}
	return " WHERE " + userClause + " AND deleted_date IS NULL", userArgs
}

// --- Query methods ---

// FindAll returns all rows matching the predicate, ordered and paginated.
// Pass nil for where to include all rows. orderBy can be empty.
func (c *Curd[T]) FindAll(ctx context.Context, where Predicate, orderBy string, limit, offset int) ([]T, error) {
	name := tableName[T]()
	cols := columnsJoinedFromType(reflect.TypeFor[T](), c.fm)

	whereClause, args := c.buildWhereClause(where)

	query := "SELECT " + cols + " FROM " + name + whereClause
	if orderBy != "" {
		query += " ORDER BY " + orderBy
	}
	nextIdx := len(args) + 1
	if limit > 0 {
		query += " LIMIT " + c.dialect.Placeholder(nextIdx)
		args = append(args, limit)
		nextIdx++
	}
	if offset > 0 {
		query += " OFFSET " + c.dialect.Placeholder(nextIdx)
		args = append(args, offset)
	}

	defer c.logSQL(ctx, query, args...)()
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

	name := tableName[T]()

	var cols string
	if len(cfg.columns) == 0 {
		cols = columnsJoinedFromType(reflect.TypeFor[T](), c.fm)
	} else {
		cols = strings.Join(cfg.columns, ",")
	}

	var fromBuilder strings.Builder
	fromBuilder.Grow(len(name) + len(cfg.joins)*32)
	fromBuilder.WriteString(name)
	for _, j := range cfg.joins {
		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(string(j.Type))
		fromBuilder.WriteString(" JOIN ")
		fromBuilder.WriteString(j.Table)
		fromBuilder.WriteString(" ON ")
		fromBuilder.WriteString(j.On)
	}
	fromClause := fromBuilder.String()

	whereClause, args := c.buildWhereClause(cfg.where)

	query := "SELECT " + cols + " FROM " + fromClause + whereClause
	if cfg.orderBy != "" {
		query += " ORDER BY " + cfg.orderBy
	}
	nextIdx := len(args) + 1
	if cfg.limit > 0 {
		query += " LIMIT " + c.dialect.Placeholder(nextIdx)
		args = append(args, cfg.limit)
		nextIdx++
	}
	if cfg.offset > 0 {
		query += " OFFSET " + c.dialect.Placeholder(nextIdx)
		args = append(args, cfg.offset)
	}

	defer c.logSQL(ctx, query, args...)()
	rows, err := c.q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find %s: %w", name, err)
	}
	defer rows.Close()
	return scanAllWithMapper[T](rows, c.fm)
}

// FindPaginated returns a page of results together with the total count.
// The count query reuses the same FROM/WHERE; when JOINs are present it is
// wrapped in a subquery to count joined rows correctly.
func (c *Curd[T]) FindPaginated(ctx context.Context, opts ...FindOption) (*PaginatedResult[T], error) {
	cfg := resolveFindConfig(opts)
	// Don't allow limit/offset on the count query
	countCfg := *cfg
	countCfg.limit = 0
	countCfg.offset = 0
	countCfg.orderBy = ""
	countCfg.columns = []string{"1"}

	name := tableName[T]()

	var fromBuilder strings.Builder
	fromBuilder.Grow(len(name) + len(cfg.joins)*32)
	fromBuilder.WriteString(name)
	for _, j := range cfg.joins {
		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(string(j.Type))
		fromBuilder.WriteString(" JOIN ")
		fromBuilder.WriteString(j.Table)
		fromBuilder.WriteString(" ON ")
		fromBuilder.WriteString(j.On)
	}
	fromClause := fromBuilder.String()

	whereClause, whereArgs := c.buildWhereClause(cfg.where)

	// Fast path without JOINs: plain COUNT(*) avoids the subquery overhead.
	// With JOINs the subquery is required to count joined rows correctly.
	var countQuery string
	if len(cfg.joins) == 0 {
		countQuery = "SELECT COUNT(*) FROM " + fromClause + whereClause
	} else {
		countQuery = "SELECT COUNT(*) FROM (SELECT 1 FROM " + fromClause + whereClause + ") AS _curd_count"
	}
	var total int64
	defer c.logSQL(ctx, countQuery, whereArgs...)()
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
	idFieldName := ""
	if _, ok := cachedFieldIndex(t, "ID"); ok {
		idFieldName = "ID"
	} else if _, ok := cachedFieldIndex(t, "Id"); ok {
		idFieldName = "Id"
	}
	if idFieldName != "" {
		returningClause = " RETURNING id"
	}

	query := "INSERT INTO " + tableName + " (" + strings.Join(cols, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")" + returningClause

	if returningClause != "" {
		defer c.logSQL(ctx, query, args...)()
		var id int64
		if err := c.q.QueryRow(ctx, query, args...).Scan(&id); err != nil {
			return fmt.Errorf("insert %s: %w", tableName, err)
		}
		setField(v, idFieldName, id)
		return nil
	}
	defer c.logSQL(ctx, query, args...)()
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

	query := "INSERT INTO " + tableName + " (" + strings.Join(cols, ",") + ") VALUES " + strings.Join(placeholders, ",")
	defer c.logSQL(ctx, query, args...)()
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
		setClauses = append(setClauses, col+" = "+c.dialect.Placeholder(argIdx))
		args = append(args, val)
		argIdx++
	}
	query := "UPDATE " + tableName + " SET " + strings.Join(setClauses, ",") + " WHERE id = " + c.dialect.Placeholder(1)
	defer c.logSQL(ctx, query, args...)()
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
		setClauses = append(setClauses, col+" = "+c.dialect.Placeholder(argIdx))
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

	query := "UPDATE " + tableName + " SET " + strings.Join(setClauses, ",") + whereSQL
	defer c.logSQL(ctx, query, args...)()
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
		query := "DELETE FROM " + tableName + " WHERE id = " + c.dialect.Placeholder(1)
		defer c.logSQL(ctx, query, id)()
		_, err := c.q.Exec(ctx, query, id)
		return err
	}
	query := "UPDATE " + tableName + " SET deleted_date = " + c.dialect.Placeholder(1) + " WHERE id = " + c.dialect.Placeholder(2)
	args := []any{time.Now().UTC(), id}
	defer c.logSQL(ctx, query, args...)()
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
	query := "DELETE FROM " + tableName + whereSQL
	defer c.logSQL(ctx, query, args...)()
	_, err := c.q.Exec(ctx, query, args...)
	return err
}

// --- Aggregate methods ---

// Count returns the number of rows matching the predicate.
// Soft-deleted rows (deleted_date IS NOT NULL) are automatically excluded.
func (c *Curd[T]) Count(ctx context.Context, where Predicate) (int64, error) {
	tableName := tableName[T]()
	whereClause, args := c.buildWhereClause(where)
	query := "SELECT COUNT(*) FROM " + tableName + whereClause
	var count int64
	defer c.logSQL(ctx, query, args...)()
	err := c.q.QueryRow(ctx, query, args...).Scan(&count)
	return count, err
}

// Exists returns true if at least one row matches the predicate.
// Soft-deleted rows are automatically excluded.
func (c *Curd[T]) Exists(ctx context.Context, where Predicate) (bool, error) {
	tableName := tableName[T]()
	whereClause, args := c.buildWhereClause(where)
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM " + tableName + whereClause + ")"
	defer c.logSQL(ctx, query, args...)()
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
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return c.InsertOne(ctx, row)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return c.InsertOne(ctx, row)
	}
	var idField reflect.Value
	if path, ok := cachedFieldIndex(v.Type(), "ID"); ok {
		idField = v.FieldByIndex(path)
	} else if path, ok := cachedFieldIndex(v.Type(), "Id"); ok {
		idField = v.FieldByIndex(path)
	} else {
		idField = v.FieldByName("ID")
		if !idField.IsValid() {
			idField = v.FieldByName("Id")
		}
	}
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
	if t.Kind() != reflect.Struct {
		return nil
	}
	// Fast path for the default mapper: reuse the cached column plan.
	if _, ok := fm.(defaultFieldMapper); ok {
		p := getRowPlan(t)
		updates := make(map[string]any, len(p.cols))
		for pos := 0; pos < len(p.cols); pos++ {
			col := p.cols[pos]
			if col == "id" {
				continue
			}
			val := v.Field(p.idx[pos]).Interface()
			for _, tr := range transforms {
				val = tr(col, val)
			}
			updates[col] = val
		}
		return updates
	}
	updates := make(map[string]any, t.NumField())
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
	query := "SELECT " + column + " FROM " + tableName + whereClause
	defer c.logSQL(ctx, query, args...)()
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
	defer logSQLGlobal(ctx, query, args...)()
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
	defer logSQLGlobal(ctx, query, args...)()
	row := q.QueryRow(ctx, query, args...)
	result, err := scanRowWithMapper[T](row, rawFieldMapper{})
	if err != nil {
		return zero, err
	}
	return result, nil
}

// ExecRaw executes a raw SQL statement and returns the number of rows affected.
func ExecRaw(ctx context.Context, q Querier, sql string, args ...any) (int64, error) {
	defer logSQLGlobal(ctx, sql, args...)()
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

// numericString converts a pgtype.Numeric-like struct to its decimal string
// representation. pgx v5 returns pgtype.Numeric (struct with Int *big.Int and
// Exp int32 fields) for PostgreSQL NUMERIC columns when scanning into *any via
// binary protocol. This function uses reflection to detect the struct without
// importing pgtype, so that types like decimal.Decimal (which implement
// sql.Scanner but don't recognise pgtype.Numeric) can still read NUMERIC values.
func numericString(src reflect.Value) (string, bool) {
	if src.Kind() != reflect.Struct {
		return "", false
	}
	intField := src.FieldByName("Int")
	expField := src.FieldByName("Exp")
	if !intField.IsValid() || !expField.IsValid() {
		return "", false
	}
	// Int must be *big.Int
	if intField.Type() != bigIntPtrType {
		return "", false
	}
	// Exp must be int32
	if expField.Kind() != reflect.Int32 {
		return "", false
	}
	// Check Valid — skip NULL (shouldn't reach here for NULL, but be safe)
	if v := src.FieldByName("Valid"); v.IsValid() && v.Kind() == reflect.Bool && !v.Bool() {
		return "", false
	}
	// Check NaN
	if v := src.FieldByName("NaN"); v.IsValid() && v.Kind() == reflect.Bool && v.Bool() {
		return "", false
	}
	// Check InfinityModifier (0 = Finite, non-zero = Infinity/-Infinity)
	if v := src.FieldByName("InfinityModifier"); v.IsValid() && v.Int() != 0 {
		return "", false
	}

	bi := intField.Interface().(*big.Int)
	if bi == nil {
		return "", false
	}
	exp := int(expField.Int())
	s := bi.String()

	if exp >= 0 {
		// Integer (or with trailing zeros), e.g. Exp=2 => "5000" + "00" = "500000"
		return s + strings.Repeat("0", exp), true
	}

	// Negative exponent: insert decimal point, e.g. Exp=-2 => "50.00"
	absExp := -exp
	if absExp >= len(s) {
		// Need leading zeros, e.g. 5 with Exp=-3 => "0.005"
		s = strings.Repeat("0", absExp-len(s)) + s
		return "0." + s, true
	}
	dotPos := len(s) - absExp
	return s[:dotPos] + "." + s[dotPos:], true
}

// jsonbText detects pgx v5 binary JSONB format (version byte 1 + JSON text)
// and returns the decoded JSON string. pgx v5, when using the binary protocol,
// wraps JSONB values with a 1-byte version header per the PostgreSQL binary
// format. Types like Jsonb[T] that implement sql.Scanner expect plain JSON
// text — the version byte causes json.Unmarshal to fail. Stripping it here
// allows sql.Scanner implementations to work transparently without requiring
// ::text casts in every query.
func jsonbText(src reflect.Value) (string, bool) {
	if src.Kind() != reflect.Slice {
		return "", false
	}
	// Only handle []byte (not []int, []string, etc.)
	if src.Type().Elem().Kind() != reflect.Uint8 {
		return "", false
	}
	b := src.Bytes()
	if len(b) == 0 || b[0] != 1 {
		return "", false
	}
	return string(b[1:]), true
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
			// Support types implementing sql.Scanner (sql.NullString,
			// sql.NullTime, sql.NullInt64, sql.NullFloat64, sql.NullBool,
			// custom Jsonb[T], decimal.Decimal, etc.).
			if f.CanAddr() {
				if scanner, ok := f.Addr().Interface().(sql.Scanner); ok {
					err := scanner.Scan(*anyPtr)
					if err != nil {
						// pgx v5 binary protocol returns pgtype.Numeric
						// for PostgreSQL NUMERIC columns. Types like
						// decimal.Decimal don't recognise that struct,
						// so convert it to a plain decimal string and retry.
						if s, ok2 := numericString(src); ok2 {
							err = scanner.Scan(s)
						}
						// pgx v5 binary protocol wraps JSONB values with a
						// 1-byte version header. Strip it so sql.Scanner
						// implementations (e.g. Jsonb[T]) receive plain JSON.
						if err != nil {
							if s, ok2 := jsonbText(src); ok2 {
								err = scanner.Scan(s)
							}
						}
					}
					if err != nil {
						f.Set(reflect.Zero(f.Type()))
					}
					continue
				}
			}

			// Support pointer-to-value for nullable columns.
			// e.g. time.Time source → *time.Time target,
			//      string source    → *string target, etc.
			if f.Kind() == reflect.Ptr {
				elemType := f.Type().Elem()
				if src.Type().AssignableTo(elemType) {
					ptr := reflect.New(elemType)
					ptr.Elem().Set(src)
					f.Set(ptr)
					continue
				}
			}

			f.Set(reflect.Zero(f.Type()))
		}
	}
}

// nullSafeCopyPlan is the plan-driven equivalent of nullSafeCopy for the
// built-in mappers. It performs the same conversions in the same order,
// but consults precomputed field metadata instead of inspecting each
// field type on every row.
func nullSafeCopyPlan(fields []reflect.Value, values []any, p *scanPlan) {
	for i, f := range fields {
		if !f.CanSet() {
			continue
		}
		raw := values[i]
		if raw == nil {
			f.Set(reflect.Zero(f.Type()))
			continue
		}

		ft := p.fieldType[i]
		// Fast paths for common driver value types. Each is equivalent to
		// the assignable/convertible branches below for the same
		// source/target combination, without reflection type comparisons.
		switch v := raw.(type) {
		case string:
			if ft.Kind() == reflect.String {
				f.SetString(v)
				continue
			}
		case int64:
			if k := ft.Kind(); k >= reflect.Int && k <= reflect.Int64 {
				f.SetInt(v)
				continue
			}
			if k := ft.Kind(); k >= reflect.Uint && k <= reflect.Uint64 {
				f.SetUint(uint64(v))
				continue
			}
		case float64:
			if k := ft.Kind(); k == reflect.Float32 || k == reflect.Float64 {
				f.SetFloat(v)
				continue
			}
			if k := ft.Kind(); k >= reflect.Int && k <= reflect.Int64 {
				f.SetInt(int64(v))
				continue
			}
		case bool:
			if ft.Kind() == reflect.Bool {
				f.SetBool(v)
				continue
			}
		case []byte:
			if ft.Kind() == reflect.String {
				f.SetString(string(v))
				continue
			}
		}

		src := reflect.ValueOf(raw)
		if src.Type().AssignableTo(ft) {
			f.Set(src)
			continue
		}
		if src.Type().ConvertibleTo(ft) {
			f.Set(src.Convert(ft))
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
			f.SetString(fmt.Sprintf("%v", raw))
		case reflect.Bool:
			switch src.Kind() {
			case reflect.Bool:
				f.SetBool(src.Bool())
			default:
				f.Set(reflect.Zero(f.Type()))
			}
		default:
			if f.CanAddr() && p.isScanner[i] {
				scanner := f.Addr().Interface().(sql.Scanner)
				err := scanner.Scan(raw)
				if err != nil {
					if s, ok2 := numericString(src); ok2 {
						err = scanner.Scan(s)
					}
					if err != nil {
						if s, ok2 := jsonbText(src); ok2 {
							err = scanner.Scan(s)
						}
					}
				}
				if err != nil {
					f.Set(reflect.Zero(f.Type()))
				}
				continue
			}
			if p.isPtr[i] {
				elemType := p.ptrElem[i]
				if src.Type().AssignableTo(elemType) {
					ptr := reflect.New(elemType)
					ptr.Elem().Set(src)
					f.Set(ptr)
					continue
				}
			}
			f.Set(reflect.Zero(f.Type()))
		}
	}
}

func scanAllWithMapper[T any](rows Rows, fm FieldMapper) ([]T, error) {
	p, ok := scanPlanFor[T](fm)
	if !ok {
		// Custom mapper: keep the generic per-row path.
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
	var results []T
	values, ptrs, fields := newScanBuffers(p)
	// For non-pointer T the result is copied into results, so a single
	// target element can be reused: mapped fields are rewritten every row
	// and never-mapped fields stay at their initial zero value.
	var fixedElem reflect.Value
	if reflect.TypeFor[T]().Kind() != reflect.Ptr {
		fixedElem = newT[T]()
	}
	for rows.Next() {
		for i := range values {
			values[i] = nil
		}
		elem := fixedElem
		if !elem.IsValid() {
			elem = newT[T]()
		}
		if !fillScanFields(elem, p, fields) {
			// Matches the original scanTargets behavior for nil pointer
			// chains: no destinations are passed to Scan.
			targets, flds := scanTargets(elem, fm)
			if err := rows.Scan(targets...); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
			nullSafeCopy(flds, targets)
			results = append(results, elem.Interface().(T))
			continue
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		if p.scalar {
			nullSafeCopy(fields[:1], ptrs[:1])
		} else {
			nullSafeCopyPlan(fields, values, p)
		}
		results = append(results, elem.Interface().(T))
	}
	return results, rows.Err()
}

func scanRowWithMapper[T any](row Row, fm FieldMapper) (T, error) {
	var zero T
	p, ok := scanPlanFor[T](fm)
	if !ok {
		elem := newT[T]()
		targets, fields := scanTargets(elem, fm)
		if err := row.Scan(targets...); err != nil {
			return zero, fmt.Errorf("scan row: %w", err)
		}
		nullSafeCopy(fields, targets)
		return elem.Interface().(T), nil
	}
	values, ptrs, fields := newScanBuffers(p)
	elem := newT[T]()
	if !fillScanFields(elem, p, fields) {
		targets, flds := scanTargets(elem, fm)
		if err := row.Scan(targets...); err != nil {
			return zero, fmt.Errorf("scan row: %w", err)
		}
		nullSafeCopy(flds, targets)
		return elem.Interface().(T), nil
	}
	if err := row.Scan(ptrs...); err != nil {
		return zero, fmt.Errorf("scan row: %w", err)
	}
	if p.scalar {
		nullSafeCopy(fields[:1], ptrs[:1])
	} else {
		nullSafeCopyPlan(fields, values, p)
	}
	return elem.Interface().(T), nil
}

// newT creates a new zero value of type T and returns it as a reflect.Value.
func newT[T any]() reflect.Value {
	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		// Interface type parameters resolved to a nil reflect.Type before;
		// keep the same failure mode (reflect.New(nil) panics).
		var zero T
		return reflect.New(reflect.TypeOf(zero)).Elem()
	}
	if typ.Kind() == reflect.Ptr {
		return reflect.New(typ.Elem())
	}
	return reflect.New(typ).Elem()
}

var (
	timeType      = reflect.TypeOf(time.Time{})
	bigIntPtrType = reflect.TypeOf((*big.Int)(nil))
)

func hasFieldUncached(v any, name string) bool {
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
	if t == nil || t.Kind() != reflect.Struct {
		return false
	}
	_, ok := t.FieldByName(name)
	return ok
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
	if t == nil || t.Kind() != reflect.Struct {
		return false
	}
	// cachedFieldIndex resolves promoted fields via FieldByName and
	// caches negatives, so its answer is authoritative.
	_, ok := cachedFieldIndex(t, name)
	return ok
}

func setField(v reflect.Value, name string, val any) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	path, ok := cachedFieldIndex(v.Type(), name)
	if !ok {
		return
	}
	f := v.FieldByIndex(path)
	if f.CanSet() {
		rv := reflect.ValueOf(val)
		if rv.IsValid() && rv.Type().AssignableTo(f.Type()) {
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
	if v.Kind() != reflect.Struct {
		return
	}
	path, ok := cachedFieldIndex(v.Type(), name)
	if !ok {
		return
	}
	f := v.FieldByIndex(path)
	if f.CanSet() && f.Type() == timeType {
		f.Set(reflect.ValueOf(time.Now().UTC()))
	}
}
