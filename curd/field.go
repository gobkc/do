package curd

import (
	"database/sql"
	"reflect"
	"strings"
	"sync"
)

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// scanPlan is a per-type, precomputed description of how a struct maps to
// scanned columns. It removes per-row type inspection (field kinds,
// sql.Scanner checks, pointer element types) from the scan hot path.
type scanPlan struct {
	scalar    bool           // T is a non-struct scalar; a single *any target
	idx       []int          // struct field indexes mapped to columns, in order
	fieldType []reflect.Type // field types parallel to idx
	isScanner []bool         // *fieldType implements sql.Scanner
	isPtr     []bool         // field type is a pointer
	ptrElem   []reflect.Type // pointer element type (nil when isPtr is false)
}

var scanPlanDefault sync.Map // map[reflect.Type]*scanPlan
var scanPlanRaw sync.Map     // map[reflect.Type]*scanPlan

// scanPlanFor returns the cached scan plan for T under the given mapper.
// Only the built-in mappers are cacheable; custom mappers may vary per
// instance, so callers keep the generic per-row path for them.
func scanPlanFor[T any](fm FieldMapper) (*scanPlan, bool) {
	var cache *sync.Map
	switch fm.(type) {
	case defaultFieldMapper:
		cache = &scanPlanDefault
	case rawFieldMapper:
		cache = &scanPlanRaw
	default:
		return nil, false
	}
	typ := reflect.TypeFor[T]()
	if v, ok := cache.Load(typ); ok {
		return v.(*scanPlan), true
	}
	t := typ
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	p := &scanPlan{}
	if t == nil || t.Kind() != reflect.Struct {
		p.scalar = true
	} else {
		p.idx = make([]int, 0, t.NumField())
		p.fieldType = make([]reflect.Type, 0, t.NumField())
		p.isScanner = make([]bool, 0, t.NumField())
		p.isPtr = make([]bool, 0, t.NumField())
		p.ptrElem = make([]reflect.Type, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if fm.ColumnName(f) == "" {
				continue
			}
			ft := f.Type
			p.idx = append(p.idx, i)
			p.fieldType = append(p.fieldType, ft)
			p.isScanner = append(p.isScanner, reflect.PtrTo(ft).Implements(scannerType))
			if ft.Kind() == reflect.Ptr {
				p.isPtr = append(p.isPtr, true)
				p.ptrElem = append(p.ptrElem, ft.Elem())
			} else {
				p.isPtr = append(p.isPtr, false)
				p.ptrElem = append(p.ptrElem, nil)
			}
		}
	}
	actual, loaded := cache.LoadOrStore(typ, p)
	if loaded {
		return actual.(*scanPlan), true
	}
	return p, true
}

// newScanBuffers allocates the reusable scan buffers for a plan.
// database/sql overwrites every destination on Scan, so the same
// []any value slots and their *any pointers are safe to reuse per row.
func newScanBuffers(p *scanPlan) (values []any, ptrs []any, fields []reflect.Value) {
	n := p.targetCount()
	values = make([]any, n)
	ptrs = make([]any, n)
	for i := range ptrs {
		ptrs[i] = &values[i]
	}
	fields = make([]reflect.Value, n)
	return
}

func (p *scanPlan) targetCount() int {
	if p.scalar {
		return 1
	}
	return len(p.idx)
}

// fillScanFields fills fields with the addressable struct fields (or the
// scalar value itself) of elem, following the plan order. It reports false
// when elem is a nil pointer chain (no scan destinations).
func fillScanFields(elem reflect.Value, p *scanPlan, fields []reflect.Value) bool {
	v := elem
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
	}
	if p.scalar {
		fields[0] = v
		return true
	}
	for i, fi := range p.idx {
		fields[i] = v.Field(fi)
	}
	return true
}

// fieldIndexCache caches struct field index paths by (type, field name).
// Missing fields are stored as a nil []int (cached negative lookup).
// Paths support embedded/promoted fields via reflect FieldByIndex.
var fieldIndexCache sync.Map // map[fieldIndexKey][]int

type fieldIndexKey struct {
	typ  reflect.Type
	name string
}

func cachedFieldIndex(t reflect.Type, name string) ([]int, bool) {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil, false
	}
	key := fieldIndexKey{typ: t, name: name}
	if v, ok := fieldIndexCache.Load(key); ok {
		if v == nil {
			return nil, false
		}
		if path, ok := v.([]int); ok {
			if path == nil {
				return nil, false
			}
			return path, true
		}
		return nil, false
	}
	f, ok := t.FieldByName(name)
	if !ok {
		fieldIndexCache.Store(key, []int(nil))
		return nil, false
	}
	path := append([]int(nil), f.Index...)
	fieldIndexCache.Store(key, path)
	return path, true
}

// columnsCache caches SELECT column lists per struct type for the default
// mapper. The cached slices are read-only; callers must not mutate them.
var columnsCache sync.Map // map[reflect.Type][]string

// columnsJoinedCache caches the comma-joined column list for SELECT
// statements, avoiding a strings.Join per query.
var columnsJoinedCache sync.Map // map[reflect.Type]string

// scanIndexCache caches struct field indexes that map to a column,
// per struct type, for the default and raw mappers.
var scanIndexCacheDefault sync.Map // map[reflect.Type][]int
var scanIndexCacheRaw sync.Map     // map[reflect.Type][]int

// rowPlanCache caches insert column plans per struct type for the default mapper.
var rowPlanCache sync.Map // map[reflect.Type]*rowPlan

type rowPlan struct {
	cols  []string // all mapped columns in field order (including id)
	idx   []int    // struct field indexes parallel to cols
	idPos int      // position of the auto-increment id field in cols, -1 if none
}

type FieldMapper interface {
	ColumnName(f reflect.StructField) string
}

type defaultFieldMapper struct{}

func (defaultFieldMapper) ColumnName(f reflect.StructField) string {
	// GORM column tag takes highest priority — it is the explicit DB column name.
	// Parsed manually (split on ';' without allocating a slice).
	if tag := f.Tag.Get("gorm"); tag != "" {
		start := 0
		for i := 0; i <= len(tag); i++ {
			if i == len(tag) || tag[i] == ';' {
				part := strings.TrimSpace(tag[start:i])
				start = i + 1
				if part == "-" {
					return ""
				}
				if after, ok := strings.CutPrefix(part, "column:"); ok {
					return strings.TrimSpace(after)
				}
			}
		}
	}

	// JSON tag is used as column name, converted to snake_case so that
	// camelCase JSON tags (e.g. "companyNameAr") map to valid PostgreSQL
	// column names (e.g. "company_name_ar") instead of being folded to
	// lowercase (e.g. "companynamear").
	if tag := f.Tag.Get("json"); tag != "" {
		if tag == "-" {
			return ""
		}
		if idx := strings.IndexByte(tag, ','); idx >= 0 {
			if name := tag[:idx]; name != "" {
				return toSnakeCase(name)
			}
		} else {
			return toSnakeCase(tag)
		}
	}

	return toSnakeCase(f.Name)
}

func toSnakeCase(s string) string {
	// Fast path: no uppercase bytes — return as-is with zero allocation.
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				prev := s[i-1]
				next := byte(0)
				if i+1 < len(s) {
					next = s[i+1]
				}
				if (prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z' && next >= 'a' && next <= 'z') {
					b.WriteByte('_')
				}
			}
			b.WriteByte(c + 32)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

func buildColumns(t reflect.Type, fm FieldMapper) []string {
	cols := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := fm.ColumnName(f)
		if name == "" {
			continue
		}
		cols = append(cols, name)
	}
	return cols
}

func columnsFromType(t reflect.Type, fm FieldMapper) []string {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	// Fast path: cache per struct type for the default mapper, whose
	// ColumnName is a pure function of the struct type.
	if _, ok := fm.(defaultFieldMapper); ok {
		if v, ok := columnsCache.Load(t); ok {
			return v.([]string)
		}
		cols := buildColumns(t, fm)
		actual, loaded := columnsCache.LoadOrStore(t, cols)
		if loaded {
			return actual.([]string)
		}
		return cols
	}
	return buildColumns(t, fm)
}

// columnsJoinedFromType returns the comma-separated column list for SELECT
// statements, cached per struct type for the default mapper. For custom
// mappers it builds the list on each call, like before.
func columnsJoinedFromType(t reflect.Type, fm FieldMapper) string {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return ""
	}
	if _, ok := fm.(defaultFieldMapper); ok {
		if v, ok := columnsJoinedCache.Load(t); ok {
			return v.(string)
		}
		s := strings.Join(buildColumns(t, fm), ",")
		actual, loaded := columnsJoinedCache.LoadOrStore(t, s)
		if loaded {
			return actual.(string)
		}
		return s
	}
	return strings.Join(buildColumns(t, fm), ",")
}

func getRowPlan(t reflect.Type) *rowPlan {
	if v, ok := rowPlanCache.Load(t); ok {
		return v.(*rowPlan)
	}
	fm := defaultFieldMapper{}
	cols := make([]string, 0, t.NumField())
	idx := make([]int, 0, t.NumField())
	idPos := -1
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := fm.ColumnName(f)
		if name == "" {
			continue
		}
		if strings.EqualFold(f.Name, "id") && idPos == -1 {
			idPos = len(cols)
		}
		cols = append(cols, name)
		idx = append(idx, i)
	}
	p := &rowPlan{cols: cols, idx: idx, idPos: idPos}
	actual, loaded := rowPlanCache.LoadOrStore(t, p)
	if loaded {
		return actual.(*rowPlan)
	}
	return p
}

func rowValues(v reflect.Value, fm FieldMapper, transforms ...FieldTransformer) ([]string, []any) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	t := v.Type()
	if t.Kind() != reflect.Struct {
		return nil, nil
	}
	// Fast path for the default mapper: reuse the cached column plan so
	// per-row work is only IsZero/Interface/transform, no tag parsing.
	if _, ok := fm.(defaultFieldMapper); ok {
		p := getRowPlan(t)
		n := len(p.cols)
		// Skip auto-generated ID field when its value is zero, so the
		// database can assign a sequence value. When the ID is present the
		// cached column slice is returned as-is (read-only by convention).
		skipID := p.idPos >= 0 && v.Field(p.idx[p.idPos]).IsZero()
		cols := p.cols
		if skipID {
			cols = make([]string, 0, n-1)
			cols = append(cols, p.cols[:p.idPos]...)
			cols = append(cols, p.cols[p.idPos+1:]...)
		}
		vals := make([]any, 0, n)
		for pos := 0; pos < n; pos++ {
			if pos == p.idPos && skipID {
				continue
			}
			name := p.cols[pos]
			val := v.Field(p.idx[pos]).Interface()
			for _, tr := range transforms {
				val = tr(name, val)
			}
			vals = append(vals, val)
		}
		return cols, vals
	}
	cols := make([]string, 0, t.NumField())
	vals := make([]any, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := fm.ColumnName(f)
		if name == "" {
			continue
		}
		// Skip auto-generated ID field when its value is zero,
		// so the database can assign a sequence value.
		if strings.EqualFold(f.Name, "id") && v.Field(i).IsZero() {
			continue
		}
		cols = append(cols, name)
		val := v.Field(i).Interface()
		for _, tr := range transforms {
			val = tr(name, val)
		}
		vals = append(vals, val)
	}
	return cols, vals
}

func cachedScanIndexes(t reflect.Type, fm FieldMapper) []int {
	var cache *sync.Map
	switch fm.(type) {
	case defaultFieldMapper:
		cache = &scanIndexCacheDefault
	case rawFieldMapper:
		cache = &scanIndexCacheRaw
	default:
		return nil
	}
	if v, ok := cache.Load(t); ok {
		return v.([]int)
	}
	idx := make([]int, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		if fm.ColumnName(t.Field(i)) == "" {
			continue
		}
		idx = append(idx, i)
	}
	actual, loaded := cache.LoadOrStore(t, idx)
	if loaded {
		return actual.([]int)
	}
	return idx
}

func scanTargets(v reflect.Value, fm FieldMapper) (targets []any, fields []reflect.Value) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		// Scalar type (int64, string, float64, etc.): scan directly into the value.
		if !v.CanAddr() {
			return nil, nil
		}
		var dest any
		targets = append(targets, &dest)
		fields = append(fields, v)
		return
	}
	t := v.Type()
	// Fast path: cached field indexes avoid per-row ColumnName tag parsing.
	// CanAddr is still checked per row since it depends on the value.
	if idx := cachedScanIndexes(t, fm); idx != nil {
		targets = make([]any, 0, len(idx))
		fields = make([]reflect.Value, 0, len(idx))
		for _, i := range idx {
			f := v.Field(i)
			if !f.CanAddr() {
				continue
			}
			var dest any
			targets = append(targets, &dest)
			fields = append(fields, f)
		}
		return targets, fields
	}
	targets = make([]any, 0, v.NumField())
	fields = make([]reflect.Value, 0, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if fm.ColumnName(t.Field(i)) == "" {
			continue
		}
		if !f.CanAddr() {
			continue
		}
		var dest any
		targets = append(targets, &dest)
		fields = append(fields, f)
	}
	return
}
