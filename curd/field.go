package curd

import (
	"reflect"
	"strings"
	"sync"
)

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
		cols := make([]string, 0, n)
		vals := make([]any, 0, n)
		for pos := 0; pos < n; pos++ {
			// Skip auto-generated ID field when its value is zero,
			// so the database can assign a sequence value.
			if pos == p.idPos && v.Field(p.idx[pos]).IsZero() {
				continue
			}
			name := p.cols[pos]
			cols = append(cols, name)
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
