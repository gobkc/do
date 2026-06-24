package postgres

import (
	"reflect"
	"strings"
)

type FieldMapper interface {
	ColumnName(f reflect.StructField) string
}

type defaultFieldMapper struct{}

func (defaultFieldMapper) ColumnName(f reflect.StructField) string {
	if tag := f.Tag.Get("json"); tag != "" {
		if tag == "-" {
			return ""
		}
		if idx := strings.IndexByte(tag, ','); idx >= 0 {
			if name := tag[:idx]; name != "" {
				return name
			}
		} else {
			return tag
		}
	}

	if tag := f.Tag.Get("gorm"); tag != "" {
		for _, part := range strings.Split(tag, ";") {
			part = strings.TrimSpace(part)
			if part == "-" {
				return ""
			}
			if after, ok := strings.CutPrefix(part, "column:"); ok {
				return strings.TrimSpace(after)
			}
		}
	}

	return toSnakeCase(f.Name)
}

func toSnakeCase(s string) string {
	var b strings.Builder
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

func columnsFromType(t reflect.Type, fm FieldMapper) []string {
	var cols []string
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

func rowValues(v reflect.Value, fm FieldMapper) ([]string, []any) {
	t := v.Type()
	var cols []string
	var vals []any
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := fm.ColumnName(f)
		if name == "" {
			continue
		}
		cols = append(cols, name)
		vals = append(vals, v.Field(i).Interface())
	}
	return cols, vals
}

func scanTargets(v reflect.Value, fm FieldMapper) (targets []any, fields []reflect.Value) {
	t := v.Type()
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
