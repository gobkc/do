package curd

import (
	"fmt"
	"strings"
)

// Op represents a SQL comparison operator.
type Op string

const (
	OpEq          Op = "="
	OpNe          Op = "!="
	OpGt          Op = ">"
	OpGte         Op = ">="
	OpLt          Op = "<"
	OpLte         Op = "<="
	OpIn          Op = "IN"
	OpNotIn       Op = "NOT IN"
	OpLike        Op = "LIKE"
	OpILike       Op = "ILIKE"
	OpIsNull      Op = "IS NULL"
	OpIsNotNull   Op = "IS NOT NULL"
	OpBetween     Op = "BETWEEN"
	OpJSONGet     Op = "->>"   // field->>'key' = value
	OpJSONContains Op = "@>"    // field @> value
)

// Predicate is a function that builds a WHERE clause fragment.
// It receives an ArgBuilder to generate parameterized placeholders
// and returns the SQL fragment for its condition.
//
// A nil Predicate means "no condition" (equivalent to an empty WHERE).
//
// Usage:
//
//	cond := curd.And(
//	    curd.Eq("status", "active"),
//	    curd.Or(
//	        curd.ILike("name", "%test%"),
//	        curd.ILike("label", "%test%"),
//	    ),
//	    curd.In("org_id", ids...),
//	)
//	results, err := c.FindAll(ctx, cond, "id ASC", 10, 0)
type Predicate func(b *ArgBuilder) string

// ArgBuilder collects parameter values and generates dialect-appropriate
// placeholders during Predicate evaluation.
type ArgBuilder struct {
	args []any
	idx  int
	d    Dialect
}

// newArgBuilder creates an ArgBuilder for the given dialect, starting at
// the specified placeholder index.
func newArgBuilder(d Dialect, startIdx int) *ArgBuilder {
	return &ArgBuilder{idx: startIdx, d: d}
}

// Arg adds val as a parameter and returns its placeholder string (e.g. "$3").
func (b *ArgBuilder) Arg(val any) string {
	ph := b.d.Placeholder(b.idx)
	b.idx++
	b.args = append(b.args, val)
	return ph
}

// Args adds multiple values as parameters and returns their comma-separated
// placeholder strings (e.g. "$1, $2, $3").
func (b *ArgBuilder) Args(vals ...any) string {
	phs := make([]string, len(vals))
	for i, v := range vals {
		phs[i] = b.Arg(v)
	}
	return strings.Join(phs, ", ")
}

// ArgsSlice returns the collected argument values.
func (b *ArgBuilder) ArgsSlice() []any {
	return b.args
}

// --- Constructor functions ---

// Eq returns a Predicate for field = value.
func Eq(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s = %s", field, b.Arg(value))
	}
}

// Ne returns a Predicate for field != value.
func Ne(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s != %s", field, b.Arg(value))
	}
}

// Gt returns a Predicate for field > value.
func Gt(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s > %s", field, b.Arg(value))
	}
}

// Gte returns a Predicate for field >= value.
func Gte(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s >= %s", field, b.Arg(value))
	}
}

// Lt returns a Predicate for field < value.
func Lt(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s < %s", field, b.Arg(value))
	}
}

// Lte returns a Predicate for field <= value.
func Lte(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s <= %s", field, b.Arg(value))
	}
}

// In returns a Predicate for field IN (values...).
// If no values are provided, the predicate evaluates to FALSE.
func In(field string, values ...any) Predicate {
	return func(b *ArgBuilder) string {
		if len(values) == 0 {
			return "FALSE"
		}
		return fmt.Sprintf("%s IN (%s)", field, b.Args(values...))
	}
}

// NotIn returns a Predicate for field NOT IN (values...).
// If no values are provided, the predicate evaluates to TRUE (NOT IN empty is trivially true).
func NotIn(field string, values ...any) Predicate {
	return func(b *ArgBuilder) string {
		if len(values) == 0 {
			return "TRUE"
		}
		return fmt.Sprintf("%s NOT IN (%s)", field, b.Args(values...))
	}
}

// Like returns a Predicate for field LIKE pattern.
func Like(field string, pattern any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s LIKE %s", field, b.Arg(pattern))
	}
}

// ILike returns a Predicate for field ILIKE pattern (PostgreSQL).
func ILike(field string, pattern any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s ILIKE %s", field, b.Arg(pattern))
	}
}

// IsNull returns a Predicate for field IS NULL.
func IsNull(field string) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s IS NULL", field)
	}
}

// IsNotNull returns a Predicate for field IS NOT NULL.
func IsNotNull(field string) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s IS NOT NULL", field)
	}
}

// Between returns a Predicate for field BETWEEN lo AND hi.
func Between(field string, lo, hi any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s BETWEEN %s AND %s", field, b.Arg(lo), b.Arg(hi))
	}
}

// JSONField returns a SQL expression for field->>'key'.
// Use it as the field name in other conditions:
//
//	curd.Eq(curd.JSONField("payload", "uuid"), someUUID)
//	// generates: payload->>'uuid' = $1
func JSONField(field, key string) string {
	return fmt.Sprintf("%s->>'%s'", field, key)
}

// JSONContains returns a Predicate for field @> value (PostgreSQL JSONB contains).
func JSONContains(field string, value any) Predicate {
	return func(b *ArgBuilder) string {
		return fmt.Sprintf("%s @> %s", field, b.Arg(value))
	}
}

// --- Combinator functions ---

// And returns a Predicate that AND-s its sub-predicates together.
// Nil sub-predicates and those that evaluate to empty string are skipped.
// If no sub-predicates remain after filtering, returns empty string (no condition).
func And(preds ...Predicate) Predicate {
	return func(b *ArgBuilder) string {
		var parts []string
		for _, p := range preds {
			if p == nil {
				continue
			}
			part := p(b)
			if part == "" {
				continue
			}
			parts = append(parts, part)
		}
		if len(parts) == 0 {
			return ""
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	}
}

// Or returns a Predicate that OR-s its sub-predicates together.
// Nil sub-predicates and those that evaluate to empty string are skipped.
// If no sub-predicates remain after filtering, returns empty string.
func Or(preds ...Predicate) Predicate {
	return func(b *ArgBuilder) string {
		var parts []string
		for _, p := range preds {
			if p == nil {
				continue
			}
			part := p(b)
			if part == "" {
				continue
			}
			parts = append(parts, part)
		}
		if len(parts) == 0 {
			return ""
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	}
}

// Not returns a Predicate that negates its sub-predicate.
func Not(pred Predicate) Predicate {
	return func(b *ArgBuilder) string {
		if pred == nil {
			return ""
		}
		part := pred(b)
		if part == "" {
			return ""
		}
		return "NOT (" + part + ")"
	}
}

// MapWhere converts a legacy map[string]any where clause into a Predicate.
// This provides backward compatibility for callers migrating from the old API.
//
// Map semantics:
//   - nil value → IS NULL
//   - []any value → = ANY($1)  (PostgreSQL array containment)
//   - any other value → = $1  (simple equality)
//
// All conditions are AND-ed together.
func MapWhere(m map[string]any) Predicate {
	if len(m) == 0 {
		return nil
	}
	return func(b *ArgBuilder) string {
		var conds []string
		for col, val := range m {
			if val == nil {
				conds = append(conds, fmt.Sprintf("%s IS NULL", col))
			} else if sl, ok := val.([]any); ok {
				conds = append(conds, fmt.Sprintf("%s = ANY(%s)", col, b.Arg(sl)))
			} else {
				conds = append(conds, fmt.Sprintf("%s = %s", col, b.Arg(val)))
			}
		}
		return strings.Join(conds, " AND ")
	}
}

// buildPredicate evaluates a Predicate and returns the WHERE clause SQL
// fragment and collected arguments. Returns ("", nil) if pred is nil.
func buildPredicate(pred Predicate, d Dialect) (clause string, args []any) {
	if pred == nil {
		return "", nil
	}
	b := newArgBuilder(d, 1)
	clause = pred(b)
	if clause == "" {
		return "", nil
	}
	return clause, b.ArgsSlice()
}
