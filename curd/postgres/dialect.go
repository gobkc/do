package postgres

import "strconv"

// Dialect implements curd.Dialect for PostgreSQL ($1, $2, ...).
type Dialect struct{}

func (Dialect) Placeholder(n int) string {
	return "$" + strconv.Itoa(n)
}
