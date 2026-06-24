package postgres

import "fmt"

type Dialect struct{}

func (Dialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}
