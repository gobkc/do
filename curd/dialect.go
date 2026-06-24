package postgres

type Dialect interface {
	Placeholder(n int) string
}
