package curd

type Dialect interface {
	Placeholder(n int) string
}
