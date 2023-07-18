package do

func OneOf[T any](condition bool, result1 T, result2 T) T {
	if condition {
		return result1
	}
	return result2
}
