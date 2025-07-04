package utils

// Assert checks a condition and panics with an optional message if the condition is false.
func Assert(condition bool, msg ...string) {
	if !condition {
		if len(msg) > 0 {
			panic(msg[0])
		}
		panic("assertion failed")
	}
}
