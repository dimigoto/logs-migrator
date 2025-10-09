package util

import (
	"strconv"
)

func FormatNumber(n uint64) string {
	s := strconv.FormatUint(n, 10)

	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}

	return s
}
