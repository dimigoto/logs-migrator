package util

import "strings"

func SplitCols(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}

	return out
}

func Ident(s string) string {
	s = strings.TrimSpace(s)

	if s == "" || strings.Contains(s, "`") {
		return s
	}

	return "`" + s + "`"
}

func IndexOf(arr []string, want string) int {
	for i, v := range arr {
		if strings.EqualFold(v, want) {
			return i
		}
	}

	return -1
}

func IndexOfCI(arr []string, want string) int {
	want = strings.TrimSpace(strings.ToLower(want))

	for i, v := range arr {
		if strings.TrimSpace(strings.ToLower(v)) == want {
			return i
		}
	}

	return -1
}
