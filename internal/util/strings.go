package util

import "strings"

func Ident(s string) string {
	s = strings.TrimSpace(s)

	if s == "" || strings.Contains(s, "`") {
		return s
	}

	return "`" + s + "`"
}

func IdentAll(s []string) []string {
	result := make([]string, 0)

	for _, v := range s {
		result = append(result, Ident(v))
	}

	return result
}
