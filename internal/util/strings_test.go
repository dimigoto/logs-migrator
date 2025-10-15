package util

import "testing"

func TestIdent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "id",
			expected: "`id`",
		},
		{
			name:     "identifier with underscore",
			input:    "user_id",
			expected: "`user_id`",
		},
		{
			name:     "identifier with number",
			input:    "column1",
			expected: "`column1`",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "", // Текущее поведение: пустая строка остается пустой
		},
		{
			name:     "identifier with special chars",
			input:    "my-column",
			expected: "`my-column`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Ident(tt.input)
			if result != tt.expected {
				t.Errorf("Ident(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIdentAll(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "multiple identifiers",
			input:    []string{"id", "name", "value"},
			expected: []string{"`id`", "`name`", "`value`"},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "single identifier",
			input:    []string{"user_id"},
			expected: []string{"`user_id`"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IdentAll(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("IdentAll(%v) length = %d, want %d", tt.input, len(result), len(tt.expected))
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("IdentAll(%v)[%d] = %q, want %q", tt.input, i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		{
			name:     "zero",
			input:    0,
			expected: "0",
		},
		{
			name:     "small number",
			input:    123,
			expected: "123",
		},
		{
			name:     "thousand",
			input:    1000,
			expected: "1,000",
		},
		{
			name:     "million",
			input:    1000000,
			expected: "1,000,000",
		},
		{
			name:     "billion",
			input:    1000000000,
			expected: "1,000,000,000",
		},
		{
			name:     "irregular number",
			input:    1234567,
			expected: "1,234,567",
		},
		{
			name:     "max uint64",
			input:    18446744073709551615,
			expected: "18,446,744,073,709,551,615",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatNumber(tt.input)
			if result != tt.expected {
				t.Errorf("FormatNumber(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
