package stagewriter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()
	loc := time.UTC

	t.Run("creates file successfully", func(t *testing.T) {
		writer, err := New(tmpDir, "test_table", 1, 1000, 0, loc)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		defer writer.Close()

		if writer == nil {
			t.Fatal("New() returned nil writer")
		}

		if writer.path == "" {
			t.Error("Writer path is empty")
		}

		// Проверяем что файл существует
		if _, err := os.Stat(writer.path); os.IsNotExist(err) {
			t.Errorf("File was not created: %s", writer.path)
		}
	})

	t.Run("file has correct permissions", func(t *testing.T) {
		writer, err := New(tmpDir, "test_table", 1, 1000, 0, loc)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		defer writer.Close()

		info, err := os.Stat(writer.path)
		if err != nil {
			t.Fatalf("os.Stat() error: %v", err)
		}

		// Проверяем права 0644 (rw-r--r--)
		mode := info.Mode().Perm()
		expected := os.FileMode(0644)
		if mode != expected {
			t.Errorf("File permissions = %o, want %o", mode, expected)
		}
	})

	t.Run("filename contains table name and range", func(t *testing.T) {
		writer, err := New(tmpDir, "logs", 100, 200, 0, loc)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		defer writer.Close()

		filename := filepath.Base(writer.path)

		if !strings.Contains(filename, "logs") {
			t.Errorf("Filename %q does not contain table name 'logs'", filename)
		}

		if !strings.Contains(filename, "100") {
			t.Errorf("Filename %q does not contain from ID '100'", filename)
		}

		if !strings.Contains(filename, "200") {
			t.Errorf("Filename %q does not contain to ID '200'", filename)
		}
	})

	t.Run("creates file in correct directory", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 0, loc)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		defer writer.Close()

		dir := filepath.Dir(writer.path)
		if dir != tmpDir {
			t.Errorf("File created in %q, want %q", dir, tmpDir)
		}
	})

	t.Run("invalid directory returns error", func(t *testing.T) {
		invalidDir := "/nonexistent/directory/path"
		writer, err := New(invalidDir, "test", 1, 10, 0, loc)
		if err == nil {
			if writer != nil {
				writer.Close()
			}
			t.Error("New() should return error for invalid directory")
		}
	})
}

func TestWriteRow(t *testing.T) {
	tmpDir := t.TempDir()
	loc := time.UTC

	t.Run("writes row with UUID successfully", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		// Данные: id, timestamp, value
		values := []any{1, "2024-01-01 12:00:00", "test_value"}

		err = writer.WriteRow(values)
		if err != nil {
			t.Errorf("WriteRow() error: %v", err)
		}

		if writer.RowsWritten() != 1 {
			t.Errorf("RowsWritten() = %d, want 1", writer.RowsWritten())
		}
	})

	t.Run("handles multiple rows", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		rows := [][]any{
			{1, "2024-01-01 12:00:00", "value1"},
			{2, "2024-01-01 12:01:00", "value2"},
			{3, "2024-01-01 12:02:00", "value3"},
		}

		for _, row := range rows {
			if err := writer.WriteRow(row); err != nil {
				t.Fatalf("WriteRow() error: %v", err)
			}
		}

		if writer.RowsWritten() != 3 {
			t.Errorf("RowsWritten() = %d, want 3", writer.RowsWritten())
		}
	})

	t.Run("empty timestamp returns error", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		values := []any{1, "", "value"}

		err = writer.WriteRow(values)
		if err == nil {
			t.Error("WriteRow() should return error for empty timestamp")
		}

		if !strings.Contains(err.Error(), "empty timestamp") {
			t.Errorf("Error message = %q, want to contain 'empty timestamp'", err.Error())
		}
	})

	t.Run("invalid timestamp format returns error", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		values := []any{1, "invalid-timestamp", "value"}

		err = writer.WriteRow(values)
		if err == nil {
			t.Error("WriteRow() should return error for invalid timestamp")
		}

		if !strings.Contains(err.Error(), "parse timestamp") {
			t.Errorf("Error message = %q, want to contain 'parse timestamp'", err.Error())
		}
	})

	t.Run("handles different timezones", func(t *testing.T) {
		// Создаем writer с LA timezone
		laLoc, err := time.LoadLocation("America/Los_Angeles")
		if err != nil {
			t.Skip("Skipping test: timezone data not available")
		}

		writer, err := New(tmpDir, "test", 1, 10, 1, laLoc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		values := []any{1, "2024-06-15 12:00:00", "value"}

		err = writer.WriteRow(values)
		if err != nil {
			t.Errorf("WriteRow() error: %v", err)
		}
	})

	t.Run("handles nil values", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		values := []any{1, "2024-01-01 12:00:00", nil}

		err = writer.WriteRow(values)
		if err != nil {
			t.Errorf("WriteRow() should handle nil values, got error: %v", err)
		}
	})

	t.Run("handles byte slice values", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		values := []any{1, "2024-01-01 12:00:00", []byte("binary_data")}

		err = writer.WriteRow(values)
		if err != nil {
			t.Errorf("WriteRow() should handle byte slice values, got error: %v", err)
		}
	})

	t.Run("handles time.Time values", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		now := time.Now()
		values := []any{1, "2024-01-01 12:00:00", now}

		err = writer.WriteRow(values)
		if err != nil {
			t.Errorf("WriteRow() should handle time.Time values, got error: %v", err)
		}
	})
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	loc := time.UTC

	t.Run("closes file successfully", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}

		values := []any{1, "2024-01-01 12:00:00", "value"}
		if err := writer.WriteRow(values); err != nil {
			t.Fatalf("WriteRow() error: %v", err)
		}

		err = writer.Close()
		if err != nil {
			t.Errorf("Close() error: %v", err)
		}

		// Проверяем что файл существует после закрытия
		if _, err := os.Stat(writer.path); os.IsNotExist(err) {
			t.Error("File should exist after Close()")
		}
	})

	t.Run("file is readable after close", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}

		values := []any{1, "2024-01-01 12:00:00", "test_value"}
		if err := writer.WriteRow(values); err != nil {
			t.Fatalf("WriteRow() error: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}

		// Пытаемся прочитать файл
		content, err := os.ReadFile(writer.path)
		if err != nil {
			t.Errorf("Cannot read file after Close(): %v", err)
		}

		if len(content) == 0 {
			t.Error("File is empty after Close()")
		}

		// Проверяем что в содержимом есть наша строка
		contentStr := string(content)
		if !strings.Contains(contentStr, "test_value") {
			t.Error("File content does not contain written data")
		}
	})
}

func TestPath(t *testing.T) {
	tmpDir := t.TempDir()
	loc := time.UTC

	writer, err := New(tmpDir, "test", 1, 10, 0, loc)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer writer.Close()

	path := writer.Path()

	if path == "" {
		t.Error("Path() returned empty string")
	}

	if !strings.HasPrefix(path, tmpDir) {
		t.Errorf("Path() = %q, want prefix %q", path, tmpDir)
	}
}

func TestRowsWritten(t *testing.T) {
	tmpDir := t.TempDir()
	loc := time.UTC

	t.Run("initial count is zero", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		if writer.RowsWritten() != 0 {
			t.Errorf("RowsWritten() = %d, want 0", writer.RowsWritten())
		}
	})

	t.Run("count increments after each write", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		defer writer.Close()

		for i := 0; i < 5; i++ {
			values := []any{i, "2024-01-01 12:00:00", "value"}
			if err := writer.WriteRow(values); err != nil {
				t.Fatalf("WriteRow() error: %v", err)
			}

			expected := uint64(i + 1)
			if writer.RowsWritten() != expected {
				t.Errorf("After write %d: RowsWritten() = %d, want %d", i+1, writer.RowsWritten(), expected)
			}
		}
	})
}

func TestCleanupOnError(t *testing.T) {
	tmpDir := t.TempDir()
	loc := time.UTC

	t.Run("removes file on cleanup", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}

		path := writer.path

		// Проверяем что файл существует
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Fatal("File should exist before cleanup")
		}

		writer.CleanupOnError()

		// Проверяем что файл удален
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Error("File should be removed after CleanupOnError()")
		}
	})

	t.Run("cleanup is idempotent", func(t *testing.T) {
		writer, err := New(tmpDir, "test", 1, 10, 1, loc)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}

		// Вызываем несколько раз - не должно паниковать
		writer.CleanupOnError()
		writer.CleanupOnError()
		writer.CleanupOnError()
	})
}

func TestAsString(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: "",
		},
		{
			name:     "string value",
			input:    "test",
			expected: "test",
		},
		{
			name:     "byte slice",
			input:    []byte("bytes"),
			expected: "bytes",
		},
		{
			name:     "integer",
			input:    42,
			expected: "42",
		},
		{
			name:     "float",
			input:    3.14,
			expected: "3.14",
		},
		{
			name:     "boolean",
			input:    true,
			expected: "true",
		},
		{
			name:     "time.Time",
			input:    time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			expected: "2024-01-01 12:00:00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := asString(tt.input)
			if result != tt.expected {
				t.Errorf("asString(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func BenchmarkWriteRow(b *testing.B) {
	tmpDir := b.TempDir()
	loc := time.UTC

	writer, err := New(tmpDir, "bench", 1, 1000000, 1, loc)
	if err != nil {
		b.Fatalf("New() error: %v", err)
	}
	defer writer.Close()

	values := []any{1, "2024-01-01 12:00:00", "benchmark_value", 42, 3.14}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = writer.WriteRow(values)
	}
}

func BenchmarkNew(b *testing.B) {
	tmpDir := b.TempDir()
	loc := time.UTC

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer, _ := New(tmpDir, "bench", uint64(i), uint64(i+1000), 1, loc)
		if writer != nil {
			writer.Close()
			writer.CleanupOnError()
		}
	}
}
