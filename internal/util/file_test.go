package util

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeRemove(t *testing.T) {
	// Создаем временную директорию для тестов
	tmpDir, err := os.MkdirTemp("", "safe-remove-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("successfully removes file in safe directory", func(t *testing.T) {
		// Создаем тестовый файл
		testFile := filepath.Join(tmpDir, "test.csv")
		if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Удаляем файл
		if err := SafeRemove(testFile, tmpDir); err != nil {
			t.Errorf("SafeRemove() failed: %v", err)
		}

		// Проверяем, что файл удален
		if _, err := os.Stat(testFile); !os.IsNotExist(err) {
			t.Error("File should be removed")
		}
	})

	t.Run("successfully removes file in subdirectory", func(t *testing.T) {
		// Создаем поддиректорию
		subDir := filepath.Join(tmpDir, "subdir")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}

		// Создаем тестовый файл в поддиректории
		testFile := filepath.Join(subDir, "test.csv")
		if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Удаляем файл
		if err := SafeRemove(testFile, tmpDir); err != nil {
			t.Errorf("SafeRemove() failed: %v", err)
		}

		// Проверяем, что файл удален
		if _, err := os.Stat(testFile); !os.IsNotExist(err) {
			t.Error("File should be removed")
		}
	})

	t.Run("returns nil if file does not exist", func(t *testing.T) {
		nonExistentFile := filepath.Join(tmpDir, "nonexistent.csv")

		if err := SafeRemove(nonExistentFile, tmpDir); err != nil {
			t.Errorf("SafeRemove() should not fail for non-existent file: %v", err)
		}
	})

	t.Run("refuses to remove file outside base directory", func(t *testing.T) {
		// Создаем файл вне tmpDir
		otherTmpDir, err := os.MkdirTemp("", "other-test-*")
		if err != nil {
			t.Fatalf("Failed to create other temp dir: %v", err)
		}
		defer os.RemoveAll(otherTmpDir)

		testFile := filepath.Join(otherTmpDir, "test.csv")
		if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Пытаемся удалить файл из другой директории
		err = SafeRemove(testFile, tmpDir)
		if err == nil {
			t.Error("SafeRemove() should fail for file outside base directory")
		}
	})

	t.Run("refuses to remove directory", func(t *testing.T) {
		// Создаем поддиректорию
		subDir := filepath.Join(tmpDir, "testdir")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}

		// Пытаемся удалить директорию
		err := SafeRemove(subDir, tmpDir)
		if err == nil {
			t.Error("SafeRemove() should fail for directory")
		}
	})

	t.Run("handles relative paths", func(t *testing.T) {
		// Создаем тестовый файл
		testFile := filepath.Join(tmpDir, "test-relative.csv")
		if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Получаем относительный путь
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatalf("Failed to get cwd: %v", err)
		}

		relPath, err := filepath.Rel(cwd, testFile)
		if err != nil {
			t.Fatalf("Failed to get relative path: %v", err)
		}

		// Удаляем файл используя относительный путь
		if err := SafeRemove(relPath, tmpDir); err != nil {
			t.Errorf("SafeRemove() failed with relative path: %v", err)
		}

		// Проверяем, что файл удален
		if _, err := os.Stat(testFile); !os.IsNotExist(err) {
			t.Error("File should be removed")
		}
	})
}

func TestIsDangerousPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		isDangerous bool
	}{
		{
			name:       "root directory",
			path:       "/",
			isDangerous: true,
		},
		{
			name:       "/etc directory",
			path:       "/etc",
			isDangerous: true,
		},
		{
			name:       "/var directory",
			path:       "/var",
			isDangerous: true,
		},
		{
			name:       "/tmp directory",
			path:       "/tmp",
			isDangerous: true,
		},
		{
			name:       "/usr directory",
			path:       "/usr",
			isDangerous: true,
		},
		{
			name:       "/home directory",
			path:       "/home",
			isDangerous: true,
		},
		{
			name:       "safe nested path",
			path:       "/var/lib/mysql-files/stage_table_1-100.csv",
			isDangerous: false,
		},
		{
			name:       "safe temp path",
			path:       "/tmp/mysql-secure/stage_table_1-100.csv",
			isDangerous: false,
		},
		{
			name:       "short path",
			path:       "/a",
			isDangerous: true,
		},
		{
			name:       "Windows C drive",
			path:       "C:\\",
			isDangerous: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDangerousPath(tt.path)
			if result != tt.isDangerous {
				t.Errorf("isDangerousPath(%q) = %v, want %v", tt.path, result, tt.isDangerous)
			}
		})
	}
}
