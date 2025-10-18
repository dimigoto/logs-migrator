package util

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SafeRemove безопасно удаляет файл с проверками:
// - Путь должен быть абсолютным
// - Путь должен находиться внутри разрешенной директории (baseDir)
// - Путь не должен содержать обход директорий (..)
// - Путь не должен быть корневой или системной директорией
func SafeRemove(filePath, baseDir string) error {
	// Нормализуем пути (разрешаем символические ссылки, убираем ..)
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return fmt.Errorf("failed to resolve base directory: %w", err)
	}

	// Очищаем пути от символических ссылок
	cleanPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		// Если файл не существует, EvalSymlinks вернет ошибку
		// Проверяем существование файла
		if os.IsNotExist(err) {
			return nil // Файл уже удален, это OK
		}
		return fmt.Errorf("failed to evaluate symlinks: %w", err)
	}

	cleanBaseDir, err := filepath.EvalSymlinks(absBaseDir)
	if err != nil {
		return fmt.Errorf("failed to evaluate base dir symlinks: %w", err)
	}

	// Проверяем, что путь находится внутри baseDir
	relPath, err := filepath.Rel(cleanBaseDir, cleanPath)
	if err != nil {
		return fmt.Errorf("failed to get relative path: %w", err)
	}

	// Проверяем, что относительный путь не начинается с ".." (выход за пределы baseDir)
	if strings.HasPrefix(relPath, "..") {
		return fmt.Errorf("path %s is outside base directory %s", cleanPath, cleanBaseDir)
	}

	// Проверяем опасные пути
	if isDangerousPath(cleanPath) {
		return fmt.Errorf("refusing to remove dangerous path: %s", cleanPath)
	}

	// Проверяем, что это файл, а не директория
	info, err := os.Stat(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Файл уже удален
		}
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if info.IsDir() {
		return fmt.Errorf("refusing to remove directory: %s", cleanPath)
	}

	// Проверяем размер файла (опционально, защита от удаления больших файлов)
	// Временные CSV файлы не должны быть огромными
	const maxFileSize = 10 * 1024 * 1024 * 1024 // 10 GB
	if info.Size() > maxFileSize {
		return fmt.Errorf("file size %d exceeds maximum allowed size %d", info.Size(), maxFileSize)
	}

	// Все проверки пройдены, удаляем файл
	if err := os.Remove(cleanPath); err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	return nil
}

// isDangerousPath проверяет, является ли путь опасным для удаления
func isDangerousPath(path string) bool {
	// Список опасных путей
	dangerousPaths := []string{
		"/",
		"/bin",
		"/boot",
		"/dev",
		"/etc",
		"/home",
		"/lib",
		"/lib64",
		"/opt",
		"/proc",
		"/root",
		"/sbin",
		"/sys",
		"/usr",
		"/var",
		"/tmp",
		"/Applications",
		"/Library",
		"/System",
		"/Users",
		"/Volumes",
	}

	cleanPath := filepath.Clean(path)

	// Проверяем точное совпадение
	for _, dangerous := range dangerousPaths {
		if cleanPath == dangerous {
			return true
		}
	}

	// Проверяем, что путь не является корнем файловой системы
	if cleanPath == "/" || cleanPath == "C:\\" || cleanPath == "C:/" {
		return true
	}

	// Проверяем, что путь не слишком короткий (например, /a или /tmp)
	parts := strings.Split(strings.Trim(cleanPath, string(filepath.Separator)), string(filepath.Separator))
	if len(parts) < 2 {
		return true
	}

	return false
}
