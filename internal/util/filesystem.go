package util

import (
	"fmt"
	"os"
	"path/filepath"
)

// SafeRemoveDir удаляет каталог, если он не является корнем или текущей директорией.
func SafeRemoveDir(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve abs path: %w", err)
	}

	// Защита от удаления корня или текущего каталога
	if abs == "/" {
		return fmt.Errorf("refused to remove root directory '/'")
	}

	cwd, _ := os.Getwd()
	if abs == cwd {
		return fmt.Errorf("refused to remove current working directory '%s'", abs)
	}

	// Проверяем, что каталог действительно существует
	info, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("stat dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", abs)
	}

	return os.RemoveAll(abs)
}
