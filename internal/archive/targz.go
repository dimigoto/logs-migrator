package archive

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Entry struct {
	Name string
	Size int64
	R    io.Reader // ограниченный ридер на entry (не читать после Size)
}

// IterateTarGz открывает tar.gz и вызывает cb для каждого файла
func IterateTarGz(path string, cb func(Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}

		if hdr.FileInfo().IsDir() {
			continue
		}

		name := hdr.Name
		ln := strings.ToLower(name)

		if !(strings.HasSuffix(ln, ".csv") || strings.HasSuffix(ln, ".csv.gz")) {
			continue
		}

		r := io.LimitReader(tr, hdr.Size)

		if err = cb(Entry{Name: name, Size: hdr.Size, R: r}); err != nil {
			return err
		}
	}
}

// TarGzDir создает tar.gz архив из каталога srcDir в файл dstPath (например "./export.tar.gz")
func TarGzDir(srcDir, dstPath string) error {
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	gz := gzip.NewWriter(out)
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		hdr, err := tar.FileInfoHeader(info, rel)
		if err != nil {
			return err
		}
		hdr.Name = rel

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(tw, f)

		return err
	})

	if err != nil {
		return err
	}

	return tw.Close()
}
