package filesink

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
)

type FileSink struct {
	dir         string
	prefix      string
	chunk       int
	rowsIn      int
	rotateEvery int

	f   *os.File
	bw  *bufio.Writer
	gz  *gzip.Writer
	csv *csv.Writer
}

func New(dir, prefix string, rotateEvery int) *FileSink {
	return &FileSink{
		dir:         dir,
		prefix:      prefix,
		rotateEvery: rotateEvery,
	}
}

func (s *FileSink) Close() error {
	var err error

	if s.csv != nil {
		s.csv.Flush()
		if e := s.csv.Error(); e != nil && err == nil {
			err = e
		}
	}

	if s.gz != nil {
		if e := s.gz.Close(); e != nil && err == nil {
			err = e
		}
	}

	if s.bw != nil {
		if e := s.bw.Flush(); e != nil && err == nil {
			err = e
		}
	}

	if s.f != nil {
		if e := s.f.Close(); e != nil && err == nil {
			err = e
		}
	}

	s.f, s.bw, s.gz, s.csv = nil, nil, nil, nil
	s.rowsIn = 0

	return err
}

func (s *FileSink) Write(rec []string) error {
	if s.csv == nil {
		if err := s.open(); err != nil {
			return err
		}
	}

	if err := s.csv.Write(rec); err != nil {
		return err
	}

	s.rowsIn++

	return nil
}

func (s *FileSink) RotateIfNeeded() (bool, int, error) {
	if s.rowsIn >= s.rotateEvery {
		rows := s.rowsIn

		if err := s.Close(); err != nil {
			return false, 0, err
		}

		return true, rows, nil
	}

	return false, 0, nil
}

func (s *FileSink) RowsInChunk() int {
	return s.rowsIn
}

func (s *FileSink) open() error {
	s.chunk++
	name := fmt.Sprintf("%s_%06d.csv.gz", s.prefix, s.chunk)
	path := filepath.Join(s.dir, name)

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	bw := bufio.NewWriterSize(f, 1<<20) // 1 MB буфер
	gz, err := gzip.NewWriterLevel(bw, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()
		return err
	}

	s.f, s.bw, s.gz = f, bw, gz
	s.csv = csv.NewWriter(gz)
	s.rowsIn = 0

	return nil
}
