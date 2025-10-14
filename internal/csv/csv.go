package csv

import (
	"bufio"
	"encoding/csv"
	"os"
)

const (
	bufferSize = 1 << 20
)

type Csv struct {
	file *os.File
	cw   *csv.Writer
}

func New(filepath string) (*Csv, error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	cw := csv.NewWriter(
		bufio.NewWriterSize(file, bufferSize),
	)

	return &Csv{
		file: file,
		cw:   cw,
	}, nil
}

func (c *Csv) Write(line []string) error {
	return c.cw.Write(line)
}

func (c *Csv) Close() error {
	c.file.Sync()
	return c.file.Close()
}

func (c *Csv) Flush() error {
	c.cw.Flush()

	if err := c.cw.Error(); err != nil {
		return err
	}

	return nil
}

func (c *Csv) Sync() error {
	return c.file.Sync()
}
