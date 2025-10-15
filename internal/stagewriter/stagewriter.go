package stagewriter

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"logs-migrator/internal/uuidv7"
	"os"
	"path/filepath"
	"time"
)

const (
	bufferSize = 1 << 20
	dateLayout = "2006-01-02 15:04:05"
)

// StagedWriter записывает CSV, добавляя UUID, сформированные из столбца с временной меткой (timestamp),
// в начало каждой строки.
type StagedWriter struct {
	file          *os.File
	cw            *csv.Writer
	path          string
	tsColumnIndex int
	tz            *time.Location
	rowsWritten   uint64
}

// New создает экземпляр StagedWriter
func New(tmpDir, tableName string, fromID, toID uint64, tsColumnIndex int, tz *time.Location) (*StagedWriter, error) {
	path := filepath.Join(
		tmpDir,
		fmt.Sprintf("stage_%s_%d-%d_%d.csv", tableName, fromID, toID, time.Now().UnixNano()),
	)

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	cw := csv.NewWriter(bufio.NewWriterSize(file, bufferSize))

	return &StagedWriter{
		file:          file,
		cw:            cw,
		path:          path,
		tsColumnIndex: tsColumnIndex,
		tz:            tz,
		rowsWritten:   0,
	}, nil
}

// WriteRow записывает строку, добавляя в её начало UUID, сгенерированный из столбца с временной меткой
func (sw *StagedWriter) WriteRow(values []any) error {
	// Получаем TS
	tsValue := values[sw.tsColumnIndex]
	tsStr := asString(tsValue)
	if tsStr == "" {
		return fmt.Errorf("empty timestamp at index %d", sw.tsColumnIndex)
	}

	// Парсим TS
	ts, err := time.ParseInLocation(dateLayout, tsStr, sw.tz)
	if err != nil {
		return fmt.Errorf("parse timestamp: %w", err)
	}

	// Генерируем UUIDv7
	uuid, err := uuidv7.FromTime(ts)
	if err != nil {
		return fmt.Errorf("generate UUID: %w", err)
	}

	// Все собираем в слайс с UUID в первом значении
	outRec := make([]string, 0, len(values)+1)
	outRec = append(outRec, uuid)
	for _, v := range values {
		outRec = append(outRec, asString(v))
	}

	if err := sw.cw.Write(outRec); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}

	sw.rowsWritten++
	return nil
}

// Close сбрасывает буфер (flush), синхронизирует (sync) и закрывает базовый CSV-файл.
func (sw *StagedWriter) Close() error {
	sw.cw.Flush()

	if err := sw.cw.Error(); err != nil {
		return err
	}

	if err := sw.file.Sync(); err != nil {
		_ = sw.file.Close()
		return err
	}

	return sw.file.Close()
}

// Path возращает путь до файла
func (sw *StagedWriter) Path() string {
	return sw.path
}

// RowsWritten возвращает количество записанных строк
func (sw *StagedWriter) RowsWritten() uint64 {
	return sw.rowsWritten
}

// CleanupOnError удаляет файл
func (sw *StagedWriter) CleanupOnError() {
	_ = os.Remove(sw.path)
}

// asString конвертирует любые значения в string
func asString(value any) string {
	switch x := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(dateLayout)
	default:
		return fmt.Sprint(x)
	}
}
