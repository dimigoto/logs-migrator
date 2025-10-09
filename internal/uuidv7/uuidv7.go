package uuidv7

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// FromTime returns UUIDv7 as 32-hex string (no dashes), per draft.
func FromTime(t time.Time) (string, error) {
	ms := uint64(t.UTC().UnixNano() / int64(time.Millisecond))
	var b [16]byte
	// 48-bit timestamp
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	var rnd [10]byte
	if _, err := io.ReadFull(rand.Reader, rnd[:]); err != nil {
		return "", err
	}
	// version 7 (0b0111) in high 4 bits of byte 6
	b[6] = 0x70 | (rnd[0] >> 4)
	b[7] = rnd[1]
	// variant 10xxxxxx
	b[8] = 0x80 | (rnd[2] & 0x3F)
	copy(b[9:], rnd[3:])

	return fmt.Sprintf("%08x%04x%04x%04x%012x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:]), nil
}
