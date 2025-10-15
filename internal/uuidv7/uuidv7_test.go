package uuidv7

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestFromTime(t *testing.T) {
	t.Run("generates valid UUID format", func(t *testing.T) {
		now := time.Now()
		uuid, err := FromTime(now)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		// Проверяем длину (32 hex символа без дефисов)
		if len(uuid) != 32 {
			t.Errorf("UUID length = %d, want 32", len(uuid))
		}

		// Проверяем что это валидный hex
		_, err = hex.DecodeString(uuid)
		if err != nil {
			t.Errorf("UUID is not valid hex: %v", err)
		}
	})

	t.Run("contains version 7", func(t *testing.T) {
		now := time.Now()
		uuid, err := FromTime(now)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		// Версия находится в 13-м символе (0-indexed: 12)
		// Для UUIDv7 это должно быть '7'
		versionChar := uuid[12]
		if versionChar != '7' {
			t.Errorf("UUID version = %c, want '7'", versionChar)
		}
	})

	t.Run("contains variant bits", func(t *testing.T) {
		now := time.Now()
		uuid, err := FromTime(now)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		// Variant находится в 17-м символе (0-indexed: 16)
		// Для RFC 4122 должен быть 8, 9, A, или B (10xxxxxx в бинарном)
		variantChar := strings.ToUpper(string(uuid[16]))
		if variantChar != "8" && variantChar != "9" && variantChar != "A" && variantChar != "B" {
			t.Errorf("UUID variant = %s, want one of [8, 9, A, B]", variantChar)
		}
	})

	t.Run("different UUIDs for different times", func(t *testing.T) {
		time1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

		uuid1, err := FromTime(time1)
		if err != nil {
			t.Fatalf("FromTime(time1) unexpected error: %v", err)
		}

		uuid2, err := FromTime(time2)
		if err != nil {
			t.Fatalf("FromTime(time2) unexpected error: %v", err)
		}

		if uuid1 == uuid2 {
			t.Error("UUIDs for different times should be different")
		}

		// Первые 12 символов (timestamp часть) должны отличаться
		if uuid1[:12] == uuid2[:12] {
			t.Error("Timestamp parts of UUIDs should be different")
		}
	})

	t.Run("UUIDs are sortable by time", func(t *testing.T) {
		earlier := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		later := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

		uuid1, err := FromTime(earlier)
		if err != nil {
			t.Fatalf("FromTime(earlier) unexpected error: %v", err)
		}

		uuid2, err := FromTime(later)
		if err != nil {
			t.Fatalf("FromTime(later) unexpected error: %v", err)
		}

		// UUID с более ранним временем должен быть лексикографически меньше
		if uuid1 >= uuid2 {
			t.Errorf("UUID from earlier time should be less than UUID from later time\nearlier: %s\nlater:   %s", uuid1, uuid2)
		}
	})

	t.Run("handles epoch time", func(t *testing.T) {
		epoch := time.Unix(0, 0)
		uuid, err := FromTime(epoch)
		if err != nil {
			t.Fatalf("FromTime(epoch) unexpected error: %v", err)
		}

		if len(uuid) != 32 {
			t.Errorf("UUID length = %d, want 32", len(uuid))
		}

		// Первые 12 символов должны быть близки к нулю для epoch
		if uuid[:12] > "000000001000" {
			t.Logf("Epoch UUID timestamp part: %s", uuid[:12])
		}
	})

	t.Run("handles far future time", func(t *testing.T) {
		farFuture := time.Date(2099, 12, 31, 23, 59, 59, 999999999, time.UTC)
		uuid, err := FromTime(farFuture)
		if err != nil {
			t.Fatalf("FromTime(farFuture) unexpected error: %v", err)
		}

		if len(uuid) != 32 {
			t.Errorf("UUID length = %d, want 32", len(uuid))
		}
	})

	t.Run("generates unique UUIDs for same time", func(t *testing.T) {
		now := time.Now()

		// Генерируем несколько UUID для одного и того же времени
		uuids := make(map[string]bool)
		count := 100

		for i := 0; i < count; i++ {
			uuid, err := FromTime(now)
			if err != nil {
				t.Fatalf("FromTime() iteration %d unexpected error: %v", i, err)
			}

			if uuids[uuid] {
				t.Errorf("Duplicate UUID generated: %s", uuid)
			}
			uuids[uuid] = true
		}

		if len(uuids) != count {
			t.Errorf("Generated %d unique UUIDs, want %d", len(uuids), count)
		}
	})

	t.Run("preserves millisecond precision", func(t *testing.T) {
		// Время с определенными миллисекундами
		specificTime := time.Date(2024, 6, 15, 12, 30, 45, 123456789, time.UTC)

		uuid1, err := FromTime(specificTime)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		// Время с другими миллисекундами
		differentMs := time.Date(2024, 6, 15, 12, 30, 45, 124456789, time.UTC)

		uuid2, err := FromTime(differentMs)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		// Timestamp части должны отличаться
		if uuid1[:12] == uuid2[:12] {
			t.Error("UUIDs with different milliseconds should have different timestamp parts")
		}
	})
}

func TestFromTimeFormat(t *testing.T) {
	t.Run("no dashes in output", func(t *testing.T) {
		now := time.Now()
		uuid, err := FromTime(now)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		if strings.Contains(uuid, "-") {
			t.Error("UUID should not contain dashes")
		}
	})

	t.Run("lowercase hex", func(t *testing.T) {
		now := time.Now()
		uuid, err := FromTime(now)
		if err != nil {
			t.Fatalf("FromTime() unexpected error: %v", err)
		}

		if uuid != strings.ToLower(uuid) {
			t.Error("UUID should be lowercase")
		}
	})
}

func BenchmarkFromTime(b *testing.B) {
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FromTime(now)
	}
}

func BenchmarkFromTimeDifferentTimes(b *testing.B) {
	times := make([]time.Time, 1000)
	for i := range times {
		times[i] = time.Now().Add(time.Duration(i) * time.Second)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FromTime(times[i%len(times)])
	}
}
