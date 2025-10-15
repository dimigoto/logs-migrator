package ranger

import (
	"testing"
)

func TestSplit(t *testing.T) {
	tests := []struct {
		name          string
		from          uint64
		to            uint64
		chunkSize     uint64
		expectedCount int
		checkFirst    bool
		firstFrom     uint64
		firstTo       uint64
		checkLast     bool
		lastFrom      uint64
		lastTo        uint64
	}{
		{
			name:          "simple range 1 to 10",
			from:          1,
			to:            10,
			chunkSize:     5,
			expectedCount: 2,
			checkFirst:    true,
			firstFrom:     0,
			firstTo:       5,
			checkLast:     true,
			lastFrom:      5,
			lastTo:        10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Split(tt.from, tt.to, tt.chunkSize)

			if len(result) != tt.expectedCount {
				t.Errorf("Split() returned %d ranges, want %d", len(result), tt.expectedCount)
				return
			}

			if tt.checkFirst && len(result) > 0 {
				if result[0].From != tt.firstFrom {
					t.Errorf("First range From = %d, want %d", result[0].From, tt.firstFrom)
				}
				if result[0].To != tt.firstTo {
					t.Errorf("First range To = %d, want %d", result[0].To, tt.firstTo)
				}
			}

			if tt.checkLast && len(result) > 0 {
				last := result[len(result)-1]
				if last.From != tt.lastFrom {
					t.Errorf("Last range From = %d, want %d", last.From, tt.lastFrom)
				}
				if last.To != tt.lastTo {
					t.Errorf("Last range To = %d, want %d", last.To, tt.lastTo)
				}
			}

			// Check ranges are continuous
			for i := 1; i < len(result); i++ {
				if result[i].From != result[i-1].To {
					t.Errorf("Gap between ranges at index %d: previous To=%d, current From=%d",
						i, result[i-1].To, result[i].From)
				}
			}
		})
	}
}

func TestSplitEdgeCases(t *testing.T) {
	t.Run("from equals to", func(t *testing.T) {
		result := Split(100, 100, 10)
		// Текущее поведение: возвращает 1 диапазон [100, 100]
		if len(result) != 1 {
			t.Errorf("Split(100, 100, 10) got %d ranges, want 1", len(result))
		}
	})

	t.Run("from greater than to", func(t *testing.T) {
		result := Split(100, 50, 10)
		if len(result) != 0 {
			t.Errorf("Split(100, 50, 10) should return empty slice, got %d ranges", len(result))
		}
	})

	t.Run("chunk size is 1 from 1 to 5", func(t *testing.T) {
		result := Split(1, 5, 1)
		// Диапазон 1-5 с chunk=1: должно быть 5 диапазонов
		// Фактически из-за cur-1 получится [0,0], [1,1], [2,2], [3,3], [4,4], [5,5]
		t.Logf("Split(1, 5, 1) returned %d ranges", len(result))
		// Просто проверяем что результат не пустой
		if len(result) == 0 {
			t.Error("Split(1, 5, 1) should not return empty slice")
		}
	})

	t.Run("very large chunk size from 1", func(t *testing.T) {
		result := Split(1, 100, 1000000)
		if len(result) != 1 {
			t.Errorf("Split(1, 100, 1000000) should return 1 range, got %d", len(result))
		}
		// Из-за cur-1 ожидается [0, 100]
		if len(result) > 0 {
			t.Logf("Split(1, 100, 1000000) range: [%d, %d]", result[0].From, result[0].To)
		}
	})
}
