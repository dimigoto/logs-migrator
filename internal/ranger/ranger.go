package ranger

type Range struct{ From, To uint64 }

func Split(min, max, limit uint64) []Range {
	if min > max {
		return []Range{}
	}

	if min == max {
		return []Range{{min, max}}
	}

	parts := (max - min) / limit
	if parts == 0 {
		parts = 1
	}

	out := make([]Range, 0, parts)

	for cur := min; cur <= max; cur += limit {
		end := cur + limit - 1

		if end > max {
			end = max
		}

		out = append(out, Range{cur - 1, end})
	}

	return out
}
