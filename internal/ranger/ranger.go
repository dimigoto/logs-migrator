package ranger

type Range struct{ From, To uint64 }

func Split(min, max uint64, parts uint64) []Range {
	if parts < 1 {
		parts = 1
	}

	step := (max - min + 1) / parts
	if step < 1 {
		step = 1
	}

	out := make([]Range, 0, parts)

	for cur := min; cur <= max; cur += step {
		end := cur + step - 1

		if end > max {
			end = max
		}

		out = append(out, Range{cur, end})
	}

	return out
}

func SplitByLimit(min, max, limit uint64) []Range {
	if min == max {
		return []Range{}
	}

	parts := (max - min) / limit

	out := make([]Range, 0, parts)

	for cur := min; cur <= max; cur += limit {
		end := cur + limit - 1

		if end > max {
			end = max
		}

		out = append(out, Range{cur, end})
	}

	return out
}
