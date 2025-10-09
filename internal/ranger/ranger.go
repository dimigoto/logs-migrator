package ranger

type Range struct{ From, To int64 }

func Split(min, max int64, parts int) []Range {
	if parts < 1 {
		parts = 1
	}

	step := (max - min + 1) / int64(parts)
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
