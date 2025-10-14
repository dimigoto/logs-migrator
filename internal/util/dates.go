package util

import "time"

const dateLayout = "2006-01-02 15:04:05"

func parseDateInTZ(date, tz string) (time.Time, error) {
	if tz == "" {
		return time.Parse(dateLayout, date)
	}

	loc, err := time.LoadLocation(tz);
	if err != nil {
		return time.Time{}, err
	}

	return time.ParseInLocation(dateLayout, date, loc)
}
