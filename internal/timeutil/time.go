package timeutil

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

func ParseMilliseconds(value string) (*int64, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	text := strings.TrimSpace(value)
	if number, err := strconv.ParseInt(text, 10, 64); err == nil {
		if number <= 10_000_000_000 {
			number *= 1000
		}
		return &number, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		// Python treats offset-free ISO timestamps as UTC.
		parsed, err = time.Parse("2006-01-02T15:04:05.999999999", text)
		if err != nil {
			return nil, fmt.Errorf("invalid time %q: expected epoch seconds/ms or ISO-8601: %w", value, err)
		}
		parsed = parsed.UTC()
	}
	result := parsed.UnixMilli()
	return &result, nil
}

func FormatMilliseconds(value *int64, timezone string) (string, error) {
	if value == nil || *value < 0 {
		return "", nil
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return "", fmt.Errorf("invalid timezone %q: %w", timezone, err)
	}
	return time.UnixMilli(*value).In(location).Format("2006-01-02 15:04:05-0700"), nil
}
