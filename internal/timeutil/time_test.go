package timeutil

import "testing"

func TestParseMillisecondsSupportsEpochAndISO(t *testing.T) {
	for input, expected := range map[string]int64{"1000": 1_000_000, "10000000001": 10_000_000_001, "1970-01-01T00:00:01Z": 1000, "1970-01-01T00:00:01": 1000} {
		actual, err := ParseMilliseconds(input)
		if err != nil || actual == nil || *actual != expected {
			t.Errorf("ParseMilliseconds(%q)=%v,%v want %d", input, actual, err, expected)
		}
	}
}

func TestFormatMillisecondsUsesTimezone(t *testing.T) {
	value := int64(1000)
	actual, err := FormatMilliseconds(&value, "America/Los_Angeles")
	if err != nil || actual != "1969-12-31 16:00:01-0800" {
		t.Fatalf("got %q, %v", actual, err)
	}
}
