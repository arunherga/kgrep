package app

import (
	"bytes"
	"testing"
	"time"

	"kgrep/internal/core"
)

func TestRowForMessageDisplaysDecodedAvroKey(t *testing.T) {
	message := core.DecodedMessage{Topic: "topic-a", Partition: 0, Offset: 10, RawKey: []byte{0, 0, 0, 0, 1}, Key: map[string]any{"CountryCode": "CN", "CartId": "357b75b6"}, Value: map[string]any{"CartId": "357b75b6"}, KeyFormat: "avro", ValueFormat: "avro"}
	result := core.MatchResult{ValueMatches: map[string]struct{}{"357b75b6": {}}}
	row, err := RowForMessage(message, result, "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if row["kafka_key"] != `{"CartId": "357b75b6", "CountryCode": "CN"}` {
		t.Fatalf("unexpected key: %s", row["kafka_key"])
	}
}

func TestFormatDuration(t *testing.T) {
	for duration, expected := range map[time.Duration]string{123 * time.Millisecond: "123 ms", 2500 * time.Millisecond: "2.50 seconds", 65250 * time.Millisecond: "1m 5.25s"} {
		if actual := FormatDuration(duration); actual != expected {
			t.Errorf("got %q, want %q", actual, expected)
		}
	}
}

func TestFormatBadRecordIncludesLocationAndSeparateErrors(t *testing.T) {
	record := core.BadRecord{
		Topic:      "topic-a",
		Partition:  4,
		Offset:     99,
		KeyError:   "invalid key\nJSON",
		ValueError: "invalid value JSON",
	}
	actual := FormatBadRecord(record)
	expected := "BAD topic-a[4]@99 key_error=invalid key JSON value_error=invalid value JSON"
	if actual != expected {
		t.Fatalf("got %q, want %q", actual, expected)
	}
}

func TestParseConsumeOptionsSupportsRepeatableFieldsAndReadAll(t *testing.T) {
	var stderr bytes.Buffer
	options, code := parseConsumeOptions("consume", []string{"--topic", "topic-a", "--read-all", "--key-field", "id", "--key-field", "nested.id"}, &stderr)
	if code != -1 || options.topic != "topic-a" || !options.readAll || len(options.keyFields) != 2 {
		t.Fatalf("options=%#v code=%d stderr=%q", options, code, stderr.String())
	}
}
