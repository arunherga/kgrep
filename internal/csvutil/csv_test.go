package csvutil

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadAllowedValuesTrimsDeduplicatesAndIgnoresEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowed.csv")
	if err := os.WriteFile(path, []byte("123, 456,,123\nABC\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	actual, err := LoadAllowedValues(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]struct{}{"123": {}, "456": {}, "ABC": {}}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("got %#v", actual)
	}
}

func TestResultWriterAndQueryRoundTrip(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "input.csv")
	writer, err := NewResultWriter(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(map[string]string{"topic": "topic", "kafka_key": "wanted"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(map[string]string{"topic": "topic", "kafka_key": "other"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := Query(input, map[string]struct{}{"wanted": {}}, "kafka_key")
	if err != nil || len(rows) != 1 || rows[0]["kafka_key"] != "wanted" {
		t.Fatalf("rows=%#v err=%v", rows, err)
	}
}

func TestJSONSnippetMatchesPythonSpacingAndLimit(t *testing.T) {
	if got := JSONSnippet(map[string]any{"id": "abc", "items": []any{1, 2}}, 100); got != `{"id": "abc", "items": [1, 2]}` {
		t.Fatalf("unexpected JSON: %s", got)
	}
	if got := JSONSnippet("abcdef", 4); got != `"abc` {
		t.Fatalf("unexpected truncation: %q", got)
	}
}
