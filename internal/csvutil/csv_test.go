package csvutil

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestResultWriterWritesHeaderAndRows(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "input.csv")
	writer, err := NewResultWriter(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(map[string]string{"topic": "topic", "kafka_key": "wanted"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(contents), strings.Join(FieldNames, ",")+"\n") {
		t.Fatalf("missing expected header: %s", contents)
	}
	if !strings.Contains(string(contents), "topic,,,,wanted,") {
		t.Fatalf("missing expected row: %s", contents)
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
