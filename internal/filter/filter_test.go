package filter

import (
	"reflect"
	"testing"

	"kgrep/internal/core"
)

func set(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func TestFindAllowedValuesRecursesThroughKeysAndValues(t *testing.T) {
	payload := map[string]any{"outer": []any{map[string]any{"id": "122128627", "active": true}}, "870000202": "present-as-key", "missing": nil}
	found, _ := FindAllowedValues(payload, set("122128627", "870000202", "True", "None"), nil)
	if !reflect.DeepEqual(found, set("122128627", "870000202", "True", "None")) {
		t.Fatalf("unexpected matches: %#v", found)
	}
}

func TestFindAllowedValuesRestrictsAndReportsFieldPaths(t *testing.T) {
	payload := map[string]any{"Manifest": map[string]any{"Pallets": []any{map[string]any{"Deliveries": map[string]any{"Order": "123"}}}}, "ignored": "456"}
	path := "Manifest.Pallets.Deliveries.Order"
	found, details := FindAllowedValues(payload, set("123", "456"), []string{path})
	if !reflect.DeepEqual(found, set("123")) || !reflect.DeepEqual(details[path], set("123")) {
		t.Fatalf("unexpected field matches: found=%#v details=%#v", found, details)
	}
}

func TestEvaluateSupportsAllMatchModes(t *testing.T) {
	message := core.DecodedMessage{RawKey: []byte("key-1"), Key: "key-1", Value: map[string]any{"id": "value-1"}}
	tests := []struct {
		mode    string
		allowed map[string]struct{}
		want    bool
	}{
		{"kafka-key", set("key-1"), true}, {"decoded-key-field", set("key-1"), true},
		{"value-field", set("value-1"), true}, {"key-or-value", set("value-1"), true},
		{"key-and-value", set("key-1"), false}, {"key-and-value", set("key-1", "value-1"), true},
	}
	for _, test := range tests {
		result, err := Evaluate(message, test.allowed, test.mode, nil, nil)
		if err != nil || result.Matched != test.want {
			t.Errorf("mode %s: matched=%v err=%v, want %v", test.mode, result.Matched, err, test.want)
		}
	}
}
