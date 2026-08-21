package filter

import (
	"fmt"
	"strings"

	"kgrep/internal/core"
)

var MatchModes = []string{"kafka-key", "decoded-key-field", "value-field", "key-or-value", "key-and-value"}

func RawKeyToText(raw []byte, decoded any) string {
	if raw == nil {
		return ""
	}
	if value, ok := decoded.(string); ok {
		return value
	}
	return strings.ToValidUTF8(string(raw), "�")
}

func ExtractPath(payload any, path string) []any {
	current := []any{payload}
	for _, part := range strings.Split(path, ".") {
		next := make([]any, 0)
		for _, item := range current {
			switch value := item.(type) {
			case map[string]any:
				if child, ok := value[part]; ok {
					next = append(next, child)
				}
			case []any:
				for _, element := range value {
					if object, ok := element.(map[string]any); ok {
						if child, exists := object[part]; exists {
							next = append(next, child)
						}
					}
				}
			}
		}
		current = next
		if len(current) == 0 {
			break
		}
	}
	return current
}

func FindAllowedValues(payload any, allowed map[string]struct{}, paths []string) (map[string]struct{}, map[string]map[string]struct{}) {
	found := make(map[string]struct{})
	details := make(map[string]map[string]struct{})
	if len(allowed) == 0 {
		return found, details
	}
	if len(paths) > 0 {
		for _, path := range paths {
			pathFound := make(map[string]struct{})
			for _, value := range ExtractPath(payload, path) {
				collect(value, allowed, pathFound)
			}
			if len(pathFound) > 0 {
				details[path] = pathFound
				merge(found, pathFound)
			}
		}
		return found, details
	}
	collect(payload, allowed, found)
	return found, details
}

func collect(payload any, allowed, found map[string]struct{}) {
	switch value := payload.(type) {
	case map[string]any:
		for key, child := range value {
			if _, ok := allowed[key]; ok {
				found[key] = struct{}{}
			}
			collect(child, allowed, found)
		}
	case []any:
		for _, child := range value {
			collect(child, allowed, found)
		}
	default:
		text := scalarText(value)
		if _, ok := allowed[text]; ok {
			found[text] = struct{}{}
		}
	}
}

func scalarText(value any) string {
	switch typed := value.(type) {
	case nil:
		return "None"
	case bool:
		if typed {
			return "True"
		}
		return "False"
	default:
		return fmt.Sprint(value)
	}
}

func Evaluate(message core.DecodedMessage, allowed map[string]struct{}, mode string, keyFields, valueFields []string) (core.MatchResult, error) {
	rawKey := RawKeyToText(message.RawKey, message.Key)
	rawMatches := make(map[string]struct{})
	if rawKey != "" {
		if _, ok := allowed[rawKey]; ok {
			rawMatches[rawKey] = struct{}{}
		}
	}
	keyMatches, keyDetails := FindAllowedValues(message.Key, allowed, keyFields)
	valueMatches, valueDetails := FindAllowedValues(message.Value, allowed, valueFields)
	keySide := len(rawMatches) > 0 || len(keyMatches) > 0
	valueSide := len(valueMatches) > 0

	var matched bool
	switch mode {
	case "kafka-key":
		matched = len(rawMatches) > 0
	case "decoded-key-field":
		matched = len(keyMatches) > 0
	case "value-field":
		matched = valueSide
	case "key-or-value":
		matched = keySide || valueSide
	case "key-and-value":
		matched = keySide && valueSide
	default:
		return core.MatchResult{}, fmt.Errorf("unsupported match mode: %s", mode)
	}

	details := make(map[string]map[string]struct{})
	if len(rawMatches) > 0 {
		details["kafka-key"] = clone(rawMatches)
	}
	if len(keyFields) > 0 {
		for path, values := range keyDetails {
			details["key."+path] = values
		}
	} else if len(keyMatches) > 0 {
		details["decoded-key"] = clone(keyMatches)
	}
	if len(valueFields) > 0 {
		for path, values := range valueDetails {
			details["value."+path] = values
		}
	} else if len(valueMatches) > 0 {
		details["value"] = clone(valueMatches)
	}

	return core.MatchResult{Matched: matched, KafkaKeyMatches: rawMatches, DecodedKeyMatches: keyMatches, ValueMatches: valueMatches, MatchedFields: details}, nil
}

func merge(target, source map[string]struct{}) {
	for value := range source {
		target[value] = struct{}{}
	}
}

func clone(source map[string]struct{}) map[string]struct{} {
	target := make(map[string]struct{}, len(source))
	merge(target, source)
	return target
}
