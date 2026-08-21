package csvutil

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

var FieldNames = []string{"topic", "partition", "offset", "timestamp", "kafka_key", "key_format", "value_format", "matched_values", "matched_fields", "decoded_key_json", "decoded_value_json"}

func LoadAllowedValues(path string) (map[string]struct{}, error) {
	values := make(map[string]struct{})
	if path == "" {
		return values, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open allowed-values CSV %q: %w", path, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read allowed-values CSV %q: %w", path, err)
		}
		for _, item := range record {
			if value := strings.TrimSpace(item); value != "" {
				values[value] = struct{}{}
			}
		}
	}
	return values, nil
}

func JSONSnippet(value any, limit int) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		encoded = []byte(fmt.Sprint(value))
	} else {
		encoded = pythonJSONSpacing(encoded)
	}
	if len(encoded) > limit {
		encoded = encoded[:limit]
	}
	return string(encoded)
}

func pythonJSONSpacing(input []byte) []byte {
	output := make([]byte, 0, len(input)+16)
	inString, escaped := false, false
	for _, character := range input {
		output = append(output, character)
		if inString {
			if escaped {
				escaped = false
			} else if character == '\\' {
				escaped = true
			} else if character == '"' {
				inString = false
			}
			continue
		}
		if character == '"' {
			inString = true
		} else if character == ',' || character == ':' {
			output = append(output, ' ')
		}
	}
	return output
}

type ResultWriter struct {
	file   *os.File
	writer *csv.Writer
}

func NewResultWriter(path string) (*ResultWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create output CSV %q: %w", path, err)
	}
	result := &ResultWriter{file: file, writer: csv.NewWriter(file)}
	if err := result.writer.Write(FieldNames); err != nil {
		file.Close()
		return nil, fmt.Errorf("write CSV header: %w", err)
	}
	return result, nil
}

func (w *ResultWriter) Write(row map[string]string) error {
	record := make([]string, len(FieldNames))
	for index, name := range FieldNames {
		record[index] = row[name]
	}
	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("write output CSV: %w", err)
	}
	return nil
}

func (w *ResultWriter) Close() error {
	w.writer.Flush()
	writeErr := w.writer.Error()
	closeErr := w.file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func SortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
