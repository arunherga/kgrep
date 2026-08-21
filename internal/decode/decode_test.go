package decode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/linkedin/goavro/v2"

	"kgrep/internal/config"
)

func TestInferFormat(t *testing.T) {
	tests := []struct {
		raw  []byte
		want string
	}{{nil, "bytes"}, {[]byte{0, 0, 0, 0, 1, 2}, "avro"}, {[]byte(`{"id":"123"}`), "json"}, {[]byte("plain"), "string"}, {[]byte{0xff, 0xfe}, "bytes"}}
	for _, test := range tests {
		if got := InferFormat(test.raw); got != test.want {
			t.Errorf("InferFormat(%q)=%q, want %q", test.raw, got, test.want)
		}
	}
}

func TestDeserializerJSONAndString(t *testing.T) {
	deserializer := New("topic", "string", "json", config.SchemaRegistrySettings{}, false, io.Discard)
	key, format, err := deserializer.DecodeKey([]byte("key"))
	if err != nil || key != "key" || format != "string" {
		t.Fatalf("key=%#v format=%s err=%v", key, format, err)
	}
	value, format, err := deserializer.DecodeValue([]byte(`{"id":123}`))
	if err != nil || format != "json" || fmt.Sprint(value.(map[string]any)["id"]) != "123" {
		t.Fatalf("value=%#v format=%s err=%v", value, format, err)
	}
}

func TestDeserializerFetchesAndCachesConfluentAvroSchema(t *testing.T) {
	const schema = `{"type":"record","name":"Message","fields":[{"name":"id","type":"string"}]}`
	requests := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if request.Header.Get("Authorization") == "" {
			t.Error("expected basic authentication")
		}
		body := io.NopCloser(strings.NewReader(fmt.Sprintf(`{"schema":%q}`, schema)))
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: body, Request: request}, nil
	})
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := codec.BinaryFromNative(nil, map[string]any{"id": "abc"})
	if err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, 5, len(payload)+5)
	binary.BigEndian.PutUint32(wire[1:], 42)
	wire = append(wire, payload...)
	deserializer := New("topic", "avro", "avro", config.SchemaRegistrySettings{URL: "https://registry.example", Username: "user", Password: "secret"}, false, io.Discard)
	deserializer.registry.http.Transport = transport
	first, _, err := deserializer.DecodeValue(wire)
	if err != nil || !reflect.DeepEqual(first, map[string]any{"id": "abc"}) {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if _, _, err := deserializer.DecodeKey(wire); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("schema fetched %d times, want once", requests)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestVerboseDiagnosticsWriteToInjectedWriter(t *testing.T) {
	var diagnostics bytes.Buffer
	deserializer := New("topic", "auto", "auto", config.SchemaRegistrySettings{}, true, &diagnostics)
	if _, _, err := deserializer.DecodeValue([]byte(`{"id":123}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diagnostics.String(), "Inferred value format as json") {
		t.Fatalf("expected diagnostic in injected writer, got: %s", diagnostics.String())
	}
}

func TestAvroRequiresConfluentHeader(t *testing.T) {
	deserializer := New("topic", "avro", "avro", config.SchemaRegistrySettings{}, false, io.Discard)
	if _, _, err := deserializer.DecodeValue([]byte("bad")); err == nil {
		t.Fatal("expected invalid wire header error")
	}
}
