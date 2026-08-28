package decode

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/bufbuild/protocompile"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

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

func TestVerboseSubjectLookupFailureDoesNotFailDecode(t *testing.T) {
	const schema = `{"type":"record","name":"Message","fields":[{"name":"id","type":"string"}]}`
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.Contains(request.URL.Path, "/subjects/") {
			return &http.Response{StatusCode: http.StatusNotFound, Status: "404 Not Found", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
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
	binary.BigEndian.PutUint32(wire[1:], 7)
	wire = append(wire, payload...)
	var diagnostics bytes.Buffer
	deserializer := New("topic", "avro", "avro", config.SchemaRegistrySettings{URL: "https://registry.example"}, true, &diagnostics)
	deserializer.registry.http.Transport = transport
	value, _, err := deserializer.DecodeValue(wire)
	if err != nil || !reflect.DeepEqual(value, map[string]any{"id": "abc"}) {
		t.Fatalf("expected successful decode despite subject-lookup failure: value=%#v err=%v", value, err)
	}
	if !strings.Contains(diagnostics.String(), "Schema Registry subject lookup failed") {
		t.Fatalf("expected diagnostic about the failed lookup, got: %s", diagnostics.String())
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

func TestDeserializerDecodesJSONSchemaMessagesAsPlainJSON(t *testing.T) {
	// Avro, JSON Schema, and Protobuf all share the identical Confluent wire
	// header (magic byte + schema ID) — this confirms a JSON-Schema-typed
	// schema ID is actually decoded as JSON rather than being misclassified
	// as Avro and having its .proto/JSON-Schema text fail to compile as an
	// Avro schema (the real bug this was regression-tested against).
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := io.NopCloser(strings.NewReader(`{"schema":"{\"type\":\"object\"}","schemaType":"JSON"}`))
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: body, Request: request}, nil
	})
	wire := append([]byte{0, 0, 0, 0, 99}, []byte(`{"id":"abc"}`)...)
	deserializer := New("topic", "avro", "avro", config.SchemaRegistrySettings{URL: "https://registry.example"}, false, io.Discard)
	deserializer.registry.http.Transport = transport
	value, format, err := deserializer.DecodeValue(wire)
	if err != nil || format != "json" || !reflect.DeepEqual(value, map[string]any{"id": "abc"}) {
		t.Fatalf("value=%#v format=%s err=%v", value, format, err)
	}
}

func compileTestProto(t *testing.T, source string) protoreflect.FileDescriptor {
	t.Helper()
	compiled, err := (&protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(map[string]string{"test.proto": source}),
		}),
	}).Compile(context.Background(), "test.proto")
	if err != nil {
		t.Fatalf("compile test schema: %v", err)
	}
	return compiled[0]
}

func TestDeserializerDecodesProtobufMessagesDynamically(t *testing.T) {
	const schema = `syntax = "proto3"; message Order { string id = 1; double amount = 2; }`
	file := compileTestProto(t, schema)
	descriptor := file.Messages().Get(0)
	message := dynamicpb.NewMessage(descriptor)
	message.Set(descriptor.Fields().ByName("id"), protoreflect.ValueOfString("abc"))
	message.Set(descriptor.Fields().ByName("amount"), protoreflect.ValueOfFloat64(42.5))
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := io.NopCloser(strings.NewReader(fmt.Sprintf(`{"schema":%q,"schemaType":"PROTOBUF"}`, schema)))
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: body, Request: request}, nil
	})

	// 5-byte Confluent header, then the message-index shortcut byte for the
	// (overwhelmingly common) single-top-level-message case, then the raw
	// protobuf bytes.
	wire := make([]byte, 5, 5+1+len(payload))
	binary.BigEndian.PutUint32(wire[1:], 11)
	wire = append(wire, 0)
	wire = append(wire, payload...)

	deserializer := New("topic", "avro", "avro", config.SchemaRegistrySettings{URL: "https://registry.example"}, false, io.Discard)
	deserializer.registry.http.Transport = transport
	value, format, err := deserializer.DecodeValue(wire)
	if err != nil || format != "protobuf" {
		t.Fatalf("value=%#v format=%s err=%v", value, format, err)
	}
	decoded, ok := value.(map[string]any)
	if !ok || decoded["id"] != "abc" {
		t.Fatalf("expected decoded field id=abc, got %#v", value)
	}
}

func TestDecodeMessageIndexesShortcutForFirstTopLevelMessage(t *testing.T) {
	indexes, rest, err := decodeMessageIndexes([]byte{0, 0xAA, 0xBB})
	if err != nil || !reflect.DeepEqual(indexes, []int{0}) || !bytes.Equal(rest, []byte{0xAA, 0xBB}) {
		t.Fatalf("indexes=%v rest=%v err=%v", indexes, rest, err)
	}
}

func TestDecodeMessageIndexesGeneralForm(t *testing.T) {
	// count=2, indexes=[3, 1], then 2 bytes of "payload" that must be left
	// untouched in rest.
	indexes, rest, err := decodeMessageIndexes([]byte{2, 3, 1, 0xAA, 0xBB})
	if err != nil || !reflect.DeepEqual(indexes, []int{3, 1}) || !bytes.Equal(rest, []byte{0xAA, 0xBB}) {
		t.Fatalf("indexes=%v rest=%v err=%v", indexes, rest, err)
	}
}

func TestDeserializerDecodesNestedProtobufMessageViaMessageIndex(t *testing.T) {
	// A schema with two top-level messages, the second having a nested
	// type -- exercises the general (non-shortcut) message-index path,
	// which the single-message shortcut test above can't reach.
	const schema = `syntax = "proto3";
		message First { string ignored = 1; }
		message Second {
			message Inner { string label = 1; }
			Inner inner = 1;
		}`
	file := compileTestProto(t, schema)
	second := file.Messages().Get(1)
	inner := second.Messages().Get(0)
	message := dynamicpb.NewMessage(inner)
	message.Set(inner.Fields().ByName("label"), protoreflect.ValueOfString("nested-value"))
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := io.NopCloser(strings.NewReader(fmt.Sprintf(`{"schema":%q,"schemaType":"PROTOBUF"}`, schema)))
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: body, Request: request}, nil
	})

	// message index [1, 0]: top-level message 1 (Second), nested message 0
	// (Inner) -- encoded in general form as count=2, then the two indexes.
	wire := make([]byte, 5, 5+3+len(payload))
	binary.BigEndian.PutUint32(wire[1:], 22)
	wire = append(wire, 2, 1, 0)
	wire = append(wire, payload...)

	deserializer := New("topic", "avro", "avro", config.SchemaRegistrySettings{URL: "https://registry.example"}, false, io.Discard)
	deserializer.registry.http.Transport = transport
	value, _, err := deserializer.DecodeValue(wire)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := value.(map[string]any)
	if !ok || decoded["label"] != "nested-value" {
		t.Fatalf("expected decoded label=nested-value, got %#v", value)
	}
}
