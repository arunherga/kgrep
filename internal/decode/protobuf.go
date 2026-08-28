package decode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protobufDecoder compiles Confluent-registered Protobuf schemas (including
// their cross-schema references) into message descriptors on demand, and
// decodes Confluent-framed Protobuf payloads into generic values using
// dynamic (reflection-based) message decoding — no generated Go types
// required, matching how Avro decoding needs no generated types either.
// google.golang.org/protobuf can decode binary protobuf against any
// descriptor, but it can't parse .proto *source text* (only pre-compiled
// descriptors) — that part is what protocompile provides.
type protobufDecoder struct {
	registry *RegistryClient
	mu       sync.Mutex
	files    map[uint32]protoreflect.FileDescriptor // compiled schema, by schema ID
}

func newProtobufDecoder(registry *RegistryClient) *protobufDecoder {
	return &protobufDecoder{registry: registry, files: make(map[uint32]protoreflect.FileDescriptor)}
}

// decode parses raw's Confluent message-index prefix, resolves the target
// message type within the schema, and dynamically decodes the remaining
// protobuf bytes into a generic value — converted through protojson so
// nested messages/enums/maps/repeated fields come out the same shape
// encoding/json would already produce for the "json" format, keeping CSV
// and terminal output consistent across formats.
func (d *protobufDecoder) decode(schemaID uint32, rawSchema string, references []schemaReference, raw []byte) (any, error) {
	indexes, body, err := decodeMessageIndexes(raw)
	if err != nil {
		return nil, fmt.Errorf("decode message index: %w", err)
	}
	file, err := d.fileDescriptor(schemaID, rawSchema, references)
	if err != nil {
		return nil, err
	}
	descriptor, err := messageAtIndex(file, indexes)
	if err != nil {
		return nil, err
	}
	message := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(body, message); err != nil {
		return nil, fmt.Errorf("unmarshal protobuf message %q: %w", descriptor.FullName(), err)
	}
	// EmitUnpopulated so a message decodes with every declared field present
	// (proto3 doesn't distinguish "unset" from "zero value" on the wire),
	// matching what a user would expect from inspecting the schema rather
	// than only seeing whichever fields happened to be non-default.
	jsonBytes, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("convert decoded protobuf to JSON: %w", err)
	}
	var result any
	resultDecoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	resultDecoder.UseNumber()
	if err := resultDecoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("parse converted protobuf JSON: %w", err)
	}
	return result, nil
}

// fileDescriptor compiles a schema's .proto source text (plus, recursively,
// every schema it references) into a FileDescriptor, caching the result by
// schema ID so a long-running scan only compiles each distinct schema once.
func (d *protobufDecoder) fileDescriptor(schemaID uint32, rawSchema string, references []schemaReference) (protoreflect.FileDescriptor, error) {
	d.mu.Lock()
	if file, ok := d.files[schemaID]; ok {
		d.mu.Unlock()
		return file, nil
	}
	d.mu.Unlock()

	const mainFile = "__kgrep_main__.proto"
	sources := map[string]string{mainFile: rawSchema}
	if err := d.collectReferences(references, sources); err != nil {
		return nil, err
	}

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(sources),
		}),
	}
	compiled, err := compiler.Compile(context.Background(), mainFile)
	if err != nil {
		return nil, fmt.Errorf("compile Protobuf schema %d: %w", schemaID, err)
	}
	file := protoreflect.FileDescriptor(compiled[0])

	d.mu.Lock()
	if existing, ok := d.files[schemaID]; ok {
		file = existing
	} else {
		d.files[schemaID] = file
	}
	d.mu.Unlock()
	return file, nil
}

// collectReferences recursively resolves a schema's "references" list (each
// naming another registered schema by subject+version) into source files
// protocompile can import by name, so a schema split across multiple
// registered .proto files compiles as a unit instead of failing on an
// unresolved import.
func (d *protobufDecoder) collectReferences(references []schemaReference, sources map[string]string) error {
	for _, reference := range references {
		if _, ok := sources[reference.Name]; ok {
			continue
		}
		resolved, err := d.registry.schemaBySubjectVersion(reference.Subject, reference.Version)
		if err != nil {
			return fmt.Errorf("resolve Protobuf reference %q (subject %q version %d): %w", reference.Name, reference.Subject, reference.Version, err)
		}
		sources[reference.Name] = resolved.rawSchema
		if err := d.collectReferences(resolved.references, sources); err != nil {
			return err
		}
	}
	return nil
}

// decodeMessageIndexes parses Confluent's message-index prefix: the path of
// nested-message indexes (from the file's top-level message list, down
// through any nested types) identifying which message type in a
// (potentially multi-message) .proto file this payload was encoded with.
// As a size optimization, index [0] — the overwhelmingly common case of a
// single top-level message — is encoded as a single zero byte rather than
// the general [count=1, index=0] form.
func decodeMessageIndexes(raw []byte) (indexes []int, rest []byte, err error) {
	count, n := protowire.ConsumeVarint(raw)
	if n < 0 {
		return nil, nil, fmt.Errorf("invalid message-index count varint")
	}
	rest = raw[n:]
	if count == 0 {
		return []int{0}, rest, nil
	}
	indexes = make([]int, count)
	for i := range indexes {
		value, n := protowire.ConsumeVarint(rest)
		if n < 0 {
			return nil, nil, fmt.Errorf("invalid message-index entry varint")
		}
		indexes[i] = int(value)
		rest = rest[n:]
	}
	return indexes, rest, nil
}

// messageAtIndex walks a message-index path down from a file's top-level
// message list through successive levels of nested message types.
func messageAtIndex(file protoreflect.FileDescriptor, indexes []int) (protoreflect.MessageDescriptor, error) {
	if len(indexes) == 0 {
		return nil, fmt.Errorf("empty message index")
	}
	messages := file.Messages()
	if indexes[0] < 0 || indexes[0] >= messages.Len() {
		return nil, fmt.Errorf("message index %d out of range (file has %d top-level message(s))", indexes[0], messages.Len())
	}
	descriptor := messages.Get(indexes[0])
	for _, index := range indexes[1:] {
		nested := descriptor.Messages()
		if index < 0 || index >= nested.Len() {
			return nil, fmt.Errorf("nested message index %d out of range within %q (%d nested message(s))", index, descriptor.FullName(), nested.Len())
		}
		descriptor = nested.Get(index)
	}
	return descriptor, nil
}
