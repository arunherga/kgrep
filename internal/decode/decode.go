package decode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"

	"kgrep/internal/config"
)

var Formats = []string{"auto", "json", "avro", "string", "bytes"}

func InferFormat(raw []byte) string {
	if raw == nil {
		return "bytes"
	}
	if len(raw) >= 5 && raw[0] == 0 {
		return "avro"
	}
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"') && json.Valid(raw) {
		return "json"
	}
	if bytes.Equal([]byte(strings.ToValidUTF8(string(raw), "�")), raw) {
		return "string"
	}
	return "bytes"
}

type Deserializer struct {
	Topic       string
	KeyFormat   string
	ValueFormat string
	Verbose     bool
	Diagnostics io.Writer
	registry    *RegistryClient
}

func New(topic, keyFormat, valueFormat string, settings config.SchemaRegistrySettings, verbose bool, diagnostics io.Writer) *Deserializer {
	return &Deserializer{Topic: topic, KeyFormat: keyFormat, ValueFormat: valueFormat, Verbose: verbose, Diagnostics: diagnostics, registry: NewRegistryClient(settings)}
}

func (d *Deserializer) DecodeKey(raw []byte) (any, string, error) {
	return d.decode(raw, "key", d.KeyFormat)
}

func (d *Deserializer) DecodeValue(raw []byte) (any, string, error) {
	return d.decode(raw, "value", d.ValueFormat)
}

func (d *Deserializer) decode(raw []byte, field, configured string) (any, string, error) {
	actual := configured
	if actual == "auto" {
		actual = InferFormat(raw)
		if d.Verbose {
			fmt.Fprintf(d.Diagnostics, "Inferred %s format as %s\n", field, actual)
		}
	}
	if raw == nil {
		return nil, actual, nil
	}
	switch actual {
	case "bytes":
		return raw, actual, nil
	case "string":
		return strings.ToValidUTF8(string(raw), "�"), actual, nil
	case "json":
		var result any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&result); err != nil {
			return nil, actual, fmt.Errorf("decode %s as JSON: %w", field, err)
		}
		return result, actual, nil
	case "avro":
		// This branch handles every Confluent-wire-format message (magic
		// byte + 4-byte schema ID), not just Avro specifically — Avro,
		// JSON Schema, and Protobuf all share that identical framing, so
		// the "avro" label only means the message IS wire-framed, not
		// which of the three it actually is. That requires asking the
		// registry what the schema ID's registered type is.
		if len(raw) < 5 || raw[0] != 0 {
			return nil, actual, fmt.Errorf("decode %s as Avro: invalid Confluent wire header", field)
		}
		schemaID := binary.BigEndian.Uint32(raw[1:5])
		if d.Verbose {
			if info, err := d.registry.LatestSubject(d.Topic + "-" + field); err != nil {
				fmt.Fprintf(d.Diagnostics, "Schema Registry subject lookup failed for %s: %v\n", field, err)
			} else {
				fmt.Fprintf(d.Diagnostics, "Schema Registry subject %s: version=%d id=%d\n", info.Subject, info.Version, info.ID)
			}
		}
		schemaType, err := d.registry.SchemaTypeOf(schemaID)
		if err != nil {
			return nil, actual, fmt.Errorf("decode %s: look up schema %d type: %w", field, schemaID, err)
		}
		switch schemaType {
		case "JSON":
			// Confluent's JSON Schema serializer uses the same 5-byte wire
			// header purely for schema-ID lookup; the payload after it is
			// plain JSON, so this reuses the same decode path as "json"
			// above.
			var result any
			jsonDecoder := json.NewDecoder(bytes.NewReader(raw[5:]))
			jsonDecoder.UseNumber()
			if err := jsonDecoder.Decode(&result); err != nil {
				return nil, actual, fmt.Errorf("decode %s as JSON Schema (schema %d): %w", field, schemaID, err)
			}
			return result, "json", nil
		case "PROTOBUF":
			// Decoding Protobuf requires parsing the registered .proto
			// schema into a descriptor and handling Confluent's
			// message-index prefix — not implemented. Reporting this
			// plainly is far more honest than the previous behavior:
			// silently trying to compile the .proto text as an Avro
			// schema and failing with an unrelated, confusing error.
			return nil, actual, fmt.Errorf("decode %s: schema %d is PROTOBUF, which kgrep does not support decoding yet", field, schemaID)
		}
		codec, err := d.registry.Codec(schemaID)
		if err != nil {
			return nil, actual, fmt.Errorf("decode %s as Avro: %w", field, err)
		}
		value, rest, err := codec.NativeFromBinary(raw[5:])
		if err != nil {
			return nil, actual, fmt.Errorf("decode %s as Avro with schema %d: %w", field, schemaID, err)
		}
		if len(rest) != 0 {
			return nil, actual, fmt.Errorf("decode %s as Avro with schema %d: %d trailing bytes", field, schemaID, len(rest))
		}
		return value, actual, nil
	default:
		return nil, actual, fmt.Errorf("unsupported format: %s", actual)
	}
}

type SubjectInfo struct {
	Subject string
	Version int
	ID      int
	// SchemaType is AVRO, JSON, or PROTOBUF. Older Schema Registry versions
	// that predate multi-format support omit this field entirely, in which
	// case it defaults to "AVRO" (the only type they ever served).
	SchemaType string
}

// schemaByID is what a single GET /schemas/ids/{id} call returns: the raw
// schema text plus its registered type. SchemaTypeOf and Codec both need
// data from the same response, so they share one cache/fetch instead of
// each hitting the registry independently for the same ID.
type schemaByID struct {
	schemaType string
	rawSchema  string
}

type RegistryClient struct {
	settings config.SchemaRegistrySettings
	http     *http.Client
	mu       sync.RWMutex
	schemas  map[uint32]schemaByID
	codecs   map[uint32]*goavro.Codec
	subjects map[string]SubjectInfo
}

func NewRegistryClient(settings config.SchemaRegistrySettings) *RegistryClient {
	return &RegistryClient{
		settings: settings,
		http:     &http.Client{Timeout: 15 * time.Second},
		schemas:  make(map[uint32]schemaByID),
		codecs:   make(map[uint32]*goavro.Codec),
		subjects: make(map[string]SubjectInfo),
	}
}

func (c *RegistryClient) fetchSchema(id uint32) (schemaByID, error) {
	c.mu.RLock()
	if entry, ok := c.schemas[id]; ok {
		c.mu.RUnlock()
		return entry, nil
	}
	c.mu.RUnlock()
	var response struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := c.get(fmt.Sprintf("/schemas/ids/%d", id), &response); err != nil {
		return schemaByID{}, err
	}
	schemaType := response.SchemaType
	if schemaType == "" {
		// Older Schema Registry versions that predate multi-format support
		// omit this field entirely; AVRO was the only type they ever served.
		schemaType = "AVRO"
	}
	entry := schemaByID{schemaType: schemaType, rawSchema: response.Schema}
	c.mu.Lock()
	if existing, ok := c.schemas[id]; ok {
		entry = existing
	} else {
		c.schemas[id] = entry
	}
	c.mu.Unlock()
	return entry, nil
}

// SchemaTypeOf returns the registered type (AVRO, JSON, or PROTOBUF) of the
// schema with this ID.
func (c *RegistryClient) SchemaTypeOf(id uint32) (string, error) {
	entry, err := c.fetchSchema(id)
	if err != nil {
		return "", err
	}
	return entry.schemaType, nil
}

func (c *RegistryClient) Codec(id uint32) (*goavro.Codec, error) {
	c.mu.RLock()
	codec := c.codecs[id]
	c.mu.RUnlock()
	if codec != nil {
		return codec, nil
	}
	entry, err := c.fetchSchema(id)
	if err != nil {
		return nil, err
	}
	created, err := goavro.NewCodec(entry.rawSchema)
	if err != nil {
		return nil, fmt.Errorf("compile Schema Registry schema %d: %w", id, err)
	}
	c.mu.Lock()
	if existing := c.codecs[id]; existing != nil {
		created = existing
	} else {
		c.codecs[id] = created
	}
	c.mu.Unlock()
	return created, nil
}

func (c *RegistryClient) LatestSubject(subject string) (SubjectInfo, error) {
	c.mu.RLock()
	info, ok := c.subjects[subject]
	c.mu.RUnlock()
	if ok {
		return info, nil
	}
	var response struct {
		Subject    string `json:"subject"`
		Version    int    `json:"version"`
		ID         int    `json:"id"`
		SchemaType string `json:"schemaType"`
	}
	path := "/subjects/" + url.PathEscape(subject) + "/versions/latest"
	if err := c.get(path, &response); err != nil {
		return SubjectInfo{}, fmt.Errorf("could not load Schema Registry subject %q: %w", subject, err)
	}
	schemaType := response.SchemaType
	if schemaType == "" {
		schemaType = "AVRO"
	}
	info = SubjectInfo{Subject: response.Subject, Version: response.Version, ID: response.ID, SchemaType: schemaType}
	c.mu.Lock()
	c.subjects[subject] = info
	c.mu.Unlock()
	return info, nil
}

func (c *RegistryClient) get(path string, destination any) error {
	if strings.TrimSpace(c.settings.URL) == "" {
		return fmt.Errorf("SCHEMA_REGISTRY_URL is required for Avro")
	}
	request, err := http.NewRequest(http.MethodGet, strings.TrimRight(c.settings.URL, "/")+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if c.settings.Username != "" || c.settings.Password != "" {
		request.SetBasicAuth(c.settings.Username, c.settings.Password)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("Schema Registry request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("Schema Registry returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		return fmt.Errorf("decode Schema Registry response: %w", err)
	}
	return nil
}
