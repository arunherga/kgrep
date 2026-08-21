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
	registry    *RegistryClient
}

func New(topic, keyFormat, valueFormat string, settings config.SchemaRegistrySettings, verbose bool) *Deserializer {
	return &Deserializer{Topic: topic, KeyFormat: keyFormat, ValueFormat: valueFormat, Verbose: verbose, registry: NewRegistryClient(settings)}
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
			fmt.Printf("Inferred %s format as %s\n", field, actual)
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
		if len(raw) < 5 || raw[0] != 0 {
			return nil, actual, fmt.Errorf("decode %s as Avro: invalid Confluent wire header", field)
		}
		schemaID := binary.BigEndian.Uint32(raw[1:5])
		if d.Verbose {
			if info, err := d.registry.LatestSubject(d.Topic + "-" + field); err != nil {
				return nil, actual, err
			} else {
				fmt.Printf("Schema Registry subject %s: version=%d id=%d\n", info.Subject, info.Version, info.ID)
			}
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
}

type RegistryClient struct {
	settings config.SchemaRegistrySettings
	http     *http.Client
	mu       sync.RWMutex
	codecs   map[uint32]*goavro.Codec
}

func NewRegistryClient(settings config.SchemaRegistrySettings) *RegistryClient {
	return &RegistryClient{settings: settings, http: &http.Client{Timeout: 15 * time.Second}, codecs: make(map[uint32]*goavro.Codec)}
}

func (c *RegistryClient) Codec(id uint32) (*goavro.Codec, error) {
	c.mu.RLock()
	codec := c.codecs[id]
	c.mu.RUnlock()
	if codec != nil {
		return codec, nil
	}
	var response struct {
		Schema string `json:"schema"`
	}
	if err := c.get(fmt.Sprintf("/schemas/ids/%d", id), &response); err != nil {
		return nil, err
	}
	created, err := goavro.NewCodec(response.Schema)
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
	var response struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
		ID      int    `json:"id"`
	}
	path := "/subjects/" + url.PathEscape(subject) + "/versions/latest"
	if err := c.get(path, &response); err != nil {
		return SubjectInfo{}, fmt.Errorf("could not load Schema Registry subject %q: %w", subject, err)
	}
	return SubjectInfo{Subject: response.Subject, Version: response.Version, ID: response.ID}, nil
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
