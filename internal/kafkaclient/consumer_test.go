package kafkaclient

import (
	"io"
	"testing"

	"github.com/segmentio/kafka-go"

	"kgrep/internal/config"
	"kgrep/internal/decode"
)

func TestNewDialerMapsSecuritySettings(t *testing.T) {
	settings := config.KafkaSettings{SecurityProtocol: "SASL_SSL", SASLMechanism: "PLAIN", Username: "user", Password: "secret"}
	dialer, err := newDialer(settings)
	if err != nil {
		t.Fatal(err)
	}
	if dialer.TLS == nil || dialer.SASLMechanism == nil {
		t.Fatalf("TLS or SASL was not configured: %#v", dialer)
	}
}

func TestDecodeMessageClassifiesValidRecordAsGood(t *testing.T) {
	consumer := &Consumer{deserializer: decode.New("topic-a", "json", "json", config.SchemaRegistrySettings{}, false, io.Discard)}
	message := kafka.Message{Topic: "topic-a", Partition: 2, Offset: 17, Key: []byte(`{"id":"key-1"}`), Value: []byte(`{"id":"value-1"}`)}

	decoded, bad := consumer.decodeMessage(message, nil)

	if bad != nil {
		t.Fatalf("unexpected bad record: %#v", bad)
	}
	if decoded.Partition != 2 || decoded.Offset != 17 || decoded.KeyFormat != "json" || decoded.ValueFormat != "json" {
		t.Fatalf("unexpected decoded record: %#v", decoded)
	}
}

func TestDecodeMessageReportsKeyAndValueFailuresAtLocation(t *testing.T) {
	consumer := &Consumer{deserializer: decode.New("topic-a", "json", "json", config.SchemaRegistrySettings{}, false, io.Discard)}
	timestamp := int64(1234)
	message := kafka.Message{Topic: "topic-a", Partition: 3, Offset: 42, Key: []byte(`{"broken"`), Value: []byte(`not-json`)}

	_, bad := consumer.decodeMessage(message, &timestamp)

	if bad == nil {
		t.Fatal("expected record to be classified as bad")
	}
	if bad.Topic != "topic-a" || bad.Partition != 3 || bad.Offset != 42 || bad.TimestampMS == nil {
		t.Fatalf("unexpected bad-record location: %#v", bad)
	}
	if bad.KeyError == "" || bad.ValueError == "" {
		t.Fatalf("expected separate key and value errors: %#v", bad)
	}
}

func TestNewDialerSupportsSCRAMAndRejectsUnknownMechanism(t *testing.T) {
	dialer, err := newDialer(config.KafkaSettings{SecurityProtocol: "SASL_SSL", SASLMechanism: "SCRAM-SHA-512", Username: "user", Password: "secret"})
	if err != nil || dialer.SASLMechanism == nil {
		t.Fatalf("expected SCRAM support, dialer=%#v err=%v", dialer, err)
	}
	_, err = newDialer(config.KafkaSettings{SecurityProtocol: "SASL_SSL", SASLMechanism: "GSSAPI"})
	if err == nil {
		t.Fatal("expected unsupported mechanism error")
	}
}
