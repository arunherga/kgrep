package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadEnvironmentProfileOverridesBase(t *testing.T) {
	directory := t.TempDir()
	base := filepath.Join(directory, ".env")
	if err := os.WriteFile(base, []byte("KAFKA_TEST_BOOTSTRAP=base:9092\nKAFKA_TEST_USER=base-user\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+".qa", []byte("KAFKA_TEST_BOOTSTRAP=qa:9092\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TEST_BOOTSTRAP", "")
	os.Unsetenv("KAFKA_TEST_BOOTSTRAP")
	t.Setenv("KAFKA_TEST_USER", "")
	os.Unsetenv("KAFKA_TEST_USER")
	loaded, err := LoadEnvironment(base, "qa")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, []string{base, base + ".qa"}) || os.Getenv("KAFKA_TEST_BOOTSTRAP") != "qa:9092" || os.Getenv("KAFKA_TEST_USER") != "base-user" {
		t.Fatalf("loaded=%v bootstrap=%q user=%q", loaded, os.Getenv("KAFKA_TEST_BOOTSTRAP"), os.Getenv("KAFKA_TEST_USER"))
	}
}

func TestKafkaFromEnvValidatesAndSplitsBrokers(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-a:9092, broker-b:9092")
	settings, err := KafkaFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(settings.BootstrapServers, []string{"broker-a:9092", "broker-b:9092"}) {
		t.Fatalf("unexpected settings: %#v", settings)
	}
}
