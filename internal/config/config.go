package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type KafkaSettings struct {
	BootstrapServers []string
	SecurityProtocol string
	SASLMechanism    string
	Username         string
	Password         string
	GroupID          string
	DefaultTopic     string
}

type SchemaRegistrySettings struct {
	URL      string
	Username string
	Password string
}

func LoadEnvironment(path, profile string) ([]string, error) {
	loaded := make([]string, 0, 2)
	if ok, err := loadDotEnv(path, false); err != nil {
		return nil, err
	} else if ok {
		loaded = append(loaded, path)
	}
	if profile != "" {
		profilePath := filepath.Join(filepath.Dir(path), filepath.Base(path)+"."+profile)
		ok, err := loadDotEnv(profilePath, true)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("environment profile file not found: %s", profilePath)
		}
		loaded = append(loaded, profilePath)
	}
	return loaded, nil
}

func loadDotEnv(path string, override bool) (bool, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("open environment file %q: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.Trim(strings.TrimSpace(parts[1]), "\""), "'")
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists || override {
			if err := os.Setenv(key, value); err != nil {
				return false, fmt.Errorf("set environment variable %q: %w", key, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("read environment file %q: %w", path, err)
	}
	return true, nil
}

func KafkaFromEnv(groupOverride string) (KafkaSettings, error) {
	bootstrap := strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	if bootstrap == "" {
		return KafkaSettings{}, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required")
	}
	group := groupOverride
	if group == "" {
		group = getenv("KAFKA_GROUP_ID", "unified-kafka-cli")
	}
	servers := make([]string, 0)
	for _, server := range strings.Split(bootstrap, ",") {
		if trimmed := strings.TrimSpace(server); trimmed != "" {
			servers = append(servers, trimmed)
		}
	}
	return KafkaSettings{
		BootstrapServers: servers,
		SecurityProtocol: getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
		SASLMechanism:    getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
		Username:         os.Getenv("KAFKA_USERNAME"), Password: os.Getenv("KAFKA_PASSWORD"),
		GroupID: group, DefaultTopic: os.Getenv("KAFKA_DEFAULT_TOPIC"),
	}, nil
}

func SchemaRegistryFromEnv() SchemaRegistrySettings {
	return SchemaRegistrySettings{URL: os.Getenv("SCHEMA_REGISTRY_URL"), Username: os.Getenv("SCHEMA_REGISTRY_USERNAME"), Password: os.Getenv("SCHEMA_REGISTRY_PASSWORD")}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
