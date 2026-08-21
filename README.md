# kgrep

A Kafka consumer/inspector CLI written in Go, using a pure-Go Kafka client.

## Functionality

- `consume`: scan and filter records, optionally printing and/or writing CSV
- `dump`: emit all records when no allowed-values file is supplied
- `print`: print all records when no allowed-values file is supplied
- `query`: filter a previously generated CSV by a selected key column
- independent `auto`, `json`, `avro`, `string`, and `bytes` decoding for keys and values
- Confluent wire-format Avro decoding through Schema Registry, with schema-ID caching
- raw Kafka-key, decoded-key, decoded-value, key-or-value, and key-and-value matching
- recursive matching or repeatable dot-separated key/value field paths
- the `delivery-identifiers` field shortcut
- epoch/ISO-8601 time bounds, IANA timezone output, environment profiles, CSV output, bounded reads, graceful cancellation, and verbose diagnostics
- good/bad deserialization counts, with topic, partition, offset, and key/value error details for every bad record

The Kafka reader snapshots each partition's high watermark before scanning. This gives the command a finite endpoint even while producers continue writing.

Deserialization failures do not stop a scan. A record is counted as good only when both its key and value deserialize successfully. If either side fails, the CLI prints its exact location and the corresponding error, continues scanning, and includes it in the final bad-record count:

```text
BAD orders[3]@42 key_error=decode key as JSON: unexpected EOF
BAD orders[5]@107 value_error=decode value as Avro: invalid Confluent wire header
Scanned: 250
Good records: 248
Bad records: 2
Emitted: 248
```

`Emitted` can be lower than `Good records` when filters are active. `--max-messages` limits the total number of inspected records, including bad records.

For meaningful data-quality counts, set the expected formats explicitly, such as `--key-format string --value-format avro`. In `auto` mode, valid UTF-8 that is not valid JSON is intentionally treated as a string, and arbitrary binary data is treated as bytes; those values are therefore considered successfully deserialized.

## Build and test

Go 1.25 or newer is required.

```sh
go test -race ./...
go vet ./...
go build -trimpath -o bin/kgrep ./cmd/kgrep
```

## Configuration

The environment variable names match the Python CLI:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=broker-a:9092,broker-b:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_USERNAME=replace-me
KAFKA_PASSWORD=replace-me
KAFKA_GROUP_ID=unified-kafka-cli
KAFKA_DEFAULT_TOPIC=replace-me
SCHEMA_REGISTRY_URL=https://registry.example
SCHEMA_REGISTRY_USERNAME=replace-me
SCHEMA_REGISTRY_PASSWORD=replace-me
```

Shared values are loaded from `.env`. A command such as `--profile qa` then loads `.env.qa` and overrides shared values. Secrets are never accepted as command-line options, which keeps them out of shell history and process listings.

## Examples

```sh
# Help
./bin/kgrep --help

# Read every message in the snapshot and write it to CSV
./bin/kgrep --profile dev consume \
  --topic orders-crud \
  --key-format string \
  --value-format avro \
  --read-all \
  --output-csv kafka_dump.csv

# Search selected nested value fields
./bin/kgrep --profile prod consume \
  --topic shipments-intransit \
  --allowed-keys-csv ../search_value.csv \
  --match-mode value-field \
  --field-shortcut delivery-identifiers \
  --print-matches-only

# Dump a bounded time range
./bin/kgrep dump \
  --topic orders-crud \
  --from-time 2026-04-13T00:00:00Z \
  --to-time 2026-04-14T00:00:00Z \
  --output-csv kafka_dump.csv

# Query an output CSV without connecting to Kafka
./bin/kgrep query \
  --input-csv kafka_dump.csv \
  --allowed-keys-csv allowed_keys.csv
```

Global options (`--env-file` and `--profile`) must appear before the command, matching the Python CLI. Command-specific options appear after it.

## Compatibility notes

- Kafka security protocols `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` are recognized. SASL mechanisms `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` are supported.
- Avro payloads must use Confluent's five-byte wire header. The schema ID in each message selects the writer schema; verbose mode additionally reports the standard `<topic>-key` or `<topic>-value` subject's latest version.
- `--max-messages 0` means unlimited. Negative limits and invalid time ranges are rejected early.
