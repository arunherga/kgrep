# Decoding

`--key-format` and `--value-format` each accept `auto` (default), `json`, `avro`, `string`, or `bytes`. This controls how the raw Kafka bytes for that side of the record become the value used for matching and display.

## `auto` mode

When a side is `auto`, `kgrep` infers the format per-record, in this order:

1. **Avro** — if the payload is at least 5 bytes and the first byte is `0x00` (Confluent's wire-format magic byte). This label covers *any* Confluent-wire-format payload, not literally only Avro — see [below](#avro--json-schema--protobuf--schema-registry) for how `kgrep` tells Avro, JSON Schema, and Protobuf apart once it gets there, since all three share this exact same header.
2. **JSON** — if, after trimming whitespace, the payload starts with `{`, `[`, or `"` *and* parses as valid JSON.
3. **String** — if the payload is valid UTF-8.
4. **Bytes** — everything else (arbitrary binary).

This is a heuristic, not a schema. In particular:
- A plain JSON payload and a Confluent-wire-format payload are told apart only by that leading byte.
- Valid UTF-8 that isn't valid JSON (e.g. a plain string key like `"order-42"` without quotes, i.e. the raw bytes `order-42`) is *intentionally* treated as `string`, not an error.
- Arbitrary binary is treated as `bytes`, also not an error.

Because `string` and `bytes` never fail to decode, **`auto` mode's good/bad counts are not very meaningful** — almost everything decodes as *something*. If you're using the scan summary's `Good records`/`Bad records` counts to judge data quality, set the format explicitly instead:

```sh
kgrep consume --topic orders-crud --key-format string --value-format avro ...
```

Now a value that isn't valid Avro is correctly counted as bad, instead of silently being reinterpreted as a string.

## Avro / JSON Schema / Protobuf + Schema Registry

Confluent's Avro, JSON Schema, and Protobuf serializers all use the *same* wire format: a `0x00` magic byte, a 4-byte big-endian schema ID, then the encoded payload. That header alone doesn't say which of the three it is — `kgrep` looks up the schema ID's registered type from Schema Registry to find out, then decodes accordingly:

- **Avro** — decoded fully. `kgrep` fetches (and caches) the schema by ID and decodes the binary payload; you never need to supply a schema up front.
- **JSON Schema** — decoded fully. The payload after the 5-byte header is already plain JSON (the schema is only used by producers for validation), so this is parsed the same way as plain `json` values.
- **Protobuf** — not supported yet. A Protobuf record is reported as a bad record with a clear `... is PROTOBUF, which kgrep does not support decoding yet` error, rather than a misleading Avro-compile failure.

Other notes:
- `SCHEMA_REGISTRY_URL` (plus optional `SCHEMA_REGISTRY_USERNAME`/`PASSWORD` for basic auth) must be set for any of this to work; without it, these records fail with a clear error rather than being silently skipped.
- Each distinct schema ID is fetched once per process and cached after that (both its type and, for Avro, its compiled schema) — a long-running scan across many records with the same schema does not re-fetch it per record.
- `--verbose` additionally looks up and prints the `<topic>-key` or `<topic>-value` subject's *latest* registered version, ID, and type, for comparing against the schema ID actually seen on the wire (useful for spotting producers still writing an old schema version). This lookup is not cached and is not on the decode path — it only runs in verbose mode, once per record's key/value, purely as a diagnostic. If the lookup itself fails (e.g. the subject doesn't exist), that failure is printed as a diagnostic but does not fail the record — the record's good/bad status still depends only on whether the schema-ID-based decode itself succeeded.

## Bad records

A record counts as bad if *either* its key or its value fails to decode under the configured/inferred format. Both sides are attempted independently, so a record can have a key error, a value error, or both — they're reported and counted separately rather than merged into one failure:

```text
BAD orders[3]@42 key_error=decode key as JSON: unexpected EOF
BAD orders[5]@107 value_error=decode value as Avro: invalid Confluent wire header
```

Deserialization failures never stop a scan — the bad record is logged with its exact topic/partition/offset, counted, and the scan continues. See [commands.md](commands.md#the-scan-summary) for how bad records factor into the end-of-run summary.
