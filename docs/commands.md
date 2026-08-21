# Commands

`kgrep` has three commands, all of which read from Kafka: `consume`, `dump`, and `print`.

Global options (`--env-file`, `--profile`) go **before** the command name; command-specific flags go after it:

```sh
kgrep --profile prod consume --topic orders-crud --read-all
```

## `consume`

The general-purpose command: scan a topic, optionally filter, optionally print, optionally write CSV.

```sh
kgrep --profile dev consume \
  --topic orders-crud \
  --key-format string --value-format avro \
  --allowed-keys-csv order_ids.csv \
  --match-mode kafka-key \
  --output-csv matches.csv
```

- With `--allowed-keys-csv`, only records matching (per `--match-mode`, see [matching.md](matching.md)) are emitted.
- With `--read-all` instead, every record is emitted regardless of the allowed-values set.
- With neither, `consume` emits nothing (there's nothing to match against) — use `dump` or `--read-all` if you want everything.
- Emitted records print to stdout unless `--output-csv` is given, in which case they're written there instead (not both — see the note below).

## `dump`

Shorthand for "give me everything." If you don't pass `--allowed-keys-csv`, `dump` behaves like `consume --read-all` automatically:

```sh
kgrep dump --topic orders-crud \
  --from-time 2026-04-13T00:00:00Z --to-time 2026-04-14T00:00:00Z \
  --output-csv day_dump.csv
```

If you *do* pass `--allowed-keys-csv` to `dump`, it filters like `consume` instead of emitting everything — the auto-read-all behavior only kicks in when no allowed-values file is supplied.

## `print`

Identical semantics to `dump` (auto-read-all when no allowed-values file), except intended for interactive inspection — pair it with `--print-matches-only` for compact one-line-per-record output instead of the full decoded value:

```sh
kgrep print --topic orders-crud --print-matches-only
```

## Output columns

Every emitted record — printed or written to CSV — has the same fields (`internal/csvutil.FieldNames`):

| Column | Meaning |
|---|---|
| `topic`, `partition`, `offset` | Record location |
| `timestamp` | Formatted per `--timezone` |
| `kafka_key` | Raw key as text, or a JSON snippet if the key format is `avro`/`json` |
| `key_format`, `value_format` | The format actually used to decode this record (relevant when `auto`) |
| `matched_values` | Every allowed-value string that matched, from any side |
| `matched_fields` | Which field path (or `kafka-key`/`decoded-key`/`value`) each match came from |
| `decoded_key_json`, `decoded_value_json` | The decoded payload, JSON-formatted and truncated to a few KB |

## The scan summary

Every run of `consume`/`dump`/`print` ends with:

```text
Scanned: 250
Good records: 248
Bad records: 2
Emitted: 248
```

`Scanned` = good + bad. `Emitted` can be lower than `Good records` when a filter is active and most records don't match. `--max-messages` caps `Scanned`, including bad records — so a run can stop well before matching anything if most of what it reads is malformed.

See [decoding.md](decoding.md) for what makes a record "bad," and [matching.md](matching.md) for how `Emitted` is decided.
