# Commands

`kgrep` has four commands: `consume`, `dump`, and `print` read from Kafka; `update` upgrades kgrep itself and doesn't touch Kafka at all.

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
- Emitted records always print to stdout, whether or not `--output-csv` is also given — `consume`/`print` are meant for watching matches happen live even while archiving them to CSV. `dump` is the one exception: when `--output-csv` is set, `dump` writes only to the CSV and stays silent on stdout, since a full-topic export could otherwise flood the terminal.

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

## `update`

Downloads the latest release for your OS/architecture, verifies its checksum against `checksums.txt`, and replaces the currently running `kgrep` binary with it — no manual download needed:

```sh
kgrep update
```

```text
Checking for updates...
Downloading kgrep v2.1.0 for linux/amd64...
Updated kgrep v2.0.1 -> v2.1.0.
```

If you're already on the latest version, it says so and exits without changing anything. `update` isn't available for locally built development binaries (version `dev`) — build a new one from source instead, or grab a release from the [Releases page](https://github.com/arunherga/kgrep/releases/latest).

If kgrep is installed somewhere that needs elevated permissions to write to (e.g. `/usr/local/bin`), run it with `sudo`/as Administrator, or download and replace the binary manually.

### The "newer version available" notice

Every other command also checks for a newer release in the background and, if one exists, prints a one-line notice to stderr after it finishes:

```text
A newer version of kgrep is available: v2.1.0 (you have v2.0.1). Run 'kgrep update' to upgrade.
```

This check has a 2-second timeout and never fails or delays the command it's attached to — if you're offline, rate-limited, or otherwise can't reach GitHub, it's silently skipped. Set `KGREP_SKIP_UPDATE_CHECK=1` to disable it entirely (useful in scripts/CI, or restricted networks where the outbound call itself is unwanted).

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

## Verbose diagnostics

`--verbose` prints extra detail — partition discovery, per-partition watermarks, format-inference decisions, Schema Registry subject/version lookups — to **stderr**, separately from the emitted records and scan summary on stdout. That means `--verbose` is safe to combine with piping stdout elsewhere (e.g. redirecting it into a file): the diagnostic lines won't get mixed into the data.
