# Commands

`kgrep` has six commands: `consume`, `dump`, and `print` read records from a topic; `topics` and `describe-topic` inspect cluster/topic metadata instead of reading records; `update` upgrades kgrep itself and doesn't touch Kafka at all.

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

## `topics`

Lists every topic in the cluster, with its partition count and whether it's internal (like `__consumer_offsets`) or a regular topic:

```sh
kgrep topics
```

```text
TOPIC                 PARTITIONS  TYPE
__consumer_offsets    50          internal
orders-crud           6           regular
shipments-intransit   12          regular

3 topic(s)
```

## `describe-topic`

Everything kgrep can tell you about one topic — partition layout, retained message count, when it was last written to, its Schema Registry schema type (if configured), and every consumer group associated with it, active or not, with per-group lag:

```sh
kgrep describe-topic --topic orders-crud
```

```text
Topic: orders-crud (regular)
Partitions: 6
Retained messages: 812345
Last message written: 2026-04-13 18:22:04 UTC

Schema:
  key: (Schema Registry not configured)
  value: (Schema Registry not configured)

Partitions:
ID     LEADER   REPLICAS         ISR                     LOW       HIGH   MESSAGES
0      1        1,2,3            1,2,3                     0     135678     135678
1      2        1,2,3            1,2,3                     0     140012     140012
...

Consumer groups:
  billing-service [Stable, active] lag=142
    - billing-1 @ /10.0.0.5 (partitions 0,1,2)
    - billing-2 @ /10.0.0.6 (partitions 3,4,5)
  reporting-batch [Empty, inactive] lag=812345
```

`--topic` defaults to `KAFKA_DEFAULT_TOPIC` like the other commands. A few things worth knowing about what this command can and can't tell you:

- **"Retained messages" is not "total messages ever produced."** Like everywhere else in kgrep, this is `high watermark − low watermark` per partition — it reflects what's currently retained under your topic's retention/compaction settings, not history that's already aged out.
- **"Inactive" means the group has committed offsets for this topic but no members right now** — it consumed from this topic at some point and stopped (crashed, scaled to zero, decommissioned), not that it's currently working slowly. An inactive group's lag just reflects how far behind its last commit is from the current high watermark.
- **Schema type requires `SCHEMA_REGISTRY_URL`** to be set, same as Avro decoding elsewhere. Without it, both key and value show as "not configured" rather than being silently omitted.
- **Two things Kafka's protocol doesn't expose at all, so kgrep can't report them:** which producer clients wrote to a topic (Kafka has no concept of "producer identity" that's queryable after the fact), and the topic's on-disk size in bytes (would require the `DescribeLogDirs` API, which the Kafka client library kgrep uses has never implemented in any release).
- **`--verbose` prints why a consumer group was left out of the report**, if any were: a group can fail to describe, fail to have its offsets fetched, or turn out to have no committed offset for this topic at all, and by default those are just silently excluded (transient per-group issues shouldn't sink the whole report). With `--verbose`, each exclusion prints a one-line reason to stderr instead of vanishing silently, e.g. `Group "reporting-batch": no committed offset for topic "orders-crud", excluding from report`.

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
Partition coverage: 3/3 fully read (no messages missed)
```

`Scanned` = good + bad. `Emitted` can be lower than `Good records` when a filter is active and most records don't match. `--max-messages` caps `Scanned`, including bad records — so a run can stop well before matching anything if most of what it reads is malformed.

`Partition coverage` tells you whether to trust the scan as complete. Every partition stops for one of: reaching the end, `--max-messages`, `--to-time`, or running out of `--idle-polls` patience while data was possibly still arriving slower than that. Only the last one is untrustworthy, and it's the only thing that keeps a partition out of the "fully read" count — if any partition falls into it, the line names it along with the offset it actually reached, e.g. `partition 1: read up to offset 41234 of 44720 (3486 message(s) potentially unread)`. If you see that, rerun with a higher `--idle-polls`.

See [decoding.md](decoding.md) for what makes a record "bad," and [matching.md](matching.md) for how `Emitted` is decided.

## Verbose diagnostics

`--verbose` prints extra detail — partition discovery, per-partition watermarks, format-inference decisions, Schema Registry subject/version lookups — to **stderr**, separately from the emitted records and scan summary on stdout. That means `--verbose` is safe to combine with piping stdout elsewhere (e.g. redirecting it into a file): the diagnostic lines won't get mixed into the data.
