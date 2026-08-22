# Architecture

This is a map of how `kgrep` is put together, for anyone changing the code rather than just running it. It stays at the package/data-flow level deliberately — for exact flag behavior, read `internal/app/app.go` or run `kgrep --help`.

## Package map

| Package | Responsibility |
|---|---|
| `cmd/kgrep` | Entry point. Handles `--version` directly, otherwise hands off to `app.Run`. |
| `internal/app` | CLI wiring: flag parsing for `consume`/`dump`/`print`/`topics`/`describe-topic`/`update`, orchestrates a scan, formats rows/topic reports for stdout/CSV. |
| `internal/config` | `.env`/profile loading, `KafkaSettings` and `SchemaRegistrySettings` construction from environment variables. |
| `internal/core` | Shared, transport-neutral types: `DecodedMessage`, `BadRecord`, `MatchResult`. No logic, just the vocabulary other packages share. |
| `internal/kafkaclient` | Owns the Kafka connection: partition discovery, watermark snapshotting, bounded reads, SASL/TLS dialer setup (`Consumer`), plus cluster/topic/consumer-group metadata for `topics`/`describe-topic` (`Admin`). |
| `internal/decode` | Turns raw key/value bytes into Go values: format inference, JSON, Avro (via Confluent wire format + Schema Registry), string, bytes. |
| `internal/filter` | Match-mode evaluation against an allowed-values set, including dot-path field extraction. |
| `internal/csvutil` | Allowed-values CSV loading, output CSV writing, JSON-snippet formatting. |
| `internal/timeutil` | Epoch/ISO-8601 parsing and timezone-aware formatting. |
| `internal/selfupdate` | GitHub release lookup, checksum verification, and safe in-place binary replacement for `kgrep update` and the passive update notice. |

## Data flow (`consume`/`dump`/`print`)

```
app.runConsume
  │
  ├─ config.KafkaFromEnv / config.SchemaRegistryFromEnv   (env → settings)
  ├─ csvutil.LoadAllowedValues                             (optional filter set)
  ├─ decode.New                                            (one Deserializer per run)
  ├─ kafkaclient.New                                       (one Consumer per run)
  │
  └─ consumer.Iterate(ctx, maxMessages, emit, reportBad)
       │
       ├─ per partition: partitionBounds()                 (snapshot low/high watermark once)
       ├─ per partition: kafka.NewReader + SetOffset(At)
       └─ poll loop:
            reader.FetchMessage
              → decodeMessage (decode.Deserializer.DecodeKey/DecodeValue)
                  → bad?  reportBad(BadRecord)   [printed, scan continues]
                  → good? filter.Evaluate(...)   → MatchResult
                              → emit(DecodedMessage)  [app.RowForMessage → print / CSV row]
```

## Key invariants

**The high-watermark snapshot is what makes a scan finite.** `Consumer.partitionBounds` reads each partition's low/high offset once, before consuming it (`internal/kafkaclient/consumer.go`). The read loop then stops at `bounds.high`, so `consume --read-all` on a live topic terminates even while producers keep appending — new records simply aren't part of that scan.

**Partitions are scanned concurrently, not sequentially.** `Consumer.Iterate` fans out one goroutine per partition (`iteratePartition`) instead of looping over them one at a time. This matters because idle-poll waits used to be additive across partitions when run sequentially — on a slow/flaky broker, a topic with several partitions could spend minutes per partition just waiting, one after another. `emit`/`reportBad`, the shared `processed` counter (for `--max-messages`), and diagnostics writes are all serialized through one `sync.Mutex` since they run concurrently now.

**`pollTimeout` (currently 5s) is both the reader's `MaxWait` and the duration of one idle poll — raising it fixed a real throttling bug, not just a cosmetic one.** It was originally 1s. Against a live Confluent Cloud cluster enforcing a consume-bandwidth quota, a too-short `MaxWait` meant fetch attempts routinely timed out waiting for a throttled broker response, then immediately retried (`kafka-go`'s reader resets its backoff to zero on `RequestTimedOut`) — faster than the throttle delay actually cleared, so partitions starved on data that was genuinely still there. Raising `MaxWait` to 5s took one 36,848-message topic from 50% coverage in 7m46s to 100% coverage in 41s. `--idle-polls` bookkeeping uses the same duration, so `defaultIdlePolls = 60` (in `internal/app/app.go`) means ~5 minutes of patience per partition, not 60 seconds.

**Idle-poll termination is a second, independent stopping condition — and an unreliable one.** Within `bounds.high`, a partition can still go quiet before the loop reaches it (e.g. after time-range filtering skips most of the range, or because of broker-side throttling as above). After `idlePolls` consecutive fetch timeouts, the loop gives up on that partition rather than waiting forever. `--idle-polls` tunes this, but it cannot distinguish "no more data before `bounds.high`" from "data exists but is arriving slowly" — both look identical from inside the loop. This is why `PartitionCoverage` exists (see below): idle-timeout is the one stop reason that must not be trusted as "fully read."

**`PartitionCoverage` turns "probably read everything" into a checkable fact.** `iteratePartition` returns a `Reason` for why it stopped (`reached-high`, `empty`, `max-messages`, `to-time`, `canceled`, or `idle-timeout`) alongside `LastOffset`. `PartitionCoverage.Complete()` is `Reason != "idle-timeout"` — every other reason is either a real end or something the caller explicitly asked for, so only idle-timeout indicates a possible gap. `internal/app/app.go`'s `writeCoverage` prints this as `Partition coverage: N/M fully read` after every scan, with per-partition offset detail when incomplete. Without this, "Scanned: X" alone can't tell you whether X is the whole topic or an early bailout — the two look the same in the output otherwise.

**`Consumer`'s per-partition `DialLeader` + `kafka.Reader` dial pattern is measurably slower against some real clusters than `Admin`'s self-routing `kafka.Client`.** Tested live against a Confluent Cloud cluster: `consume`/`dump`/`print` on a topic with just 3 messages to read took 30–45 seconds end to end, with `--verbose` on or off making no measurable difference (ruling out per-record Schema Registry lookups as the cause) — the cost is in `Consumer`'s connection-establishment path itself, not decoding. This is pre-existing behavior, not something introduced alongside `Admin`, and every read still completed with correct data every time — it's a latency characteristic of this dial pattern against that specific cluster/region, not a hang or a correctness bug. If `Consumer` is ever revisited for performance, `Admin`'s approach (one `kafka.Client`/`Transport` that discovers and routes to brokers itself, rather than a fresh `DialLeader` per partition) is the template, matching the note on `Admin` below.

**A record counts as good only if both key and value decode.** `decodeMessage` calls `DecodeKey` and `DecodeValue` independently and merges failures into one `BadRecord` with both errors kept separate (`KeyError`/`ValueError`). This is intentional: the two failure modes usually indicate different problems (e.g. wrong key format vs. corrupt Avro payload), and collapsing them would lose that signal. Deserialization failures never abort the scan — they're reported and counted, and the loop moves on.

**Format inference order matters.** `decode.InferFormat` checks, in order: 5-byte Confluent Avro header (`raw[0] == 0`) → valid JSON → valid UTF-8 string → bytes. A JSON-looking byte string that also happens to start with a `0x00` byte would be misclassified as Avro; this hasn't come up in practice because Avro's leading zero byte is not valid UTF-8/JSON-starting a byte sequence in the same way, but it's why `auto` is a heuristic, not a guarantee. Set `--key-format`/`--value-format` explicitly when you need reliable good/bad counts.

**Verbose diagnostics are injected, not global.** `decode.Deserializer` and `kafkaclient.Consumer` each take a `diagnostics io.Writer` at construction (`app.runConsume` passes `stderr`), and write through it rather than calling `fmt.Printf` directly. This matters for two reasons: it keeps stdout as pure data output (safe to pipe or redirect even with `--verbose` on), and it makes verbose behavior unit-testable (`TestVerboseDiagnosticsWriteToInjectedWriter` asserts against a `bytes.Buffer` instead of the real process stdout).

**Self-update never touches disk from inside `internal/selfupdate` without an explicit caller decision.** `app.runUpdate` takes both a `*selfupdate.Client` and an `install func(path string, content []byte) error` as parameters rather than constructing them internally — production code passes `selfupdate.New()` and `selfupdate.ReplaceExecutable`, but tests substitute a fake HTTP transport and a no-op install function, so `TestRunUpdateDownloadsVerifiesAndInstalls` (`internal/app/app_test.go`) can exercise the entire check → download → checksum-verify pipeline without ever overwriting the real test binary. `ReplaceExecutable` itself (`internal/selfupdate/selfupdate.go`) handles the two platforms differently: on Windows, the running executable can't be overwritten directly, so it's renamed aside, the new binary is renamed into place, and the old one is restored if that second rename fails; on Unix, a straight `os.Rename` works because replacing a file that's currently mapped/executing is permitted. The passive per-run notice (`checkForUpdate`) and the `dev` version sentinel both short-circuit before any network call, so a locally built binary never nags about updates it has no way to compare against.

**`Admin` (`internal/kafkaclient/admin.go`) uses `kafka.Client`, not `kafka.Dialer`/`kafka.Reader` like `Consumer`.** `Client`'s `Transport` self-discovers cluster layout and routes each request (Metadata, ListOffsets, ListGroups, DescribeGroups, OffsetFetch, Fetch) to the correct broker automatically, so `Admin` never manually dials a specific partition leader the way `Consumer.partitionBounds` does. This is also why watermarks for `describe-topic` come from one bulk `ListOffsets` call across all partitions instead of `Consumer`'s per-partition `DialLeader` + separate low/high reads — a real inefficiency in `Consumer` that wasn't worth refactoring for this feature but would be the template if `Consumer` is ever revisited. The pure aggregation logic (`buildPartitionDetails`, `summarizeGroup`) is deliberately factored out from the I/O-performing methods so it's testable against hand-built `kafka-go` response structs, without needing to fake `Client`'s `RoundTripper` (which operates on `kafka-go`'s internal wire-protocol types, not its friendly request/response structs, and isn't worth the complexity to mock).

**A `ListOffsets` request only populates the field matching what you asked for.** Sending `FirstOffsetOf(p)` returns that partition's `FirstOffset` correctly but `LastOffset` comes back as `-1` (and vice versa for `LastOffsetOf`) — this isn't documented clearly by `kafka-go` and was only caught by testing against a real cluster (an empty-looking topic that actually had 19 retained messages). `buildPartitionDetails` requires both `FirstOffsetOf(p)` and `LastOffsetOf(p)` per partition in the same request; the response then correctly merges them into one `PartitionOffsets` entry per partition.

**`DescribeGroups` and `OffsetFetch` scale badly as large single calls, but fine as many small concurrent ones.** Measured against a real 85-consumer-group Confluent Cloud cluster: one `DescribeGroups` call for all 85 groups took ~45s; batches of `describeGroupsBatchSize` (5) run at `adminFetchConcurrency` (20) took ~2s. `OffsetFetch` has no bulk variant at all in `kafka-go`, so it's one call per group — sequential took ~61s, the same concurrency dropped that to ~5s. `fillLastMessageTimes` applies the identical pattern per-partition for the same reason. `ListGroups` itself has no per-item structure to parallelize (it takes no arguments), so it remains the practical floor on `describe-topic`'s latency for a group-heavy cluster (~20s in that same test) — the 3-minute context timeout in `app.runDescribeTopic` is sized generously above that, not because 3 minutes is expected, but because cluster-to-cluster variance in group count is unknown at the call site.

**Two things `describe-topic` cannot report, on purpose, not as an oversight:** which producer client wrote to a topic (Kafka's protocol has no concept of producer identity that survives past the write itself), and the topic's on-disk byte size (the `DescribeLogDirs` API that would provide this has never been implemented by `kafka-go`, checked across all of its published versions as of this writing). Getting topic size would mean switching to a different Kafka client library, which conflicts with this project's pure-Go, no-cgo design.

**A consumer group counts as "associated with" a topic if it has ever committed an offset for it, active or not.** `groupsForTopic` calls `OffsetFetch` per group (`kafka-go`'s `Client` has no bulk variant) and keeps the group only if at least one partition has a non-sentinel (`>= 0`) committed offset — `-1` means that group never committed on that partition. `Active` is a separate signal (`len(group.Members) > 0` from `DescribeGroups`), so a group can appear with real lag but zero active members, meaning it consumed in the past and stopped.

**Schema Registry lookups are cached by schema ID, not by subject.** `RegistryClient.Codec` (`internal/decode/decode.go`) locks a `map[uint32]*goavro.Codec` under an `RWMutex` and only calls out to the registry on a cache miss. `LatestSubject` (used only in `--verbose` mode, to print the subject's current version alongside the schema ID actually on the wire) is a separate, uncached call and is not part of the hot decode path — it's diagnostic only.

**Field paths and recursive matching are separate strategies, not a fallback chain.** `filter.FindAllowedValues` either walks the exact dot-separated paths given via `--key-field`/`--value-field` (`ExtractPath`, which follows a path through both maps and lists), or — if no paths are given — recursively searches the entire decoded payload (`collect`). Passing fields narrows the search; omitting them widens it. `scalarText` deliberately renders `nil`/`bool` the way Python's `str()` would (`"None"`, `"True"`, `"False"`) rather than Go's default, since the allowed-values CSVs this tool consumes were written against the original Python CLI's behavior.

**CSV/JSON output spacing mimics Python's `json.dumps` defaults.** `csvutil.pythonJSONSpacing` re-inserts the `", "` / `": "` spacing Go's compact `json.Marshal` drops, so `decoded_value_json` columns are byte-comparable with output from the Python tool this was ported from.

**Secrets only ever come from the environment.** `config.LoadEnvironment` loads `.env` first (without overriding already-set variables), then a `--profile NAME` file (`.env.NAME`) with override permission. No command accepts a secret as a flag, by design, to keep credentials out of shell history and process listings.

## Testing approach

Every package with logic worth protecting has unit tests colocated with it (`*_test.go`). Notably:
- `internal/decode/decode_test.go` fakes the Schema Registry with a `http.RoundTripper` function type (`roundTripFunc`) rather than hitting a real registry, and asserts the schema is fetched once and then served from cache.
- `internal/kafkaclient/consumer_test.go` tests `newDialer` (SASL/TLS config mapping) and `decodeMessage` (good/bad classification) directly, without a live broker — the network-facing parts of `Iterate` itself aren't unit-tested and are exercised only against a real cluster.
- `internal/selfupdate/selfupdate_test.go` and `internal/app/app_test.go` fake the GitHub API the same way, and additionally exercise `ReplaceExecutable` for real against a throwaway file in `t.TempDir()` — never the actual test binary.
- `internal/kafkaclient/admin_test.go` tests `buildPartitionDetails` and `summarizeGroup` against hand-built `kafka-go` response structs (no fake transport needed, since these are pure functions over already-fetched data) — the I/O-performing `Admin` methods themselves (`ListTopics`, `DescribeTopic`) aren't unit-tested and were instead verified by hand against the real network/error paths (a live cluster wasn't available in this environment to test the happy path end-to-end).

CI (`.github/workflows/release.yml`) currently runs `go vet` and `go test -race` only as a release gate — triggered by pushing a `vX.Y.Z` tag on `main` — not on every commit or pull request. There is no continuous-integration workflow independent of cutting a release.

## Extension points

- **New decode format**: add a case in `decode.Deserializer.decode` and register the name in `decode.Formats`.
- **New match mode**: add a case in `filter.Evaluate`'s switch and register the name in `filter.MatchModes`.
- **New field shortcut** (like `delivery-identifiers`): extend the constant and expansion logic in `app.resolvePaths` (`internal/app/app.go`).
