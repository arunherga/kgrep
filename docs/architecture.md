# Architecture

This is a map of how `kgrep` is put together, for anyone changing the code rather than just running it. It stays at the package/data-flow level deliberately — for exact flag behavior, read `internal/app/app.go` or run `kgrep --help`.

## Package map

| Package | Responsibility |
|---|---|
| `cmd/kgrep` | Entry point. Handles `--version` directly, otherwise hands off to `app.Run`. |
| `internal/app` | CLI wiring: flag parsing for `consume`/`dump`/`print`/`update`, orchestrates a scan, formats rows for stdout/CSV. |
| `internal/config` | `.env`/profile loading, `KafkaSettings` and `SchemaRegistrySettings` construction from environment variables. |
| `internal/core` | Shared, transport-neutral types: `DecodedMessage`, `BadRecord`, `MatchResult`. No logic, just the vocabulary other packages share. |
| `internal/kafkaclient` | Owns the Kafka connection: partition discovery, watermark snapshotting, bounded reads, SASL/TLS dialer setup. |
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

**Idle-poll termination is a second, independent stopping condition.** Within `bounds.high`, a partition can still go quiet before the loop reaches it (e.g. after time-range filtering skips most of the range). After `idlePolls` consecutive 1-second fetch timeouts, the loop gives up on that partition rather than waiting forever. `--idle-polls` tunes this.

**A record counts as good only if both key and value decode.** `decodeMessage` calls `DecodeKey` and `DecodeValue` independently and merges failures into one `BadRecord` with both errors kept separate (`KeyError`/`ValueError`). This is intentional: the two failure modes usually indicate different problems (e.g. wrong key format vs. corrupt Avro payload), and collapsing them would lose that signal. Deserialization failures never abort the scan — they're reported and counted, and the loop moves on.

**Format inference order matters.** `decode.InferFormat` checks, in order: 5-byte Confluent Avro header (`raw[0] == 0`) → valid JSON → valid UTF-8 string → bytes. A JSON-looking byte string that also happens to start with a `0x00` byte would be misclassified as Avro; this hasn't come up in practice because Avro's leading zero byte is not valid UTF-8/JSON-starting a byte sequence in the same way, but it's why `auto` is a heuristic, not a guarantee. Set `--key-format`/`--value-format` explicitly when you need reliable good/bad counts.

**Verbose diagnostics are injected, not global.** `decode.Deserializer` and `kafkaclient.Consumer` each take a `diagnostics io.Writer` at construction (`app.runConsume` passes `stderr`), and write through it rather than calling `fmt.Printf` directly. This matters for two reasons: it keeps stdout as pure data output (safe to pipe or redirect even with `--verbose` on), and it makes verbose behavior unit-testable (`TestVerboseDiagnosticsWriteToInjectedWriter` asserts against a `bytes.Buffer` instead of the real process stdout).

**Self-update never touches disk from inside `internal/selfupdate` without an explicit caller decision.** `app.runUpdate` takes both a `*selfupdate.Client` and an `install func(path string, content []byte) error` as parameters rather than constructing them internally — production code passes `selfupdate.New()` and `selfupdate.ReplaceExecutable`, but tests substitute a fake HTTP transport and a no-op install function, so `TestRunUpdateDownloadsVerifiesAndInstalls` (`internal/app/app_test.go`) can exercise the entire check → download → checksum-verify pipeline without ever overwriting the real test binary. `ReplaceExecutable` itself (`internal/selfupdate/selfupdate.go`) handles the two platforms differently: on Windows, the running executable can't be overwritten directly, so it's renamed aside, the new binary is renamed into place, and the old one is restored if that second rename fails; on Unix, a straight `os.Rename` works because replacing a file that's currently mapped/executing is permitted. The passive per-run notice (`checkForUpdate`) and the `dev` version sentinel both short-circuit before any network call, so a locally built binary never nags about updates it has no way to compare against.

**Schema Registry lookups are cached by schema ID, not by subject.** `RegistryClient.Codec` (`internal/decode/decode.go`) locks a `map[uint32]*goavro.Codec` under an `RWMutex` and only calls out to the registry on a cache miss. `LatestSubject` (used only in `--verbose` mode, to print the subject's current version alongside the schema ID actually on the wire) is a separate, uncached call and is not part of the hot decode path — it's diagnostic only.

**Field paths and recursive matching are separate strategies, not a fallback chain.** `filter.FindAllowedValues` either walks the exact dot-separated paths given via `--key-field`/`--value-field` (`ExtractPath`, which follows a path through both maps and lists), or — if no paths are given — recursively searches the entire decoded payload (`collect`). Passing fields narrows the search; omitting them widens it. `scalarText` deliberately renders `nil`/`bool` the way Python's `str()` would (`"None"`, `"True"`, `"False"`) rather than Go's default, since the allowed-values CSVs this tool consumes were written against the original Python CLI's behavior.

**CSV/JSON output spacing mimics Python's `json.dumps` defaults.** `csvutil.pythonJSONSpacing` re-inserts the `", "` / `": "` spacing Go's compact `json.Marshal` drops, so `decoded_value_json` columns are byte-comparable with output from the Python tool this was ported from.

**Secrets only ever come from the environment.** `config.LoadEnvironment` loads `.env` first (without overriding already-set variables), then a `--profile NAME` file (`.env.NAME`) with override permission. No command accepts a secret as a flag, by design, to keep credentials out of shell history and process listings.

## Testing approach

Every package with logic worth protecting has unit tests colocated with it (`*_test.go`). Notably:
- `internal/decode/decode_test.go` fakes the Schema Registry with a `http.RoundTripper` function type (`roundTripFunc`) rather than hitting a real registry, and asserts the schema is fetched once and then served from cache.
- `internal/kafkaclient/consumer_test.go` tests `newDialer` (SASL/TLS config mapping) and `decodeMessage` (good/bad classification) directly, without a live broker — the network-facing parts of `Iterate` itself aren't unit-tested and are exercised only against a real cluster.
- `internal/selfupdate/selfupdate_test.go` and `internal/app/app_test.go` fake the GitHub API the same way, and additionally exercise `ReplaceExecutable` for real against a throwaway file in `t.TempDir()` — never the actual test binary.

CI (`.github/workflows/release.yml`) currently runs `go vet` and `go test -race` only as a release gate — triggered by pushing a `vX.Y.Z` tag on `main` — not on every commit or pull request. There is no continuous-integration workflow independent of cutting a release.

## Extension points

- **New decode format**: add a case in `decode.Deserializer.decode` and register the name in `decode.Formats`.
- **New match mode**: add a case in `filter.Evaluate`'s switch and register the name in `filter.MatchModes`.
- **New field shortcut** (like `delivery-identifiers`): extend the constant and expansion logic in `app.resolvePaths` (`internal/app/app.go`).
