# kgrep

A command-line tool for looking inside a Kafka topic: search for specific records, export what you find to a CSV file, or just watch messages go by — without writing any code.

## What you can do with it

- **Search** a topic for records matching values you care about (an order ID, a customer ID, anything) — [`consume`](docs/commands.md)
- **Export** everything in a topic to a CSV file you can open in Excel or Google Sheets — [`dump`](docs/commands.md)
- **Watch** records scroll by live in your terminal, for a quick look — [`print`](docs/commands.md)

It automatically understands plain text, JSON, and Avro-encoded messages, and it tells you clearly when a record couldn't be read, instead of silently skipping it or crashing.

---

## Quick start (no coding required)

### 1. Download kgrep

Go to the [Releases page](https://github.com/arunherga/kgrep/releases/latest) and download the file that matches your computer:

| Your computer | Download |
|---|---|
| Windows | `kgrep-windows-amd64.exe` |
| Mac (Apple Silicon — M1/M2/M3/M4) | `kgrep-darwin-arm64` |
| Mac (Intel) | `kgrep-darwin-amd64` |
| Linux | `kgrep-linux-amd64` |

Not sure which Mac you have? Click the Apple menu → **About This Mac**. If it says "Apple M1/M2/M3/M4," pick Apple Silicon; if it says "Intel," pick Intel.

Detailed step-by-step download/verify/install commands for each OS are in [Installing in detail](#installing-in-detail) below.

### 2. Tell it how to connect to Kafka

kgrep reads its connection details from a plain text file named `.env`, sitting next to the program. Ask whoever manages your Kafka cluster for these values, then create a file called `.env` with a text editor (Notepad, TextEdit, VS Code — anything that saves plain text) containing:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=broker-a:9092,broker-b:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_USERNAME=the-username-you-were-given
KAFKA_PASSWORD=the-password-you-were-given
KAFKA_DEFAULT_TOPIC=the-topic-you-want-to-look-at
```

Save it in the same folder as the `kgrep` program. See [Configuration](#configuration) below for what each line means and what to do if your setup also uses Schema Registry.

### 3. Run your first command

Open a terminal in the folder where you saved `kgrep` and its `.env` file, then run:

```sh
kgrep print --read-all
```

(On Windows, run `kgrep.exe print --read-all` instead.)

This connects to the topic named in `KAFKA_DEFAULT_TOPIC` and prints every record it finds. To stop early, press `Ctrl+C`.

### 4. Understand what you're seeing

Each line printed is one record. At the end, you'll see a summary like:

```text
Scanned: 250
Good records: 248
Bad records: 2
Emitted: 248
```

- **Scanned** — how many records it looked at in total.
- **Good records** — how many it could read successfully.
- **Bad records** — how many it couldn't read (shown above the summary, with the reason and exact location — this doesn't stop the scan).
- **Emitted** — how many were actually shown to you (lower than "Good records" if you searched for specific values and most records didn't match).

---

## A few everyday examples

**Export everything from a topic to a CSV file** (open it later in Excel/Sheets):

```sh
kgrep dump --topic orders-crud --output-csv my_export.csv
```

**Search for specific values** — say you have a list of order IDs you're looking for. Save them as `order_ids.csv`, one per line (or one per column) — just the values, **no header row**:

```csv
order-1001
order-1002
order-1005
```

Then point kgrep at it:

```sh
kgrep consume --topic orders-crud --allowed-keys-csv order_ids.csv
```

kgrep will scan the topic and only show you records where one of these values appears — as the record's key, inside its decoded value, or both, depending on `--match-mode` (see [docs/matching.md](docs/matching.md)). Every non-empty cell in the file counts as a value to search for, so leave out any header/column-title row — it would otherwise be treated as a value to search for too.

**Only look at a specific time window:**

```sh
kgrep dump --topic orders-crud \
  --from-time 2026-04-13T00:00:00Z \
  --to-time 2026-04-14T00:00:00Z \
  --output-csv day_export.csv
```

**Keep a live view while archiving to CSV at the same time:**

```sh
kgrep consume --topic orders-crud --read-all --output-csv archive.csv
```

**See everything kgrep is doing behind the scenes** (useful when something looks wrong and you want more detail — this extra detail is kept separate from your actual results):

```sh
kgrep consume --topic orders-crud --read-all --verbose
```

For the full list of commands and options, see [docs/commands.md](docs/commands.md), [docs/matching.md](docs/matching.md) (how searching works), and [docs/decoding.md](docs/decoding.md) (how message formats are handled).

---

## Configuration

kgrep is configured entirely through environment variables — usually via a `.env` file, so you never have to type secrets on the command line (which would leave them visible in your terminal history).

| Setting | What it means |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | The address(es) of your Kafka cluster. Ask your Kafka administrator. Multiple addresses are comma-separated. |
| `KAFKA_SECURITY_PROTOCOL` | How to connect securely. One of `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` — ask your administrator which one your cluster uses. |
| `KAFKA_SASL_MECHANISM` | Only needed for `SASL_SSL`/`SASL_PLAINTEXT`. One of `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | Your Kafka login credentials. |
| `KAFKA_DEFAULT_TOPIC` | The topic to use when you don't pass `--topic` on the command line. |
| `SCHEMA_REGISTRY_URL` | Only needed if your messages are Avro-encoded. The address of your Schema Registry. |
| `SCHEMA_REGISTRY_USERNAME` / `SCHEMA_REGISTRY_PASSWORD` | Only needed if Schema Registry requires a login. |

A ready-to-copy template is in [.env.example](.env.example) — copy it to `.env` and fill in your real values.

### Using more than one environment (dev / qa / prod)

If you regularly switch between environments, keep the shared settings in `.env` and put anything that differs (like `KAFKA_BOOTSTRAP_SERVERS`) into a second file named `.env.qa` (or `.env.prod`, etc.). Then add `--profile qa` right after `kgrep`, before the command:

```sh
kgrep --profile qa print --read-all
```

kgrep loads `.env` first, then overlays `.env.qa` on top of it.

---

## Installing in detail

### Windows (PowerShell)

```powershell
Invoke-WebRequest -Uri https://github.com/arunherga/kgrep/releases/latest/download/kgrep-windows-amd64.exe -OutFile kgrep.exe
Invoke-WebRequest -Uri https://github.com/arunherga/kgrep/releases/latest/download/checksums.txt -OutFile checksums.txt

# Optional but recommended: verify the file wasn't corrupted or tampered with
$expected = ((Select-String -Path checksums.txt -Pattern 'kgrep-windows-amd64\.exe$' | Select-Object -First 1).Line -split '\s+')[0]
if ((Get-FileHash .\kgrep.exe -Algorithm SHA256).Hash.ToLower() -ne $expected) {
    throw "checksum mismatch"
}

.\kgrep.exe --version
```

You can now run it as `.\kgrep.exe` from this folder. To run it as just `kgrep` from anywhere, move `kgrep.exe` into a folder that's already on your `PATH`.

### macOS / Linux

```sh
# Replace kgrep-linux-amd64 with the file name matching your computer (see the table above)
curl -fLO https://github.com/arunherga/kgrep/releases/latest/download/kgrep-linux-amd64
curl -fLO https://github.com/arunherga/kgrep/releases/latest/download/checksums.txt

# Optional but recommended: verify the file wasn't corrupted or tampered with
sha256sum -c checksums.txt --ignore-missing
# macOS doesn't have sha256sum by default — use this instead:
# shasum -a 256 -c checksums.txt --ignore-missing

chmod +x kgrep-linux-amd64
sudo mv kgrep-linux-amd64 /usr/local/bin/kgrep

kgrep --version
```

On macOS, the first run may be blocked by Gatekeeper since the binary isn't notarized. If so, run:

```sh
xattr -d com.apple.quarantine /usr/local/bin/kgrep
```

### Getting a specific version instead of the latest

Every release is also available individually. Replace `latest` in the URLs above with a version tag, e.g. `.../releases/download/v1.0.0/kgrep-linux-amd64`, to install that exact version instead of always tracking the newest one.

---

## Documentation

- [docs/commands.md](docs/commands.md) — the three commands in depth, output columns, the scan summary
- [docs/matching.md](docs/matching.md) — how searching for values works, field paths, the `delivery-identifiers` shortcut
- [docs/decoding.md](docs/decoding.md) — how message formats are detected, Avro/Schema Registry behavior, what makes a record "bad"
- [docs/architecture.md](docs/architecture.md) — for contributors changing the code: package responsibilities, data flow, key invariants

## For developers

Building from source requires Go 1.25 or newer.

```sh
go test -race ./...
go vet ./...
go build -trimpath -o bin/kgrep ./cmd/kgrep
```

Every tag pushed as `vX.Y.Z` on `main` automatically builds and publishes binaries for Linux, macOS, and Windows via [GitHub Actions](.github/workflows/release.yml).

## Compatibility notes

- Kafka security protocols `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` are recognized. SASL mechanisms `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` are supported.
- Avro payloads must use Confluent's five-byte wire header. The schema ID in each message selects the writer schema; `--verbose` additionally reports the standard `<topic>-key` or `<topic>-value` subject's latest version.
- `--max-messages 0` means unlimited. Negative limits and invalid time ranges are rejected early.
- Deserialization failures never stop a scan — a record is counted "good" only if both its key and value decode successfully; failures are reported with their exact topic/partition/offset location and counted separately. See [docs/decoding.md](docs/decoding.md) for details.
