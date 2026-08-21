# Matching

Filtering works by supplying a set of *allowed values* (via `--allowed-keys-csv`) and a `--match-mode` that decides where in each record `kgrep` looks for them.

## The allowed-values file

`--allowed-keys-csv` points at a CSV file. Every non-empty cell across every row and column is added to the allowed-values set, with **no header-row awareness** — there's no fixed column layout, so you can keep the values in one column or several, but the loader has no concept of a title row and will add one literally if you include it:

```csv
order-42
ALT-9001
order-43
```

This loads `{"order-42", "ALT-9001", "order-43"}` as the allowed set. If you added a header row (e.g. `order_id,alt_id`), the header cells themselves would also be added to the set — harmless in practice, since they're unlikely to match anything in your actual data, but worth knowing so you don't mistake it for a bug.

The file is parsed by a real CSV reader, not split on newlines, so comma-separated values on a single line work exactly like one-per-line — `order-42,order-43` and two separate lines `order-42` / `order-43` load identically. You can mix both styles in the same file.

### Matching is always by exact text

CSV cells have no type — they're just strings. On the Kafka side, whatever kgrep decodes (a raw key, a JSON number, a boolean, `null`, an Avro field, …) is converted to text before comparison, the same way every time:

- `null` → `None`
- `true` / `false` → `True` / `False`
- everything else → its printed form — a JSON number keeps the exact digits it had in the source payload (e.g. `123.450` stays `123.450`, not renormalized to `123.45`)

There's no numeric-aware matching on top of that — `123` and `123.0` are different strings and won't match each other. Put the value in your CSV exactly as it appears in the actual Kafka data (as text), regardless of whether the underlying field is a string, number, or boolean.

## Match modes (`--match-mode`)

| Mode | Matches against |
|---|---|
| `kafka-key` | The raw Kafka key, as text (not decoded) |
| `decoded-key-field` | The decoded key payload |
| `value-field` | The decoded value payload |
| `key-or-value` (default) | Either the raw key, the decoded key, or the decoded value |
| `key-and-value` | Requires a hit on the key side (raw *or* decoded) **and** a hit on the value side |

"Decoded key/value" here means whatever `--key-format`/`--value-format` produced — see [decoding.md](decoding.md). The raw key is always available as plain text regardless of key format, which is why `kafka-key` mode exists separately from `decoded-key-field`.

## Where in the payload it looks

By default (no `--key-field`/`--value-field`), matching is **recursive**: it walks the entire decoded key or value — every map key, every list element, every nested object — and checks each map key and each scalar leaf value against the allowed set. This is the right default when you don't know exactly where a value lives in the payload, or when it could appear in more than one place.

`--key-field` / `--value-field` (repeatable) instead restrict the search to specific dot-separated paths, e.g.:

```sh
kgrep consume --topic orders-crud \
  --allowed-keys-csv order_ids.csv \
  --match-mode value-field \
  --value-field Order.OrderId \
  --value-field Order.Customer.Id
```

A path is followed through both objects and lists transparently — `Manifest.Pallets.Deliveries.Order` will collect the `Order` field from every element of a `Deliveries` list nested inside every element of a `Pallets` list. If any segment of the path is missing at some point in the payload, that branch simply yields nothing (not an error).

Giving explicit fields **narrows** the search to just those paths; omitting them **widens** it to the whole payload. They aren't a fallback chain — you get one or the other, not both, for a given side.

## The `delivery-identifiers` shortcut

`--field-shortcut delivery-identifiers` is equivalent to passing:

```
--value-field Manifest.Pallets.Deliveries.DeliveryDocument
--value-field Manifest.Pallets.Deliveries.Order
```

It exists because this pair of paths is common enough in the payloads this tool was built against to be worth a name. It only adds value-field paths, and combines with any other explicit `--value-field` flags you also pass (duplicates are deduplicated).

## Reading the output

- `matched_values` (a CSV/print column) lists every allowed-value string that matched, from any side, deduplicated and sorted.
- `matched_fields` breaks that down by *where* the match came from: `kafka-key`, `decoded-key`, `value`, or (when you passed explicit `--key-field`/`--value-field` paths) `key.<path>` / `value.<path>` for each path that had a hit.
- A record can be counted "emitted" (see [commands.md](commands.md#the-scan-summary)) only if `Matched` is true under the chosen mode — `matched_values`/`matched_fields` show *why*.
