# Runbook: ingestion failure replay

Permanently failed records are stored in `iot_data.ingestion_failures` and their Kafka offsets committed, so a bad record never blocks a partition. Raw payloads can contain device identifiers: restrict database access, and do not paste payloads into tickets or chat. Logs and `list` output show only the SHA-256 and Kafka coordinates.

## Alert

`iot_ingestion_unresolved_failures` rising, or `iot_ingestion_oldest_unresolved_failure_age_seconds` above the agreed bound. Unresolved rows are never purged automatically.

## Inspect

```bash
failure-replay list -limit 50          # unresolved rows, oldest id first
failure-replay list -after 1200        # next page
```

The stage tells you the cause:

| Stage | Meaning | Usual action |
|---|---|---|
| `grid_validation` | Unknown grid, or a house/meter identity that is not registered | Provision the grid or register the house, wait for a registry refresh (up to `ADMISSION_REFRESH_INTERVAL`, default 120s), then replay |
| `decode` | Malformed payload, unsupported `schema_version`, out-of-range value, oversized record, key/house mismatch | Usually `discard` (fix the publisher). Replay only if the contract changed to accept it |
| `timescale_write` | Retries exhausted against the database, or an immutable-history conflict | Check for a data-integrity problem first. A conflict means two different payloads share one identity; do not force it |
| `redis_write` | Retries exhausted against Redis | Replay after Redis is healthy |
| `handler_panic` | A bug; the stack is in the logs | Fix and deploy, then replay |

## Replay

```bash
failure-replay replay -actor alice -id 1201 -id 1202
```

- IDs are explicit and capped at 100 per run. There is no "replay all".
- Each payload is **revalidated by the current decoder** and written through the same idempotent handlers as live traffic, so replaying twice cannot duplicate history or regress Redis latest state.
- The row is locked while it is replayed; a concurrent replay of the same failure is skipped.
- On success the row becomes `replayed` with `resolved_at` and `resolved_by`. On failure it stays `unresolved`, with the new error and an incremented attempt count.
- Kafka offsets are never touched.

## Discard

```bash
failure-replay discard -actor alice -id 1203
```

Marks the row `discarded` with actor and time. Use only when the record is known to be invalid and the publisher is fixed. Note the reason in the incident ticket.

## Retention

Resolved rows may be purged under the audit policy agreed with the data owner (open decision: resolved-failure retention). Unresolved rows are kept until acted on.
