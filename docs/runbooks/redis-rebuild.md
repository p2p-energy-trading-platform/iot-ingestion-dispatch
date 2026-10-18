# Runbook: Redis rebuild

Redis is a rebuildable projection of TimescaleDB. Rebuild after Redis data loss, a failover to an empty replica, an accidental `FLUSHDB`, or a period under `REDIS_ACK_POLICY=async-repair` with Redis down.

## What it rebuilds

| Key | Source |
|---|---|
| `grid:{grid}:houses` | Every row of `iot_data.houses` |
| `house:{house}:status` | Houses whose `last_heartbeat_at` is still inside the 10 minute liveness window, with only the **remaining** TTL. A house is never made to look more recently alive than it was |
| `meter:{grid}:{house}:latest` | Newest reading per house within `-lookback` (default 1h), written with the same compare-and-set as live ingestion |

Quiet houses with no reading inside the lookback have no latest key; that is correct, not an error.

## Run

```bash
redis-rebuild -dry-run                       # counts only, no Redis writes
redis-rebuild                                # lookback 1h, 500 rows per query
redis-rebuild -lookback 6h -page-size 200
```

It uses the service's `POSTGRES_URL` / `REDIS_*` settings, a small fixed pool, keyset-paginated queries and an overall `-timeout` (default 10m), so it cannot scan history or exhaust connections.

**It is safe while ingestion is running.** Every write is idempotent and newest-wins: it cannot overwrite a newer reading the consumer already stored. Run it once Redis is healthy again; under `async-repair`, also after the outage ends.

## Verify

- Output line `redis rebuild finished` with `houses`, `status_restored`, `latest_applied`.
- `redis-cli SCARD grid:grid01:houses` matches the number of houses in that grid.
- `redis-cli TTL house:<id>:status` is positive for recently-alive houses.
- `iot_ingestion_cache_degraded_total` stops increasing.

## Expected duration

Roughly proportional to the number of houses (a few queries and Redis calls per 500 houses). Measure it in the load-test environment and record the number here; the production figure is an acceptance criterion in 11-production-readiness.md.
