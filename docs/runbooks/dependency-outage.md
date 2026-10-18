# Runbook: dependency outage

Baseline behaviour (11-production-readiness.md). Nothing here requires a manual offset change: the consumer never commits a record it did not finish.

| Dependency | What the service does | What to check | Recovery |
|---|---|---|---|
| **TimescaleDB** down | Handlers fail transiently, retry with backoff, then cannot record the failure either, so the consumer **stops without committing**. Lag grows; no data is lost. `/healthz` stays up; `iot_ingestion_dependency_up{dependency="postgres"} = 0`. | Database health, `POSTGRES_URL`, pool metrics, connection limit, disk. | Restore the database. The consumer fails fast and the orchestrator restarts it; it resumes from the last committed offset. Replays are idempotent. |
| **Redis** down, `REDIS_ACK_POLICY=required` | Records are written to TimescaleDB, then fail at Redis and stay uncommitted. Consumption effectively pauses; lag grows. | `iot_ingestion_dependency_up{dependency="redis"}`, memory, eviction policy. | Restore Redis. Records retry and succeed. No rebuild needed. |
| **Redis** down, `REDIS_ACK_POLICY=async-repair` | Ingestion continues; `iot_ingestion_cache_degraded_total` increases. Latest-state and liveness in Redis go stale. | Same as above. | Restore Redis, then run the [rebuild](redis-rebuild.md). New readings repair their own house on arrival; the rebuild fixes quiet houses. |
| **Kafka** unavailable | `kafka fetch error` logs, no new records. The query side (if any) stays up. | Broker health, `KAFKA_BROKER`, TLS/SASL credentials, topic ACLs. | Franz-go reconnects on its own. Alert on lag after it returns. |
| **Grid refresh** failing after startup | The last known-good snapshot stays in use; `iot_ingestion_grid_registry_refreshes_total{outcome="error"}` increases. | Age of `iot_ingestion_grid_registry_last_successful_refresh_timestamp_seconds`; database reachability. | Fix the database. Newly provisioned grids are quarantined until a refresh succeeds, then [replayed](failure-replay.md). |

## Startup failures

- `database migration version X does not match expected version Y`: run `migrate up` as the migration owner **before** starting replicas. Never let replicas migrate. During an expand/contract rollout, `POSTGRES_EXPECTED_MIGRATION` pins the version old replicas accept.
- Stuck or failed migration: goose runs each migration in a transaction, so a failed migration leaves the previous version in place. Fix the cause and re-run `migrate up`. Check `migrate status` first. Do not edit the `goose_db_version` table by hand.
- `grid registry bootstrap: ... empty`: no rows in `iot_data.grids`. Migration `00010` provisions the simulator grids; production grids need their own migration.
- `redis maxmemory-policy ... can evict liveness/latest keys` (production only): set `maxmemory-policy noeviction` or a `volatile-*` policy on the server.

## Credential rotation

Secrets come from the deployment secret mechanism and are read at process start, so **rotation needs a rolling restart**. Rotate one dependency at a time: add the new credential alongside the old, roll the replicas, then revoke the old one. The migration role and the runtime role are separate; rotate them independently.
