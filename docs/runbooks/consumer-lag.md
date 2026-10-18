# Runbook: consumer lag

**Alert:** sustained growth of consumer-group lag on `iot.meter-readings` or `iot.heartbeats`, or no `iot_ingestion_records_total{result="ok"}` increase for 5 minutes.
**Severity:** page when lag grows for 10+ minutes; ticket otherwise.

## Triage

1. **Is the service up and consuming?**
   - `GET /readyz` on the health port; `kubectl get pods` / `docker compose ps`.
   - Logs: `partition assigned` lines after a restart; repeated `kafka fetch error` means a broker problem (see below).
2. **Is a dependency slow or down?** `iot_ingestion_dependency_up{dependency="postgres|redis"}` and `iot_ingestion_postgres_pool_acquired_conns` against `..._max_conns`. If the pool is saturated and `..._empty_acquires` climbs, PostgreSQL is the bottleneck. Go to [dependency-outage.md](dependency-outage.md).
3. **Is one partition stuck?** The consumer is sequential per partition. A single poison record is recorded and skipped, so a stuck partition means a handler that cannot finish: look for `transient ingestion failure; backing off` repeating with the same offset, which points at a dependency, not the record.
4. **Is failure recording failing?** If `failure recorder` errors appear, the consumer stops rather than skip a record (by design). Restore PostgreSQL; the service resumes from the uncommitted offset.
5. **Rebalance storm?** `iot_ingestion_rebalance_events_total` rising quickly means replicas are flapping. Check `KAFKA_SESSION_TIMEOUT` against handler duration (`iot_ingestion_record_processing_seconds`) and pod restarts.

## Catching up

- Lag drains at the sustainable rate. Add replicas **only up to the partition count** of the topic; beyond that they idle.
- Raise `INGESTION_MAX_CONCURRENT_HANDLERS` and `POSTGRES_MAX_CONNS` together (a handler holds one connection while running). Keep `POSTGRES_MAX_CONNS` below the database's connection budget across all replicas.
- Never reset offsets to skip lag: that silently drops telemetry. If data must be abandoned, that is a product decision with an owner, not an operator shortcut.
- Kafka retention must exceed the outage plus catch-up time; check it before a long incident ends.

## After recovery

Confirm `iot_ingestion_unresolved_failures` did not jump, and spot-check that the latest readings in Redis are current (`redis-rebuild -dry-run` shows what Postgres has).
