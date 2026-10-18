//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/rebuild"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

const (
	meterTopic     = "iot.meter-readings"
	heartbeatTopic = "iot.heartbeats"
)

type env struct {
	dsn    string
	pg     *postgresstore.Store
	rd     *redisstore.Store
	router *ingestion.Router
}

// newEnv migrates a fresh database, registers one house (the registration
// flow is out of scope for this service, so the test plays that part) and wires
// the real handlers to the real stores.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()

	dsn := newTimescale(t)
	if _, err := gooseProvider(t, dsn).Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := openDB(t, dsn).ExecContext(ctx, `
		INSERT INTO iot_data.houses (house_id, grid_id, device_class, meter_id)
		VALUES ('grid01-house0042', 'grid01', 'residential_prosumer', 'meter-grid01-house0042')`); err != nil {
		t.Fatal(err)
	}

	pg, err := postgresstore.NewWithPool(ctx, dsn, postgresstore.PoolConfig{MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)

	rd, err := redisstore.New(ctx, redisstore.Config{Address: newRedis(t), MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rd.Close() })

	registry := admission.NewRegistry()
	if err := admission.NewRefresher(registry, pg, admission.RefresherConfig{}, quietLogger()).Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}

	router := ingestion.NewRouter()
	router.Register(meterTopic, ingestion.NewMeterHandler(registry, pg, rd, quietLogger()))
	router.Register(heartbeatTopic, ingestion.NewHeartbeatHandler(registry, pg, rd, quietLogger()))
	return &env{dsn: dsn, pg: pg, rd: rd, router: router}
}

func meterJSON(houseNum int, seq int, at time.Time, net float64) string {
	house := fmt.Sprintf("grid01-house%04d", houseNum)
	return fmt.Sprintf(`{"schema_version":"1.0","meter_id":"meter-%s","house_id":%q,"grid_id":"grid01",
	"device_class":"residential_prosumer","timestamp":%q,"seq":%d,
	"readings":{"solar_kw":2.5,"consumption_kw":1,"net_kw":%v,
	"storage_assets":[{"asset_id":"bat_%04d","asset_type":"bess","soc_pct":50,"power_kw":-0.5,"capacity_kwh":10}]}}`,
		house, house, at.UTC().Format(time.RFC3339), seq, net, houseNum)
}

func heartbeatJSON(houseNum int, gridID, asset string, capacity float64) string {
	house := fmt.Sprintf("grid01-house%04d", houseNum)
	return fmt.Sprintf(`{"schema_version":"1.0","grid_id":%q,"house_id":%q,"meter_id":"meter-%s","status":"online",
	"device_class":"residential_prosumer","rated_solar_kw":5,
	"flexible_assets":[{"asset_id":%q,"asset_type":"bess","capacity_kwh":%v,"max_charge_kw":3.5,"max_discharge_kw":3.5}]}`,
		gridID, house, house, asset, capacity)
}

func route(e *env, topic, payload string) error {
	return e.router.Route(context.Background(), topic, nil, []byte(payload))
}

func TestHeartbeatPersistsLivenessAndProjectsToRedis(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	if err := route(e, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 10)); err != nil {
		t.Fatal(err)
	}

	if got := scalar[bool](t, e.dsn, `SELECT last_heartbeat_at IS NOT NULL FROM iot_data.houses WHERE house_id='grid01-house0042'`); !got {
		t.Error("durable liveness was not recorded")
	}
	// A heartbeat discovers the asset but never registers it.
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.flexible_assets`); n != 0 {
		t.Errorf("heartbeat inserted %d registered asset(s); it must only record a discovery", n)
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.discovered_assets WHERE asset_id='bat_0042'`); n != 1 {
		t.Errorf("discovered assets = %d, want 1", n)
	}

	status, err := e.rd.GetHouseStatus(ctx, "grid01-house0042")
	if err != nil || status.Status != "online" {
		t.Fatalf("redis status = %+v, err = %v", status, err)
	}
	ids, _ := e.rd.GridHouseIDs(ctx, "grid01")
	if len(ids) != 1 || ids[0] != "grid01-house0042" {
		t.Errorf("grid membership = %v", ids)
	}

	// Redis and Postgres carry the same receive timestamp.
	pgAt := scalar[time.Time](t, e.dsn, `SELECT last_heartbeat_at FROM iot_data.houses WHERE house_id='grid01-house0042'`)
	if d := pgAt.Sub(status.LastHeartbeatAt).Abs(); d > time.Second {
		t.Errorf("postgres %v and redis %v differ by %v", pgAt, status.LastHeartbeatAt, d)
	}
}

func TestHeartbeatIdentityFailuresWriteNothing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{"unregistered house", heartbeatJSON(7, "grid01", "bat_0007", 10), domain.ErrHouseIdentity},
		{"unknown grid", strings.NewReplacer("grid01", "grid99").Replace(heartbeatJSON(42, "grid01", "bat_0042", 10)), admission.ErrUnknownGrid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := route(e, heartbeatTopic, tt.payload)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}

	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.houses WHERE house_id='grid01-house0007'`); n != 0 {
		t.Error("a heartbeat created a house; houses must come from the registration flow")
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.discovered_assets`); n != 0 {
		t.Errorf("rejected heartbeats left %d discovered asset(s)", n)
	}
	if _, err := e.rd.GetHouseStatus(ctx, "grid01-house0007"); !errors.Is(err, redisstore.ErrNotFound) {
		t.Error("redis was written for a rejected heartbeat")
	}
}

func TestAssetOwnershipConflictRollsBackWholeHeartbeat(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := openDB(t, e.dsn).ExecContext(ctx, `
		INSERT INTO iot_data.houses (house_id, grid_id, device_class, meter_id)
		VALUES ('grid01-house0043', 'grid01', 'consumer', 'meter-grid01-house0043')`); err != nil {
		t.Fatal(err)
	}

	// house 42 observes bat_X first; house 43 then claims the same asset.
	if err := route(e, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_X", 10)); err != nil {
		t.Fatal(err)
	}
	liveness := func() time.Time {
		return scalar[time.Time](t, e.dsn, `SELECT last_heartbeat_at FROM iot_data.houses WHERE house_id='grid01-house0043'`)
	}
	before := liveness()
	time.Sleep(10 * time.Millisecond)
	err := route(e, heartbeatTopic, heartbeatJSON(43, "grid01", "bat_X", 10))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("error = %v, want ErrIntegrityConflict", err)
	}
	if after := liveness(); !after.Equal(before) {
		t.Errorf("house liveness moved %v -> %v although the asset conflict should have rolled the transaction back", before, after)
	}
	if owner := scalar[string](t, e.dsn, `SELECT observed_house_id FROM iot_data.discovered_assets WHERE asset_id='bat_X'`); owner != "grid01-house0042" {
		t.Errorf("asset owner = %q, it was silently reassigned", owner)
	}
}

func TestOnlyChangedCapabilitiesAreRewritten(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	if err := route(e, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 10)); err != nil {
		t.Fatal(err)
	}
	xmin := func() string {
		return scalar[string](t, e.dsn, `SELECT xmin::text FROM iot_data.discovered_assets WHERE asset_id='bat_0042'`)
	}
	before := xmin()

	if err := route(e, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 10)); err != nil {
		t.Fatal(err)
	}
	if after := xmin(); after != before {
		t.Errorf("unchanged heartbeat rewrote the asset row (xmin %s -> %s)", before, after)
	}

	if err := route(e, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 12)); err != nil {
		t.Fatal(err)
	}
	if c := scalar[float64](t, e.dsn, `SELECT capacity_kwh FROM iot_data.discovered_assets WHERE asset_id='bat_0042'`); c != 12 {
		t.Errorf("changed capacity = %v, want 12", c)
	}
	_ = ctx
}

func TestMeterReadingIsImmutableAndReplaySafe(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Second)

	payload := meterJSON(42, 100, at, 1.5)
	for i := 0; i < 3; i++ { // at-least-once: the same record delivered repeatedly
		if err := route(e, meterTopic, payload); err != nil {
			t.Fatal(err)
		}
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.meter_readings`); n != 1 {
		t.Errorf("readings = %d after 3 deliveries, want 1", n)
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.storage_asset_readings`); n != 1 {
		t.Errorf("storage readings = %d after 3 deliveries, want 1", n)
	}

	// Same identity, different values: an integrity conflict, never an update.
	err := route(e, meterTopic, meterJSON(42, 100, at, 9.9))
	if !errors.Is(err, domain.ErrIntegrityConflict) {
		t.Fatalf("conflicting duplicate: error = %v, want ErrIntegrityConflict", err)
	}
	if net := scalar[float64](t, e.dsn, `SELECT net_kw FROM iot_data.meter_readings`); net != 1.5 {
		t.Errorf("history was rewritten: net_kw = %v", net)
	}

	// An older reading arriving late must not regress Redis latest state.
	if err := route(e, meterTopic, meterJSON(42, 101, at.Add(5*time.Second), 2.5)); err != nil {
		t.Fatal(err)
	}
	if err := route(e, meterTopic, meterJSON(42, 99, at.Add(-5*time.Second), 0.5)); err != nil {
		t.Fatal(err)
	}
	latest, err := e.rd.GetLatest(ctx, "grid01", "grid01-house0042")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Seq != 101 || latest.NetKw != 2.5 {
		t.Errorf("latest = seq %d net %v, want seq 101 net 2.5", latest.Seq, latest.NetKw)
	}
}

func TestMeterReadingFromUnknownGridTouchesNothing(t *testing.T) {
	e := newEnv(t)
	bad := strings.ReplaceAll(meterJSON(42, 1, time.Now(), 1.5), "grid01", "grid99")
	if err := route(e, meterTopic, bad); !errors.Is(err, admission.ErrUnknownGrid) {
		t.Fatalf("error = %v, want ErrUnknownGrid", err)
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.meter_readings`); n != 0 {
		t.Error("an unknown grid's reading reached the telemetry table")
	}
}

func TestRedisRebuildRestoresProjectionFromTimescale(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Second)

	if err := route(e, heartbeatTopic, heartbeatJSON(42, "grid01", "bat_0042", 10)); err != nil {
		t.Fatal(err)
	}
	if err := route(e, meterTopic, meterJSON(42, 7, at, 1.5)); err != nil {
		t.Fatal(err)
	}
	// Redis loss.
	if err := e.rd.FlushForTest(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.rd.GetLatest(ctx, "grid01", "grid01-house0042"); !errors.Is(err, redisstore.ErrNotFound) {
		t.Fatal("redis was not emptied")
	}

	res, err := rebuild.Run(ctx, e.pg, e.rd, rebuild.Options{PageSize: 10}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if res.Houses != 1 || res.StatusRestored != 1 || res.LatestApplied != 1 {
		t.Fatalf("rebuild result %+v", res)
	}

	latest, err := e.rd.GetLatest(ctx, "grid01", "grid01-house0042")
	if err != nil || latest.Seq != 7 || len(latest.StorageAssets) != 1 {
		t.Fatalf("rebuilt latest = %+v, err = %v", latest, err)
	}
	if st, err := e.rd.GetHouseStatus(ctx, "grid01-house0042"); err != nil || st.Status != "online" {
		t.Fatalf("rebuilt status = %+v, err = %v", st, err)
	}
	if ids, _ := e.rd.GridHouseIDs(ctx, "grid01"); len(ids) != 1 {
		t.Fatalf("rebuilt membership = %v", ids)
	}

	// A second rebuild is a harmless no-op for latest state.
	again, err := rebuild.Run(ctx, e.pg, e.rd, rebuild.Options{}, quietLogger())
	if err != nil || again.LatestApplied != 0 {
		t.Fatalf("second rebuild applied %d latest readings, err = %v; want 0", again.LatestApplied, err)
	}
}

func TestFailureReplayLifecycle(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	at := time.Now().UTC()
	payload := meterJSON(42, 5, at, 1.5)

	if err := e.pg.RecordFailure(ctx, ingestion.IngestionFailure{
		Topic: meterTopic, Partition: 0, Offset: 11, RawPayload: []byte(payload),
		PayloadSHA256: "abc", Stage: ingestion.StageGridValidation, Reason: "was unknown grid", Attempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := e.pg.ListUnresolvedFailures(ctx, 0, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list = %v, err = %v", rows, err)
	}
	id := rows[0].ID

	// A failing replay leaves the row unresolved and notes the new error.
	_, res, err := e.pg.WithLockedFailure(ctx, id, "alice", func(f postgresstore.FailureRow) (string, error) {
		return "", errors.New("still broken")
	})
	if err != nil || res != "" {
		t.Fatalf("failed replay: res=%q err=%v", res, err)
	}
	if reason := scalar[string](t, e.dsn, `SELECT error_reason FROM iot_data.ingestion_failures WHERE id=$1`, id); reason != "still broken" {
		t.Errorf("reason = %q", reason)
	}

	// Concurrent replay of one failure is prevented by the row lock.
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, _, err := e.pg.WithLockedFailure(ctx, id, "alice", func(f postgresstore.FailureRow) (string, error) {
			close(locked)
			<-release
			return "", nil
		})
		done <- err
	}()
	<-locked
	_, _, err = e.pg.WithLockedFailure(ctx, id, "bob", func(postgresstore.FailureRow) (string, error) {
		t.Error("second operator entered the critical section")
		return "", nil
	})
	if !errors.Is(err, postgresstore.ErrFailureUnavailable) {
		t.Errorf("concurrent replay: error = %v, want ErrFailureUnavailable", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Real replay through the idempotent handler.
	row, res, err := e.pg.WithLockedFailure(ctx, id, "alice", func(f postgresstore.FailureRow) (string, error) {
		if err := e.router.Route(ctx, f.Topic, f.Key, f.RawPayload); err != nil {
			return "", err
		}
		return "replayed", nil
	})
	if err != nil || res != "replayed" {
		t.Fatalf("replay: res=%q err=%v row=%+v", res, err, row)
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.meter_readings`); n != 1 {
		t.Errorf("readings after replay = %d, want 1", n)
	}
	if by := scalar[string](t, e.dsn, `SELECT resolved_by FROM iot_data.ingestion_failures WHERE id=$1`, id); by != "alice" {
		t.Errorf("resolved_by = %q", by)
	}

	// A resolved failure cannot be replayed again.
	_, _, err = e.pg.WithLockedFailure(ctx, id, "alice", func(postgresstore.FailureRow) (string, error) { return "replayed", nil })
	if !errors.Is(err, postgresstore.ErrFailureUnavailable) {
		t.Errorf("replay of a resolved failure: error = %v, want ErrFailureUnavailable", err)
	}

	count, _, err := e.pg.FailureBacklog(ctx)
	if err != nil || count != 0 {
		t.Errorf("backlog = %d, err = %v; want 0", count, err)
	}
}
