package rebuild

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

type fakeSource struct {
	houses   []postgres.HouseRow
	readings []domain.MeterReading
	pages    int
}

func (f *fakeSource) HousesPage(_ context.Context, after string, limit int) ([]postgres.HouseRow, error) {
	f.pages++
	var out []postgres.HouseRow
	for _, h := range f.houses {
		if h.HouseID > after && len(out) < limit {
			out = append(out, h)
		}
	}
	return out, nil
}

func (f *fakeSource) LatestReadingsPage(_ context.Context, _ time.Time, after string, limit int) ([]domain.MeterReading, error) {
	var out []domain.MeterReading
	for _, r := range f.readings {
		if r.HouseID > after && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeTarget struct {
	members  map[string]string
	restored map[string]time.Time
	latest   map[string]redisstore.MeterProjection
	err      error
}

func newTarget() *fakeTarget {
	return &fakeTarget{members: map[string]string{}, restored: map[string]time.Time{}, latest: map[string]redisstore.MeterProjection{}}
}

func (f *fakeTarget) SetLatestIfNewer(_ context.Context, p redisstore.MeterProjection) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	f.latest[p.HouseID] = p
	return true, nil
}

func (f *fakeTarget) RestoreHeartbeat(_ context.Context, _, houseID string, s redisstore.HouseStatus, now time.Time) (bool, error) {
	if now.Sub(s.LastHeartbeatAt) >= redisstore.HeartbeatTTL {
		return false, nil
	}
	f.restored[houseID] = s.LastHeartbeatAt
	return true, nil
}

func (f *fakeTarget) AddHouseToGrid(_ context.Context, gridID, houseID string) error {
	f.members[houseID] = gridID
	return nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRunRebuildsMembershipStatusAndLatest(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	recent, stale := now.Add(-time.Minute), now.Add(-30*time.Minute)
	src := &fakeSource{
		houses: []postgres.HouseRow{
			{HouseID: "h1", GridID: "g1", LastHeartbeatAt: &recent},
			{HouseID: "h2", GridID: "g1", LastHeartbeatAt: &stale},
			{HouseID: "h3", GridID: "g2"},
		},
		readings: []domain.MeterReading{
			{HouseID: "h1", GridID: "g1", MeterID: "m1", EventTime: now, Seq: 7},
			{HouseID: "h3", GridID: "g2", MeterID: "m3", EventTime: now, Seq: 2},
		},
	}
	dst := newTarget()

	res, err := Run(context.Background(), src, dst, Options{PageSize: 2, Now: func() time.Time { return now }}, quiet)
	if err != nil {
		t.Fatal(err)
	}

	if res.Houses != 3 || res.Memberships != 3 {
		t.Fatalf("houses/memberships = %d/%d, want 3/3", res.Houses, res.Memberships)
	}
	if len(dst.members) != 3 {
		t.Fatalf("members = %v, want all three houses", dst.members)
	}
	if _, ok := dst.restored["h1"]; !ok || len(dst.restored) != 1 {
		t.Fatalf("restored = %v, want only the recently-seen house", dst.restored)
	}
	if res.StatusRestored != 1 || res.LatestApplied != 2 || dst.latest["h1"].Seq != 7 {
		t.Fatalf("result %+v / latest %+v unexpected", res, dst.latest)
	}
	if src.pages < 2 {
		t.Fatalf("houses were read in %d page(s); a page size of 2 must paginate", src.pages)
	}
}

func TestRunDryRunWritesNothing(t *testing.T) {
	now := time.Now()
	src := &fakeSource{
		houses:   []postgres.HouseRow{{HouseID: "h1", GridID: "g1", LastHeartbeatAt: &now}},
		readings: []domain.MeterReading{{HouseID: "h1", GridID: "g1", EventTime: now}},
	}
	dst := newTarget()

	res, err := Run(context.Background(), src, dst, Options{DryRun: true}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if res.Houses != 1 || res.Latest != 1 {
		t.Fatalf("dry run should still count: %+v", res)
	}
	if len(dst.members)+len(dst.restored)+len(dst.latest) != 0 {
		t.Fatal("dry run wrote to the target")
	}
}

func TestRunStopsOnTargetError(t *testing.T) {
	now := time.Now()
	src := &fakeSource{readings: []domain.MeterReading{{HouseID: "h1", GridID: "g1", MeterID: "m1", EventTime: now}}}
	dst := newTarget()
	dst.err = errors.New("redis down")

	if _, err := Run(context.Background(), src, dst, Options{}, quiet); !errors.Is(err, dst.err) {
		t.Fatalf("error = %v, want the redis error", err)
	}
}
