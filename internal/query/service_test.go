package query

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

type fakeHouses struct {
	rows       []postgresstore.HouseInfoRow
	assets     []postgresstore.FlexibleAssetRow
	gotFilter  postgresstore.HouseFilter
	listCalled bool
}

func (f *fakeHouses) ListHouses(_ context.Context, filter postgresstore.HouseFilter) ([]postgresstore.HouseInfoRow, error) {
	f.listCalled = true
	f.gotFilter = filter
	if len(f.rows) > filter.Limit {
		return f.rows[:filter.Limit], nil
	}
	return f.rows, nil
}

func (f *fakeHouses) GetHouse(_ context.Context, houseID string) (postgresstore.HouseInfoRow, []postgresstore.FlexibleAssetRow, error) {
	for _, r := range f.rows {
		if r.HouseID == houseID {
			return r, f.assets, nil
		}
	}
	return postgresstore.HouseInfoRow{}, nil, fmt.Errorf("house %q: %w", houseID, domain.ErrNotFound)
}

type fakeEnergy struct {
	buckets   []postgresstore.EnergyBucketRow
	summary   postgresstore.EnergySummaryRow
	gotTZ     string
	gotWidth  time.Duration
	callCount int
}

func (f *fakeEnergy) EnergySeries(_ context.Context, _ string, _, _ time.Time, width time.Duration, tz string) ([]postgresstore.EnergyBucketRow, error) {
	f.callCount++
	f.gotTZ = tz
	f.gotWidth = width
	return f.buckets, nil
}

func (f *fakeEnergy) EnergySummary(_ context.Context, _ string, _, _ time.Time) (postgresstore.EnergySummaryRow, error) {
	f.callCount++
	return f.summary, nil
}

type fakeLatest struct {
	proj       redisstore.MeterProjection
	err        error
	gotGrid    string
	gotHouseID string
}

func (f *fakeLatest) GetLatest(_ context.Context, gridID, houseID string) (redisstore.MeterProjection, error) {
	f.gotGrid, f.gotHouseID = gridID, houseID
	return f.proj, f.err
}

type fakeStatus struct {
	byHouse map[string]redisstore.HouseStatus
}

func (f *fakeStatus) GetHouseStatus(_ context.Context, houseID string) (redisstore.HouseStatus, error) {
	if s, ok := f.byHouse[houseID]; ok {
		return s, nil
	}
	return redisstore.HouseStatus{}, redisstore.ErrNotFound
}

func strp(v string) *string { return &v }

func house(id string, owner *string) postgresstore.HouseInfoRow {
	return postgresstore.HouseInfoRow{HouseID: id, GridID: "grid01", MeterID: "meter-" + id, DeviceClass: "residential_prosumer", OwnerUserID: owner}
}

type env struct {
	houses *fakeHouses
	energy *fakeEnergy
	latest *fakeLatest
	status *fakeStatus
	svc    *Service
}

func newEnv(rows ...postgresstore.HouseInfoRow) *env {
	e := &env{
		houses: &fakeHouses{rows: rows},
		energy: &fakeEnergy{},
		latest: &fakeLatest{},
		status: &fakeStatus{byHouse: map[string]redisstore.HouseStatus{}},
	}
	e.svc = NewService(e.houses, e.energy, e.latest, e.status)
	return e
}

var (
	t0 = time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(24 * time.Hour)
)

func TestListHousesDefaultsAndClampsPageSize(t *testing.T) {
	cases := map[int32]int{0: 51, 10: 11, 200: 201, 500: 201}
	for size, wantLimit := range cases {
		e := newEnv()
		if _, err := e.svc.ListHouses(context.Background(), ListHousesParams{PageSize: size}); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if e.houses.gotFilter.Limit != wantLimit {
			t.Errorf("size %d: reader limit = %d, want %d", size, e.houses.gotFilter.Limit, wantLimit)
		}
	}

	e := newEnv()
	if _, err := e.svc.ListHouses(context.Background(), ListHousesParams{PageSize: -1}); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("negative page size: err = %v, want ErrValidation", err)
	}
	if e.houses.listCalled {
		t.Error("reader called for invalid request")
	}
}

func TestListHousesPassesFilters(t *testing.T) {
	e := newEnv()
	_, err := e.svc.ListHouses(context.Background(), ListHousesParams{OwnerUserID: "u1", GridID: "grid01"})
	if err != nil {
		t.Fatal(err)
	}
	if e.houses.gotFilter.OwnerUserID != "u1" || e.houses.gotFilter.GridID != "grid01" || e.houses.gotFilter.AfterHouseID != "" {
		t.Fatalf("filter = %+v", e.houses.gotFilter)
	}
}

func TestListHousesPageToken(t *testing.T) {
	var rows []postgresstore.HouseInfoRow
	for i := 0; i < 51; i++ {
		rows = append(rows, house(fmt.Sprintf("h%03d", i), nil))
	}
	e := newEnv(rows...)
	res, err := e.svc.ListHouses(context.Background(), ListHousesParams{PageSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Houses) != 50 {
		t.Fatalf("houses = %d, want 50", len(res.Houses))
	}
	decoded, err := base64.RawURLEncoding.DecodeString(res.NextPageToken)
	if err != nil || string(decoded) != "h049" {
		t.Fatalf("next token decodes to %q (%v), want h049", decoded, err)
	}

	e2 := newEnv(rows...)
	if _, err := e2.svc.ListHouses(context.Background(), ListHousesParams{PageToken: res.NextPageToken}); err != nil {
		t.Fatal(err)
	}
	if e2.houses.gotFilter.AfterHouseID != "h049" {
		t.Fatalf("after = %q, want h049", e2.houses.gotFilter.AfterHouseID)
	}

	short := newEnv(house("a", nil))
	res, err = short.svc.ListHouses(context.Background(), ListHousesParams{})
	if err != nil || res.NextPageToken != "" {
		t.Fatalf("last page: token %q err %v, want empty", res.NextPageToken, err)
	}

	for _, bad := range []string{"!!not-base64!!", "a"} {
		if _, err := newEnv().svc.ListHouses(context.Background(), ListHousesParams{PageToken: bad}); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("token %q: err = %v, want ErrValidation", bad, err)
		}
	}
}

func TestHouseStatusMapping(t *testing.T) {
	beat := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e := newEnv(house("on", nil), house("off", nil))
	e.status.byHouse["on"] = redisstore.HouseStatus{Status: "online", LastHeartbeatAt: beat}

	res, err := e.svc.ListHouses(context.Background(), ListHousesParams{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]House{}
	for _, h := range res.Houses {
		got[h.HouseID] = h
	}
	if !got["on"].Online || got["on"].LastHeartbeatAt == nil || !got["on"].LastHeartbeatAt.Equal(beat) {
		t.Errorf("online house = %+v", got["on"])
	}
	if got["off"].Online || got["off"].LastHeartbeatAt != nil {
		t.Errorf("offline house = %+v", got["off"])
	}
}

func TestOwnershipEnforced(t *testing.T) {
	ctx := context.Background()
	calls := map[string]func(s *Service, owner string) error{
		"GetHouse": func(s *Service, owner string) error {
			_, _, err := s.GetHouse(ctx, "h1", owner)
			return err
		},
		"GetLatestReading": func(s *Service, owner string) error {
			_, err := s.GetLatestReading(ctx, "h1", owner)
			return err
		},
		"GetEnergySeries": func(s *Service, owner string) error {
			_, err := s.GetEnergySeries(ctx, EnergySeriesParams{HouseID: "h1", OwnerUserID: owner, Start: t0, End: t1, BucketWidth: time.Hour})
			return err
		},
		"GetEnergySummary": func(s *Service, owner string) error {
			_, err := s.GetEnergySummary(ctx, EnergySummaryParams{HouseID: "h1", OwnerUserID: owner, Start: t0, End: t1})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			owned := newEnv(house("h1", strp("u1")))
			if err := call(owned.svc, "u2"); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("other user: err = %v, want ErrNotFound", err)
			}
			if owned.energy.callCount != 0 {
				t.Error("energy reader called for a house the caller does not own")
			}
			unowned := newEnv(house("h1", nil))
			if err := call(unowned.svc, "u2"); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("unowned house: err = %v, want ErrNotFound", err)
			}
			if err := call(owned.svc, "u1"); err != nil {
				t.Errorf("owner: err = %v", err)
			}
			if err := call(owned.svc, ""); err != nil {
				t.Errorf("no owner check: err = %v", err)
			}
			if err := call(newEnv().svc, ""); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("unknown house: err = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestLatestReading(t *testing.T) {
	e := newEnv(house("h1", nil))
	e.latest.proj = redisstore.MeterProjection{HouseID: "h1", SolarKw: 3.8}
	got, err := e.svc.GetLatestReading(context.Background(), "h1", "")
	if err != nil || got.SolarKw != 3.8 {
		t.Fatalf("latest = %+v, %v", got, err)
	}
	if e.latest.gotGrid != "grid01" || e.latest.gotHouseID != "h1" {
		t.Errorf("redis key args = %q/%q, want grid01/h1", e.latest.gotGrid, e.latest.gotHouseID)
	}

	missing := newEnv(house("h1", nil))
	missing.latest.err = redisstore.ErrNotFound
	if _, err := missing.svc.GetLatestReading(context.Background(), "h1", ""); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("no reading: err = %v, want ErrNotFound", err)
	}

	broken := newEnv(house("h1", nil))
	broken.latest.err = errors.New("connection refused")
	if _, err := broken.svc.GetLatestReading(context.Background(), "h1", ""); err == nil || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrValidation) {
		t.Fatalf("redis failure: err = %v, want an internal error", err)
	}
}

func TestEnergyRangeValidation(t *testing.T) {
	cases := map[string]EnergySeriesParams{
		"empty house":      {Start: t0, End: t1, BucketWidth: time.Hour},
		"zero start":       {HouseID: "h1", End: t1, BucketWidth: time.Hour},
		"zero end":         {HouseID: "h1", Start: t0, BucketWidth: time.Hour},
		"end equals start": {HouseID: "h1", Start: t0, End: t0, BucketWidth: time.Hour},
		"end before start": {HouseID: "h1", Start: t1, End: t0, BucketWidth: time.Hour},
		"range 32 days":    {HouseID: "h1", Start: t0, End: t0.Add(32 * 24 * time.Hour), BucketWidth: 24 * time.Hour},
		"width 4 min":      {HouseID: "h1", Start: t0, End: t1, BucketWidth: 4 * time.Minute},
		"width 25 h":       {HouseID: "h1", Start: t0, End: t1, BucketWidth: 25 * time.Hour},
		"8640 buckets":     {HouseID: "h1", Start: t0, End: t0.Add(30 * 24 * time.Hour), BucketWidth: 5 * time.Minute},
		"bad zone":         {HouseID: "h1", Start: t0, End: t1, BucketWidth: time.Hour, TimeZone: "Mars/Base"},
		"local zone":       {HouseID: "h1", Start: t0, End: t1, BucketWidth: time.Hour, TimeZone: "Local"},
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(house("h1", nil))
			if _, err := e.svc.GetEnergySeries(context.Background(), p); !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("err = %v, want ErrValidation", err)
			}
			if e.energy.callCount != 0 {
				t.Error("energy reader called for invalid request")
			}
		})
	}

	summaryCases := map[string]EnergySummaryParams{
		"empty house":   {Start: t0, End: t1},
		"zero start":    {HouseID: "h1", End: t1},
		"end before":    {HouseID: "h1", Start: t1, End: t0},
		"range 32 days": {HouseID: "h1", Start: t0, End: t0.Add(32 * 24 * time.Hour)},
	}
	for name, p := range summaryCases {
		t.Run("summary "+name, func(t *testing.T) {
			if _, err := newEnv(house("h1", nil)).svc.GetEnergySummary(context.Background(), p); !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("err = %v, want ErrValidation", err)
			}
		})
	}

	ok := newEnv(house("h1", nil))
	p := EnergySeriesParams{HouseID: "h1", Start: t0, End: t0.Add(31 * 24 * time.Hour), BucketWidth: 24 * time.Hour, TimeZone: "Asia/Dubai"}
	if _, err := ok.svc.GetEnergySeries(context.Background(), p); err != nil {
		t.Fatalf("31 days of day buckets in Asia/Dubai: %v", err)
	}
}

func TestEnergySeriesAveragePower(t *testing.T) {
	e := newEnv(house("h1", nil))
	e.energy.buckets = []postgresstore.EnergyBucketRow{
		{Start: t0, CoveredHours: 0.5, Energy: postgresstore.EnergyTotals{GeneratedKwh: 1.5, ConsumedKwh: 1, ExportedKwh: 0.25, ImportedKwh: 0.5}},
		{Start: t0.Add(time.Hour), CoveredHours: 0, Energy: postgresstore.EnergyTotals{}},
	}
	got, err := e.svc.GetEnergySeries(context.Background(), EnergySeriesParams{HouseID: "h1", Start: t0, End: t1, BucketWidth: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if e.energy.gotTZ != "UTC" || e.energy.gotWidth != time.Hour {
		t.Errorf("reader got tz %q width %v, want UTC 1h", e.energy.gotTZ, e.energy.gotWidth)
	}
	want := AveragePower{SolarKw: 3, ConsumptionKw: 2, ExportKw: 0.5, ImportKw: 1}
	if len(got) != 2 || got[0].AveragePower != want || got[0].Energy.GeneratedKwh != 1.5 || !got[0].Start.Equal(t0) {
		t.Fatalf("bucket 0 = %+v, want average %+v", got, want)
	}
	if got[1].AveragePower != (AveragePower{}) {
		t.Errorf("bucket without coverage = %+v, want zero averages", got[1].AveragePower)
	}
}

func TestSelfConsumption(t *testing.T) {
	e := newEnv(house("h1", nil))
	e.energy.summary = postgresstore.EnergySummaryRow{
		Totals: postgresstore.EnergyTotals{GeneratedKwh: 10},
		Flows:  postgresstore.EnergyFlows{SolarToGridKwh: 2},
	}
	got, err := e.svc.GetEnergySummary(context.Background(), EnergySummaryParams{HouseID: "h1", Start: t0, End: t1})
	if err != nil {
		t.Fatal(err)
	}
	if got.SelfConsumptionPct == nil || math.Abs(*got.SelfConsumptionPct-80) > 1e-9 {
		t.Fatalf("self consumption = %v, want 80", got.SelfConsumptionPct)
	}
	if got.Totals.GeneratedKwh != 10 {
		t.Errorf("totals not passed through: %+v", got.Totals)
	}

	night := newEnv(house("h1", nil))
	got, err = night.svc.GetEnergySummary(context.Background(), EnergySummaryParams{HouseID: "h1", Start: t0, End: t1})
	if err != nil || got.SelfConsumptionPct != nil {
		t.Fatalf("no generation: pct = %v err %v, want nil", got.SelfConsumptionPct, err)
	}
}
