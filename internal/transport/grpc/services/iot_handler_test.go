package services

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/query"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/test/helpers"
)

type fakeQueries struct {
	houses  query.ListHousesResult
	house   query.House
	assets  []postgresstore.FlexibleAssetRow
	latest  redisstore.MeterProjection
	series  []query.EnergyBucket
	summary query.EnergySummary
	err     error

	gotList    query.ListHousesParams
	gotHouseID string
	gotOwner   string
	gotSeries  query.EnergySeriesParams
	gotSummary query.EnergySummaryParams
}

func (f *fakeQueries) ListHouses(_ context.Context, p query.ListHousesParams) (query.ListHousesResult, error) {
	f.gotList = p
	return f.houses, f.err
}

func (f *fakeQueries) GetHouse(_ context.Context, houseID, owner string) (query.House, []postgresstore.FlexibleAssetRow, error) {
	f.gotHouseID, f.gotOwner = houseID, owner
	return f.house, f.assets, f.err
}

func (f *fakeQueries) GetLatestReading(_ context.Context, houseID, owner string) (redisstore.MeterProjection, error) {
	f.gotHouseID, f.gotOwner = houseID, owner
	return f.latest, f.err
}

func (f *fakeQueries) GetEnergySeries(_ context.Context, p query.EnergySeriesParams) ([]query.EnergyBucket, error) {
	f.gotSeries = p
	return f.series, f.err
}

func (f *fakeQueries) GetEnergySummary(_ context.Context, p query.EnergySummaryParams) (query.EnergySummary, error) {
	f.gotSummary = p
	return f.summary, f.err
}

func f64p(v float64) *float64 { return &v }
func boolp(v bool) *bool      { return &v }
func strp(v string) *string   { return &v }

var beat = time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)

func TestIotServerListHousesMapping(t *testing.T) {
	f := &fakeQueries{houses: query.ListHousesResult{
		NextPageToken: "next",
		Houses: []query.House{
			{
				HouseInfoRow: postgresstore.HouseInfoRow{
					HouseID: "h1", GridID: "grid01", GridDisplayName: "Zone 1", MeterID: "m1",
					DeviceClass: "residential_prosumer", RatedSolarKw: f64p(6), OwnerUserID: strp("u1"),
				},
				Online: true, LastHeartbeatAt: &beat,
			},
			{HouseInfoRow: postgresstore.HouseInfoRow{HouseID: "h2", GridID: "grid02", DeviceClass: "consumer"}},
			{HouseInfoRow: postgresstore.HouseInfoRow{HouseID: "h3", GridID: "grid02", DeviceClass: "commercial"}},
		},
	}}
	res, err := NewIotHandler(f, helpers.TestLogger()).ListHouses(context.Background(), &iotv1.ListHousesRequest{
		OwnerUserId: "u1", GridId: "grid01", PageSize: 10, PageToken: "tok",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.gotList != (query.ListHousesParams{OwnerUserID: "u1", GridID: "grid01", PageSize: 10, PageToken: "tok"}) {
		t.Errorf("params = %+v", f.gotList)
	}
	if res.GetNextPageToken() != "next" || len(res.GetHouses()) != 3 {
		t.Fatalf("response = %v", res)
	}
	h1 := res.GetHouses()[0]
	if h1.GetHouseId() != "h1" || h1.GetGrid().GetGridId() != "grid01" || h1.GetGrid().GetDisplayName() != "Zone 1" ||
		h1.GetMeterId() != "m1" || h1.GetDeviceClass() != iotv1.DeviceClass_DEVICE_CLASS_RESIDENTIAL_PROSUMER ||
		h1.RatedSolarKw == nil || h1.GetRatedSolarKw() != 6 ||
		h1.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_ONLINE || !h1.GetLastHeartbeatAt().AsTime().Equal(beat) {
		t.Errorf("house 1 = %v", h1)
	}
	h2 := res.GetHouses()[1]
	if h2.GetDeviceClass() != iotv1.DeviceClass_DEVICE_CLASS_CONSUMER || h2.RatedSolarKw != nil ||
		h2.GetMeterStatus() != iotv1.MeterStatus_METER_STATUS_OFFLINE || h2.GetLastHeartbeatAt() != nil {
		t.Errorf("house 2 = %v", h2)
	}
	if res.GetHouses()[2].GetDeviceClass() != iotv1.DeviceClass_DEVICE_CLASS_COMMERCIAL {
		t.Errorf("house 3 class = %v", res.GetHouses()[2].GetDeviceClass())
	}
}

func TestIotServerGetHouseAssets(t *testing.T) {
	f := &fakeQueries{
		house: query.House{HouseInfoRow: postgresstore.HouseInfoRow{HouseID: "h1", GridID: "grid01", DeviceClass: "consumer"}},
		assets: []postgresstore.FlexibleAssetRow{
			{AssetID: "bat", AssetType: "bess", CapacityKwh: 14, MaxChargeKw: 5, MaxDischargeKw: 4},
			{AssetID: "ev", AssetType: "ev", CapacityKwh: 75, MaxChargeKw: 11, MaxDischargeKw: 11, V2GCapable: boolp(true)},
		},
	}
	res, err := NewIotHandler(f, helpers.TestLogger()).GetHouse(context.Background(), &iotv1.GetHouseRequest{HouseId: "h1", OwnerUserId: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.gotHouseID != "h1" || f.gotOwner != "u1" || res.GetHouse().GetHouseId() != "h1" {
		t.Fatalf("args %q/%q, response %v", f.gotHouseID, f.gotOwner, res)
	}
	bat, ev := res.GetFlexibleAssets()[0], res.GetFlexibleAssets()[1]
	if bat.GetAssetType() != iotv1.AssetType_ASSET_TYPE_BESS || bat.GetCapacityKwh() != 14 || bat.GetMaxChargeKw() != 5 ||
		bat.GetMaxDischargeKw() != 4 || bat.V2GCapable != nil {
		t.Errorf("battery = %v", bat)
	}
	if ev.GetAssetType() != iotv1.AssetType_ASSET_TYPE_EV || ev.V2GCapable == nil || !ev.GetV2GCapable() {
		t.Errorf("ev = %v", ev)
	}
}

func TestIotServerLatestReading(t *testing.T) {
	at := time.Date(2026, 10, 23, 10, 0, 5, 123_000_000, time.UTC)
	f := &fakeQueries{latest: redisstore.MeterProjection{
		EventTimeUnixMs: at.UnixMilli(), Seq: 42, MeterID: "m1", SolarKw: 3.8, ConsumptionKw: 2.5, NetKw: 0.1,
		StorageAssets: []redisstore.StorageAssetProjection{
			{AssetID: "bat", AssetType: "bess", SocPct: 94, PowerKw: 1.2, CapacityKwh: 14},
			{AssetID: "ev", AssetType: "ev", SocPct: 67, PowerKw: -2, CapacityKwh: 75, PluggedIn: boolp(false)},
		},
	}}
	res, err := NewIotHandler(f, helpers.TestLogger()).GetLatestReading(context.Background(), &iotv1.GetLatestReadingRequest{HouseId: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	r := res.GetReading()
	if !r.GetMeasuredAt().AsTime().Equal(at) || r.GetSeq() != 42 || r.GetMeterId() != "m1" ||
		r.GetSolarKw() != 3.8 || r.GetConsumptionKw() != 2.5 || r.GetNetKw() != 0.1 {
		t.Fatalf("reading = %v", r)
	}
	bat, ev := r.GetStorageAssets()[0], r.GetStorageAssets()[1]
	if bat.GetAssetType() != iotv1.AssetType_ASSET_TYPE_BESS || bat.GetSocPct() != 94 || bat.GetPowerKw() != 1.2 || bat.PluggedIn != nil {
		t.Errorf("battery = %v", bat)
	}
	if ev.GetAssetType() != iotv1.AssetType_ASSET_TYPE_EV || ev.PluggedIn == nil || ev.GetPluggedIn() || ev.GetCapacityKwh() != 75 {
		t.Errorf("ev = %v", ev)
	}
}

func TestIotServerEnergyRequestsPassThrough(t *testing.T) {
	start := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	f := &fakeQueries{series: []query.EnergyBucket{{
		Start:        start,
		Energy:       postgresstore.EnergyTotals{GeneratedKwh: 1, ConsumedKwh: 2, ExportedKwh: 3, ImportedKwh: 4, StorageChargedKwh: 5, StorageDischargedKwh: 6},
		AveragePower: query.AveragePower{SolarKw: 7, ConsumptionKw: 8, ExportKw: 9, ImportKw: 10},
	}}}
	srv := NewIotHandler(f, helpers.TestLogger())
	res, err := srv.GetEnergySeries(context.Background(), &iotv1.GetEnergySeriesRequest{
		HouseId: "h1", OwnerUserId: "u1", StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
		BucketWidth: durationpb.New(30 * time.Minute), TimeZone: "Asia/Dubai",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := query.EnergySeriesParams{HouseID: "h1", OwnerUserID: "u1", Start: start, End: end, BucketWidth: 30 * time.Minute, TimeZone: "Asia/Dubai"}
	if f.gotSeries != want {
		t.Errorf("series params = %+v, want %+v", f.gotSeries, want)
	}
	b := res.GetBuckets()[0]
	e, p := b.GetEnergy(), b.GetAveragePower()
	if !b.GetStartsAt().AsTime().Equal(start) ||
		e.GetGeneratedKwh() != 1 || e.GetConsumedKwh() != 2 || e.GetExportedKwh() != 3 || e.GetImportedKwh() != 4 ||
		e.GetStorageChargedKwh() != 5 || e.GetStorageDischargedKwh() != 6 ||
		p.GetSolarKw() != 7 || p.GetConsumptionKw() != 8 || p.GetExportKw() != 9 || p.GetImportKw() != 10 {
		t.Errorf("bucket = %v", b)
	}

	if _, err := srv.GetEnergySeries(context.Background(), &iotv1.GetEnergySeriesRequest{HouseId: "h1"}); err != nil {
		t.Fatal(err)
	}
	if !f.gotSeries.Start.IsZero() || !f.gotSeries.End.IsZero() || f.gotSeries.BucketWidth != 0 {
		t.Errorf("missing fields should reach the service as zero values, got %+v", f.gotSeries)
	}

	invalid := &timestamppb.Timestamp{Seconds: 1, Nanos: -1}
	_, err = srv.GetEnergySummary(context.Background(), &iotv1.GetEnergySummaryRequest{HouseId: "h1", StartTime: invalid, EndTime: timestamppb.New(end)})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("malformed timestamp: code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := srv.GetEnergySummary(context.Background(), &iotv1.GetEnergySummaryRequest{
		HouseId: "h1", OwnerUserId: "u1", StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
	}); err != nil {
		t.Fatal(err)
	}
	if f.gotSummary != (query.EnergySummaryParams{HouseID: "h1", OwnerUserID: "u1", Start: start, End: end}) {
		t.Errorf("summary params = %+v", f.gotSummary)
	}
}

func TestIotServerSummaryOptionalFields(t *testing.T) {
	peakAt := time.Date(2026, 10, 23, 19, 0, 0, 0, time.UTC)
	full := &fakeQueries{summary: query.EnergySummary{
		EnergySummaryRow: postgresstore.EnergySummaryRow{
			Totals: postgresstore.EnergyTotals{GeneratedKwh: 10},
			Flows: postgresstore.EnergyFlows{SolarToHomeKwh: 1, SolarToStorageKwh: 2, SolarToGridKwh: 3, GridToHomeKwh: 4,
				GridToStorageKwh: 5, StorageToHomeKwh: 6, StorageToGridKwh: 7},
			PeakConsumptionKw: f64p(3.4), PeakTime: &peakAt,
		},
		SelfConsumptionPct: f64p(70),
	}}
	res, err := NewIotHandler(full, helpers.TestLogger()).GetEnergySummary(context.Background(), &iotv1.GetEnergySummaryRequest{HouseId: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	fl := res.GetFlows()
	if res.GetTotals().GetGeneratedKwh() != 10 || res.SelfConsumptionPct == nil || res.GetSelfConsumptionPct() != 70 ||
		res.GetPeakDemand().GetConsumptionKw() != 3.4 || !res.GetPeakDemand().GetOccurredAt().AsTime().Equal(peakAt) ||
		fl.GetSolarToHomeKwh() != 1 || fl.GetSolarToStorageKwh() != 2 || fl.GetSolarToGridKwh() != 3 || fl.GetGridToHomeKwh() != 4 ||
		fl.GetGridToStorageKwh() != 5 || fl.GetStorageToHomeKwh() != 6 || fl.GetStorageToGridKwh() != 7 {
		t.Errorf("summary = %v", res)
	}

	empty, err := NewIotHandler(&fakeQueries{}, helpers.TestLogger()).GetEnergySummary(context.Background(), &iotv1.GetEnergySummaryRequest{HouseId: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	if empty.SelfConsumptionPct != nil || empty.GetPeakDemand() != nil || empty.GetTotals() == nil || empty.GetFlows() == nil {
		t.Errorf("empty summary = %v", empty)
	}
}

func TestIotServerErrorCodes(t *testing.T) {
	cases := map[error]codes.Code{
		domain.NewValidationError("start_time", "is required"): codes.InvalidArgument,
		domain.ErrNotFound:                         codes.NotFound,
		errors.New("postgres: connection refused"): codes.Internal,
	}
	for in, want := range cases {
		srv := NewIotHandler(&fakeQueries{err: in}, helpers.TestLogger())
		_, err := srv.GetHouse(context.Background(), &iotv1.GetHouseRequest{HouseId: "h1"})
		if status.Code(err) != want {
			t.Errorf("%v: code = %v, want %v", in, status.Code(err), want)
		}
	}
}

func TestIotServerLogsInternalCause(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	srv := NewIotHandler(&fakeQueries{err: errors.New("postgres: connection refused")}, logger)
	_, err := srv.GetEnergySummary(context.Background(), &iotv1.GetEnergySummaryRequest{HouseId: "h1"})

	if status.Code(err) != codes.Internal || strings.Contains(status.Convert(err).Message(), "connection refused") {
		t.Fatalf("client error = %v, want Internal without the cause", err)
	}
	if !strings.Contains(buf.String(), "connection refused") || !strings.Contains(buf.String(), "GetEnergySummary") {
		t.Fatalf("log = %q, want the cause and the method", buf.String())
	}
}

func TestIotServerDoesNotLogClientErrors(t *testing.T) {
	for _, in := range []error{domain.NewValidationError("end_time", "must be after start_time"), domain.ErrNotFound, context.Canceled} {
		var buf bytes.Buffer
		srv := NewIotHandler(&fakeQueries{err: in}, slog.New(slog.NewTextHandler(&buf, nil)))
		if _, err := srv.GetHouse(context.Background(), &iotv1.GetHouseRequest{HouseId: "h1"}); err == nil {
			t.Fatalf("%v: want an error", in)
		}
		if buf.Len() != 0 {
			t.Errorf("%v: logged %q, want nothing", in, buf.String())
		}
	}
}
