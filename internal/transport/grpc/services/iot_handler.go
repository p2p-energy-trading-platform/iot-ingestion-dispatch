package services

import (
	"context"
	"errors"
	"log/slog"
	"time"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/query"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
	grpctransport "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc"
)

// IotQueries is what IotHandler needs from query.Service.
type IotQueries interface {
	ListHouses(ctx context.Context, p query.ListHousesParams) (query.ListHousesResult, error)
	GetHouse(ctx context.Context, houseID, ownerUserID string) (query.House, []postgresstore.FlexibleAssetRow, error)
	GetLatestReading(ctx context.Context, houseID, ownerUserID string) (redisstore.MeterProjection, error)
	GetEnergySeries(ctx context.Context, p query.EnergySeriesParams) ([]query.EnergyBucket, error)
	GetEnergySummary(ctx context.Context, p query.EnergySummaryParams) (query.EnergySummary, error)
}

// IotHandler adapts the query service to the gridx.iot.v1.IotService
// contract. It only converts types; validation and ownership live in query.
type IotHandler struct {
	iotv1.UnimplementedIotServiceServer
	queries IotQueries
	logger  *slog.Logger
}

func NewIotHandler(queries IotQueries, logger *slog.Logger) *IotHandler {
	return &IotHandler{queries: queries, logger: logger}
}

// fail converts err to a gRPC status. Server-side failures are logged here
// with their cause, because the status sent to the client deliberately hides it.
func (s *IotHandler) fail(ctx context.Context, method string, err error) error {
	if !errors.Is(err, domain.ErrValidation) && !errors.Is(err, domain.ErrNotFound) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.logger.ErrorContext(ctx, "iot query failed", "method", method, "error", err)
	}
	return grpctransport.ToGRPCError(err)
}

func (s *IotHandler) ListHouses(ctx context.Context, req *iotv1.ListHousesRequest) (*iotv1.ListHousesResponse, error) {
	res, err := s.queries.ListHouses(ctx, query.ListHousesParams{
		OwnerUserID: req.GetOwnerUserId(), GridID: req.GetGridId(),
		PageSize: req.GetPageSize(), PageToken: req.GetPageToken(),
	})
	if err != nil {
		return nil, s.fail(ctx, "ListHouses", err)
	}
	out := &iotv1.ListHousesResponse{NextPageToken: res.NextPageToken}
	for _, h := range res.Houses {
		out.Houses = append(out.Houses, toProtoHouse(h))
	}
	return out, nil
}

func (s *IotHandler) GetHouse(ctx context.Context, req *iotv1.GetHouseRequest) (*iotv1.GetHouseResponse, error) {
	house, assets, err := s.queries.GetHouse(ctx, req.GetHouseId(), req.GetOwnerUserId())
	if err != nil {
		return nil, s.fail(ctx, "GetHouse", err)
	}
	out := &iotv1.GetHouseResponse{House: toProtoHouse(house)}
	for _, a := range assets {
		out.FlexibleAssets = append(out.FlexibleAssets, &iotv1.FlexibleAsset{
			AssetId:        a.AssetID,
			AssetType:      toProtoAssetType(a.AssetType),
			CapacityKwh:    a.CapacityKwh,
			MaxChargeKw:    a.MaxChargeKw,
			MaxDischargeKw: a.MaxDischargeKw,
			V2GCapable:     a.V2GCapable,
		})
	}
	return out, nil
}

func (s *IotHandler) GetLatestReading(ctx context.Context, req *iotv1.GetLatestReadingRequest) (*iotv1.GetLatestReadingResponse, error) {
	r, err := s.queries.GetLatestReading(ctx, req.GetHouseId(), req.GetOwnerUserId())
	if err != nil {
		return nil, s.fail(ctx, "GetLatestReading", err)
	}
	reading := &iotv1.MeterReading{
		MeterId:       r.MeterID,
		MeasuredAt:    timestamppb.New(time.UnixMilli(r.EventTimeUnixMs)),
		Seq:           r.Seq,
		SolarKw:       r.SolarKw,
		ConsumptionKw: r.ConsumptionKw,
		NetKw:         r.NetKw,
	}
	for _, a := range r.StorageAssets {
		reading.StorageAssets = append(reading.StorageAssets, &iotv1.StorageAssetReading{
			AssetId:     a.AssetID,
			AssetType:   toProtoAssetType(a.AssetType),
			SocPct:      a.SocPct,
			PowerKw:     a.PowerKw,
			CapacityKwh: a.CapacityKwh,
			PluggedIn:   a.PluggedIn,
		})
	}
	return &iotv1.GetLatestReadingResponse{Reading: reading}, nil
}

func (s *IotHandler) GetEnergySeries(ctx context.Context, req *iotv1.GetEnergySeriesRequest) (*iotv1.GetEnergySeriesResponse, error) {
	start, end, err := requestRange(req.GetStartTime(), req.GetEndTime())
	if err != nil {
		return nil, s.fail(ctx, "GetEnergySeries", err)
	}
	width, err := requestDuration(req.GetBucketWidth())
	if err != nil {
		return nil, s.fail(ctx, "GetEnergySeries", err)
	}
	buckets, err := s.queries.GetEnergySeries(ctx, query.EnergySeriesParams{
		HouseID: req.GetHouseId(), OwnerUserID: req.GetOwnerUserId(),
		Start: start, End: end, BucketWidth: width, TimeZone: req.GetTimeZone(),
	})
	if err != nil {
		return nil, s.fail(ctx, "GetEnergySeries", err)
	}
	out := &iotv1.GetEnergySeriesResponse{}
	for _, b := range buckets {
		out.Buckets = append(out.Buckets, &iotv1.EnergyBucket{
			StartsAt: timestamppb.New(b.Start),
			Energy:   toProtoTotals(b.Energy),
			AveragePower: &iotv1.AveragePower{
				SolarKw:       b.AveragePower.SolarKw,
				ConsumptionKw: b.AveragePower.ConsumptionKw,
				ExportKw:      b.AveragePower.ExportKw,
				ImportKw:      b.AveragePower.ImportKw,
			},
		})
	}
	return out, nil
}

func (s *IotHandler) GetEnergySummary(ctx context.Context, req *iotv1.GetEnergySummaryRequest) (*iotv1.GetEnergySummaryResponse, error) {
	start, end, err := requestRange(req.GetStartTime(), req.GetEndTime())
	if err != nil {
		return nil, s.fail(ctx, "GetEnergySummary", err)
	}
	summary, err := s.queries.GetEnergySummary(ctx, query.EnergySummaryParams{
		HouseID: req.GetHouseId(), OwnerUserID: req.GetOwnerUserId(), Start: start, End: end,
	})
	if err != nil {
		return nil, s.fail(ctx, "GetEnergySummary", err)
	}
	f := summary.Flows
	out := &iotv1.GetEnergySummaryResponse{
		Totals: toProtoTotals(summary.Totals),
		Flows: &iotv1.EnergyFlows{
			SolarToHomeKwh:    f.SolarToHomeKwh,
			SolarToStorageKwh: f.SolarToStorageKwh,
			SolarToGridKwh:    f.SolarToGridKwh,
			GridToHomeKwh:     f.GridToHomeKwh,
			GridToStorageKwh:  f.GridToStorageKwh,
			StorageToHomeKwh:  f.StorageToHomeKwh,
			StorageToGridKwh:  f.StorageToGridKwh,
		},
		SelfConsumptionPct: summary.SelfConsumptionPct,
	}
	if summary.PeakConsumptionKw != nil && summary.PeakTime != nil {
		out.PeakDemand = &iotv1.PeakDemand{ConsumptionKw: *summary.PeakConsumptionKw, OccurredAt: timestamppb.New(*summary.PeakTime)}
	}
	return out, nil
}

func toProtoHouse(h query.House) *iotv1.House {
	out := &iotv1.House{
		HouseId:      h.HouseID,
		Grid:         &iotv1.Grid{GridId: h.GridID, DisplayName: h.GridDisplayName},
		MeterId:      h.MeterID,
		DeviceClass:  toProtoDeviceClass(h.DeviceClass),
		RatedSolarKw: h.RatedSolarKw,
		MeterStatus:  iotv1.MeterStatus_METER_STATUS_OFFLINE,
	}
	if h.Online {
		out.MeterStatus = iotv1.MeterStatus_METER_STATUS_ONLINE
	}
	if h.LastHeartbeatAt != nil {
		out.LastHeartbeatAt = timestamppb.New(*h.LastHeartbeatAt)
	}
	return out
}

func toProtoTotals(t postgresstore.EnergyTotals) *iotv1.EnergyTotals {
	return &iotv1.EnergyTotals{
		GeneratedKwh:         t.GeneratedKwh,
		ConsumedKwh:          t.ConsumedKwh,
		ExportedKwh:          t.ExportedKwh,
		ImportedKwh:          t.ImportedKwh,
		StorageChargedKwh:    t.StorageChargedKwh,
		StorageDischargedKwh: t.StorageDischargedKwh,
	}
}

func toProtoDeviceClass(class string) iotv1.DeviceClass {
	switch domain.DeviceClass(class) {
	case domain.DeviceClassConsumer:
		return iotv1.DeviceClass_DEVICE_CLASS_CONSUMER
	case domain.DeviceClassResidentialProsumer:
		return iotv1.DeviceClass_DEVICE_CLASS_RESIDENTIAL_PROSUMER
	case domain.DeviceClassCommercial:
		return iotv1.DeviceClass_DEVICE_CLASS_COMMERCIAL
	default:
		return iotv1.DeviceClass_DEVICE_CLASS_UNSPECIFIED
	}
}

func toProtoAssetType(assetType string) iotv1.AssetType {
	switch domain.AssetType(assetType) {
	case domain.AssetTypeBESS:
		return iotv1.AssetType_ASSET_TYPE_BESS
	case domain.AssetTypeEV:
		return iotv1.AssetType_ASSET_TYPE_EV
	default:
		return iotv1.AssetType_ASSET_TYPE_UNSPECIFIED
	}
}

// requestRange converts the range timestamps. A missing timestamp becomes the
// zero time, which the query service rejects as a required field.
func requestRange(start, end *timestamppb.Timestamp) (time.Time, time.Time, error) {
	s, err := requestTime("start_time", start)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	e, err := requestTime("end_time", end)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return s, e, nil
}

func requestTime(field string, ts *timestamppb.Timestamp) (time.Time, error) {
	if ts == nil {
		return time.Time{}, nil
	}
	if err := ts.CheckValid(); err != nil {
		return time.Time{}, domain.NewValidationError(field, "is not a valid timestamp")
	}
	return ts.AsTime(), nil
}

func requestDuration(d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return 0, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, domain.NewValidationError("bucket_width", "is not a valid duration")
	}
	return d.AsDuration(), nil
}
