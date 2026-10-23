// Package query serves read access to houses, their assets and their metered
// energy for the IotService RPCs. It validates requests and enforces the
// optional owner scope; the stores do the reading.
package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	redisstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/redis"
)

type HouseReader interface {
	ListHouses(ctx context.Context, filter postgresstore.HouseFilter) ([]postgresstore.HouseInfoRow, error)
	// GetHouse returns domain.ErrNotFound for an unknown house.
	GetHouse(ctx context.Context, houseID string) (postgresstore.HouseInfoRow, []postgresstore.FlexibleAssetRow, error)
}

type EnergyReader interface {
	EnergySeries(ctx context.Context, houseID string, start, end time.Time, width time.Duration, tz string) ([]postgresstore.EnergyBucketRow, error)
	EnergySummary(ctx context.Context, houseID string, start, end time.Time) (postgresstore.EnergySummaryRow, error)
}

type LatestReader interface {
	GetLatest(ctx context.Context, gridID, houseID string) (redisstore.MeterProjection, error)
}

type StatusReader interface {
	GetHouseStatus(ctx context.Context, houseID string) (redisstore.HouseStatus, error)
}

type House struct {
	postgresstore.HouseInfoRow
	Online          bool
	LastHeartbeatAt *time.Time
}

type ListHousesParams struct {
	OwnerUserID string
	GridID      string
	PageSize    int32
	PageToken   string
}

type ListHousesResult struct {
	Houses        []House
	NextPageToken string
}

type EnergySeriesParams struct {
	HouseID     string
	OwnerUserID string
	Start       time.Time
	End         time.Time
	BucketWidth time.Duration
	TimeZone    string
}

type AveragePower struct {
	SolarKw       float64
	ConsumptionKw float64
	ExportKw      float64
	ImportKw      float64
}

type EnergyBucket struct {
	Start        time.Time
	Energy       postgresstore.EnergyTotals
	AveragePower AveragePower
}

type EnergySummaryParams struct {
	HouseID     string
	OwnerUserID string
	Start       time.Time
	End         time.Time
}

type EnergySummary struct {
	postgresstore.EnergySummaryRow
	SelfConsumptionPct *float64
}

type Service struct {
	houses HouseReader
	energy EnergyReader
	latest LatestReader
	status StatusReader
}

func NewService(houses HouseReader, energy EnergyReader, latest LatestReader, status StatusReader) *Service {
	return &Service{houses: houses, energy: energy, latest: latest, status: status}
}

func (s *Service) ListHouses(ctx context.Context, p ListHousesParams) (ListHousesResult, error) {
	size, err := pageSize(p.PageSize)
	if err != nil {
		return ListHousesResult{}, err
	}
	after, err := decodePageToken(p.PageToken)
	if err != nil {
		return ListHousesResult{}, err
	}

	rows, err := s.houses.ListHouses(ctx, postgresstore.HouseFilter{
		OwnerUserID: p.OwnerUserID, GridID: p.GridID, AfterHouseID: after, Limit: size + 1,
	})
	if err != nil {
		return ListHousesResult{}, fmt.Errorf("list houses: %w", err)
	}

	var res ListHousesResult
	if len(rows) > size {
		rows = rows[:size]
		res.NextPageToken = encodePageToken(rows[size-1].HouseID)
	}
	for _, row := range rows {
		h, err := s.withStatus(ctx, row)
		if err != nil {
			return ListHousesResult{}, err
		}
		res.Houses = append(res.Houses, h)
	}
	return res, nil
}

func (s *Service) GetHouse(ctx context.Context, houseID, ownerUserID string) (House, []postgresstore.FlexibleAssetRow, error) {
	if houseID == "" {
		return House{}, nil, domain.NewValidationError("house_id", "is required")
	}
	row, assets, err := s.ownedHouse(ctx, houseID, ownerUserID)
	if err != nil {
		return House{}, nil, err
	}
	h, err := s.withStatus(ctx, row)
	if err != nil {
		return House{}, nil, err
	}
	return h, assets, nil
}

func (s *Service) GetLatestReading(ctx context.Context, houseID, ownerUserID string) (redisstore.MeterProjection, error) {
	if houseID == "" {
		return redisstore.MeterProjection{}, domain.NewValidationError("house_id", "is required")
	}
	row, _, err := s.ownedHouse(ctx, houseID, ownerUserID)
	if err != nil {
		return redisstore.MeterProjection{}, err
	}
	reading, err := s.latest.GetLatest(ctx, row.GridID, row.HouseID)
	if errors.Is(err, redisstore.ErrNotFound) {
		return redisstore.MeterProjection{}, fmt.Errorf("no reading yet for house %q: %w", houseID, domain.ErrNotFound)
	}
	if err != nil {
		return redisstore.MeterProjection{}, fmt.Errorf("latest reading: %w", err)
	}
	return reading, nil
}

func (s *Service) GetEnergySeries(ctx context.Context, p EnergySeriesParams) ([]EnergyBucket, error) {
	if err := validateRange(p.HouseID, p.Start, p.End); err != nil {
		return nil, err
	}
	if err := validateBuckets(p.Start, p.End, p.BucketWidth); err != nil {
		return nil, err
	}
	tz, err := timeZone(p.TimeZone)
	if err != nil {
		return nil, err
	}
	if _, _, err := s.ownedHouse(ctx, p.HouseID, p.OwnerUserID); err != nil {
		return nil, err
	}

	rows, err := s.energy.EnergySeries(ctx, p.HouseID, p.Start, p.End, p.BucketWidth, tz)
	if err != nil {
		return nil, fmt.Errorf("energy series: %w", err)
	}
	buckets := make([]EnergyBucket, 0, len(rows))
	for _, r := range rows {
		buckets = append(buckets, EnergyBucket{Start: r.Start, Energy: r.Energy, AveragePower: averagePower(r)})
	}
	return buckets, nil
}

func (s *Service) GetEnergySummary(ctx context.Context, p EnergySummaryParams) (EnergySummary, error) {
	if err := validateRange(p.HouseID, p.Start, p.End); err != nil {
		return EnergySummary{}, err
	}
	if _, _, err := s.ownedHouse(ctx, p.HouseID, p.OwnerUserID); err != nil {
		return EnergySummary{}, err
	}

	row, err := s.energy.EnergySummary(ctx, p.HouseID, p.Start, p.End)
	if err != nil {
		return EnergySummary{}, fmt.Errorf("energy summary: %w", err)
	}
	summary := EnergySummary{EnergySummaryRow: row}
	if generated := row.Totals.GeneratedKwh; generated > 0 {
		pct := (generated - row.Flows.SolarToGridKwh) / generated * 100
		summary.SelfConsumptionPct = &pct
	}
	return summary, nil
}

// ownedHouse loads a house and applies the owner scope. A house owned by
// someone else is reported exactly like a missing one, so callers cannot
// probe which house IDs exist.
func (s *Service) ownedHouse(ctx context.Context, houseID, ownerUserID string) (postgresstore.HouseInfoRow, []postgresstore.FlexibleAssetRow, error) {
	row, assets, err := s.houses.GetHouse(ctx, houseID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return postgresstore.HouseInfoRow{}, nil, err
		}
		return postgresstore.HouseInfoRow{}, nil, fmt.Errorf("get house: %w", err)
	}
	if ownerUserID != "" && (row.OwnerUserID == nil || *row.OwnerUserID != ownerUserID) {
		return postgresstore.HouseInfoRow{}, nil, fmt.Errorf("house %q: %w", houseID, domain.ErrNotFound)
	}
	return row, assets, nil
}

func (s *Service) withStatus(ctx context.Context, row postgresstore.HouseInfoRow) (House, error) {
	h := House{HouseInfoRow: row}
	status, err := s.status.GetHouseStatus(ctx, row.HouseID)
	if errors.Is(err, redisstore.ErrNotFound) {
		return h, nil
	}
	if err != nil {
		return House{}, fmt.Errorf("house status: %w", err)
	}
	beat := status.LastHeartbeatAt
	h.Online = true
	h.LastHeartbeatAt = &beat
	return h, nil
}

func averagePower(r postgresstore.EnergyBucketRow) AveragePower {
	if r.CoveredHours <= 0 {
		return AveragePower{}
	}
	return AveragePower{
		SolarKw:       r.Energy.GeneratedKwh / r.CoveredHours,
		ConsumptionKw: r.Energy.ConsumedKwh / r.CoveredHours,
		ExportKw:      r.Energy.ExportedKwh / r.CoveredHours,
		ImportKw:      r.Energy.ImportedKwh / r.CoveredHours,
	}
}
