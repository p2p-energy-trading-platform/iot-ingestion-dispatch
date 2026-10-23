package query

import (
	"encoding/base64"
	"fmt"
	"time"
	_ "time/tzdata" // the runtime image has no zoneinfo; time zones come from the binary

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
	// Measured on TimescaleDB 2.30 with compressed chunks: 30 days of 5 s
	// readings take ~5 s to aggregate, 184 days ~30 s. Longer ranges need
	// continuous aggregates before they fit the callers' deadlines.
	maxRange       = 31 * 24 * time.Hour
	minBucketWidth = 5 * time.Minute
	maxBucketWidth = 24 * time.Hour
	maxBuckets     = 5000
)

func pageSize(requested int32) (int, error) {
	switch {
	case requested < 0:
		return 0, domain.NewValidationError("page_size", "must not be negative")
	case requested == 0:
		return defaultPageSize, nil
	case requested > maxPageSize:
		return maxPageSize, nil
	default:
		return int(requested), nil
	}
}

func encodePageToken(lastHouseID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(lastHouseID))
}

func decodePageToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) == 0 {
		return "", domain.NewValidationError("page_token", "is not a token returned by this service")
	}
	return string(decoded), nil
}

func validateRange(houseID string, start, end time.Time) error {
	if houseID == "" {
		return domain.NewValidationError("house_id", "is required")
	}
	if start.IsZero() {
		return domain.NewValidationError("start_time", "is required")
	}
	if end.IsZero() {
		return domain.NewValidationError("end_time", "is required")
	}
	if !end.After(start) {
		return domain.NewValidationError("end_time", "must be after start_time")
	}
	if end.Sub(start) > maxRange {
		return domain.NewValidationError("end_time", fmt.Sprintf("range must not exceed %d days", int(maxRange.Hours()/24)))
	}
	return nil
}

func validateBuckets(start, end time.Time, width time.Duration) error {
	if width < minBucketWidth || width > maxBucketWidth {
		return domain.NewValidationError("bucket_width", "must be between 5 minutes and 1 day")
	}
	span := end.Sub(start)
	buckets := span / width
	if span%width != 0 {
		buckets++
	}
	if buckets > maxBuckets {
		return domain.NewValidationError("bucket_width", fmt.Sprintf("range needs %d buckets; at most %d are allowed", buckets, maxBuckets))
	}
	return nil
}

// timeZone returns the IANA name to bucket with. "Local" is refused because it
// would depend on the server's configuration rather than the caller's intent.
func timeZone(name string) (string, error) {
	if name == "" {
		return "UTC", nil
	}
	if name == "Local" {
		return "", domain.NewValidationError("time_zone", "must be an IANA time zone name")
	}
	if _, err := time.LoadLocation(name); err != nil {
		return "", domain.NewValidationError("time_zone", "must be an IANA time zone name")
	}
	return name, nil
}
