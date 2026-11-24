package services

import (
	"context"
	"errors"
	"time"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/live"
)

// LiveWatcher is satisfied by *live.Hub.
type LiveWatcher interface {
	Watch(ctx context.Context, houseID string) (*live.Stream, error)
}

// WithLive enables StreamHouseLive. Each stream ends after maxAge, so no call
// runs forever; callers reopen it.
func (s *IotHandler) WithLive(watcher LiveWatcher, maxAge time.Duration) *IotHandler {
	s.live, s.streamMaxAge = watcher, maxAge
	return s
}

// StreamHouseLive sends the house's current status and latest reading, then
// each newer reading and every status change, until the caller leaves, the
// stream reaches its maximum age (DEADLINE_EXCEEDED) or the server shuts down
// (UNAVAILABLE).
func (s *IotHandler) StreamHouseLive(req *iotv1.StreamHouseLiveRequest, stream grpc.ServerStreamingServer[iotv1.StreamHouseLiveResponse]) error {
	ctx := stream.Context()

	// Validation and the optional owner check, exactly as for GetHouse.
	house, _, err := s.queries.GetHouse(ctx, req.GetHouseId(), req.GetOwnerUserId())
	if err != nil {
		return s.fail(ctx, "StreamHouseLive", err)
	}

	ctx, cancel := context.WithTimeout(ctx, s.streamMaxAge)
	defer cancel()

	// Watch before reading the snapshot, so nothing between the two is lost.
	watch, err := s.live.Watch(ctx, house.HouseID)
	if err != nil {
		s.logger.WarnContext(ctx, "live updates unavailable", "house_id", house.HouseID, "error", err)
		return status.Error(codes.Unavailable, "live updates are unavailable; retry later")
	}
	defer watch.Close()

	snapshot := live.Status{Online: house.Online}
	if house.LastHeartbeatAt != nil {
		snapshot.LastHeartbeatAt = *house.LastHeartbeatAt
	}
	if err := stream.Send(statusResponse(snapshot)); err != nil {
		return err
	}
	// Ownership was checked above, so the reading is read without an owner.
	latest, err := s.queries.GetLatestReading(ctx, house.HouseID, "")
	switch {
	case errors.Is(err, domain.ErrNotFound):
	case err != nil:
		return s.fail(ctx, "StreamHouseLive", err)
	default:
		if err := stream.Send(&iotv1.StreamHouseLiveResponse{Event: &iotv1.StreamHouseLiveResponse_Reading{Reading: toProtoReading(latest)}}); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			if stream.Context().Err() == nil {
				return status.Error(codes.DeadlineExceeded, "stream reached its maximum age; reopen it")
			}
			return status.FromContextError(stream.Context().Err()).Err()
		case <-watch.Done():
			return status.Error(codes.Unavailable, "server is shutting down; reconnect")
		case <-watch.Notify():
			st, reading := watch.Take()
			if st != nil {
				if err := stream.Send(statusResponse(*st)); err != nil {
					return err
				}
			}
			if reading != nil {
				if err := stream.Send(&iotv1.StreamHouseLiveResponse{Event: &iotv1.StreamHouseLiveResponse_Reading{Reading: toProtoReading(*reading)}}); err != nil {
					return err
				}
			}
		}
	}
}

func statusResponse(st live.Status) *iotv1.StreamHouseLiveResponse {
	update := &iotv1.MeterStatusUpdate{MeterStatus: iotv1.MeterStatus_METER_STATUS_OFFLINE}
	if st.Online {
		update.MeterStatus = iotv1.MeterStatus_METER_STATUS_ONLINE
		if !st.LastHeartbeatAt.IsZero() {
			update.LastHeartbeatAt = timestamppb.New(st.LastHeartbeatAt)
		}
	}
	return &iotv1.StreamHouseLiveResponse{Event: &iotv1.StreamHouseLiveResponse_Status{Status: update}}
}
