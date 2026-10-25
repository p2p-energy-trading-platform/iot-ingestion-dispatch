
package services

import (
	"context"
	"errors"
	"strings"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegistrationService defines the registration operations required by
// the gRPC transport layer.
type RegistrationService interface {
	InitiateMeterClaim(
		ctx context.Context,
		userReference string,
		meterID string,
	) (*ClaimResult, error)

	GetRegistrationStatus(
		ctx context.Context,
		registrationID string,
	) (*RegistrationStatus, error)

	ValidateDeviceAssociation(
		ctx context.Context,
		meterID string,
	) (*DeviceAssociation, error)

	ReverifyRegistration(
		ctx context.Context,
		registrationID string,
		userReference string,
	) (*ReverificationResult, error)
}

// ClaimResult contains the result of initiating a meter claim.
type ClaimResult struct {
	RegistrationID string
	Status         string
}

// RegistrationStatus contains the current local registration state.
type RegistrationStatus struct {
	RegistrationID string
	MeterID        string
	ApprovalStatus string
}

// DeviceAssociation contains the meter ownership validation result.
type DeviceAssociation struct {
	IsRegistered  bool
	PrimaryUserID string
}

// ReverificationResult contains the outcome of reverification.
type ReverificationResult struct {
	RegistrationID string
	MeterID        string
	PreviousStatus string
	CurrentStatus  string
	Success        bool
	Message        string
}

// RegistrationHandler implements DeviceRegistrationServiceServer.
type RegistrationHandler struct {
	iotv1.UnimplementedDeviceRegistrationServiceServer

	registrationService RegistrationService
}

// NewRegistrationHandler creates a registration gRPC handler.
func NewRegistrationHandler(
	registrationService RegistrationService,
) *RegistrationHandler {
	return &RegistrationHandler{
		registrationService: registrationService,
	}
}

// validateService ensures the handler has its required dependency.
func (h *RegistrationHandler) validateService() error {
	if h == nil || h.registrationService == nil {
		return status.Error(
			codes.Internal,
			"registration service is not configured",
		)
	}

	return nil
}

// InitiateMeterClaim starts a new meter registration request.
func (h *RegistrationHandler) InitiateMeterClaim(
	ctx context.Context,
	req *iotv1.ClaimMeterRequest,
) (*iotv1.ClaimMeterResponse, error) {
	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if err := h.validateService(); err != nil {
		return nil, err
	}

	userReference := strings.TrimSpace(req.GetUserReference())
	meterID := strings.TrimSpace(req.GetMeterId())

	if userReference == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"user reference is required",
		)
	}

	if meterID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"meter ID is required",
		)
	}

	result, err := h.registrationService.InitiateMeterClaim(
		ctx,
		userReference,
		meterID,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	if result == nil {
		return nil, status.Error(
			codes.Internal,
			"registration service returned an empty result",
		)
	}

	return &iotv1.ClaimMeterResponse{
		RegistrationId: result.RegistrationID,
		Status:         result.Status,
	}, nil
}

// GetRegistrationStatus retrieves the current local registration state.
func (h *RegistrationHandler) GetRegistrationStatus(
	ctx context.Context,
	req *iotv1.GetStatusRequest,
) (*iotv1.GetStatusResponse, error) {
	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if err := h.validateService(); err != nil {
		return nil, err
	}

	registrationID := strings.TrimSpace(req.GetRegistrationId())

	if registrationID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"registration ID is required",
		)
	}

	result, err := h.registrationService.GetRegistrationStatus(
		ctx,
		registrationID,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	if result == nil {
		return nil, status.Error(
			codes.Internal,
			"registration service returned an empty result",
		)
	}

	return &iotv1.GetStatusResponse{
		RegistrationId: result.RegistrationID,
		MeterId:        result.MeterID,
		ApprovalStatus: result.ApprovalStatus,
	}, nil
}

// ValidateDeviceAssociation checks whether a meter has an active
// registered association and retrieves its primary user.
func (h *RegistrationHandler) ValidateDeviceAssociation(
	ctx context.Context,
	req *iotv1.ValidateDeviceRequest,
) (*iotv1.ValidateDeviceResponse, error) {
	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if err := h.validateService(); err != nil {
		return nil, err
	}

	meterID := strings.TrimSpace(req.GetMeterId())

	if meterID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"meter ID is required",
		)
	}

	result, err := h.registrationService.ValidateDeviceAssociation(
		ctx,
		meterID,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	if result == nil {
		return nil, status.Error(
			codes.Internal,
			"registration service returned an empty result",
		)
	}

	return &iotv1.ValidateDeviceResponse{
		IsRegistered:  result.IsRegistered,
		PrimaryUserId: result.PrimaryUserID,
	}, nil
}

// ReverifyRegistration retries verification of an existing registration.
// The business service determines whether to resume polling or initiate
// a new DEWA request.
func (h *RegistrationHandler) ReverifyRegistration(
	ctx context.Context,
	req *iotv1.ReverifyRequest,
) (*iotv1.ReverifyResponse, error) {
	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if err := h.validateService(); err != nil {
		return nil, err
	}

	registrationID := strings.TrimSpace(req.GetRegistrationId())
	userReference := strings.TrimSpace(req.GetUserReference())

	if registrationID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"registration ID is required",
		)
	}

	if userReference == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"user reference is required",
		)
	}

	result, err := h.registrationService.ReverifyRegistration(
		ctx,
		registrationID,
		userReference,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	if result == nil {
		return nil, status.Error(
			codes.Internal,
			"registration service returned an empty result",
		)
	}

	return &iotv1.ReverifyResponse{
		RegistrationId: result.RegistrationID,
		MeterId:        result.MeterID,
		PreviousStatus: result.PreviousStatus,
		CurrentStatus:  result.CurrentStatus,
		Success:        result.Success,
		Message:        result.Message,
	}, nil
}

// mapRegistrationError translates business errors into gRPC status errors.
func mapRegistrationError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(
			codes.Canceled,
			"request canceled",
		)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(
			codes.DeadlineExceeded,
			"registration request timed out",
		)
	}

	// Avoid returning internal implementation details to gRPC clients.
	return status.Error(
		codes.Internal,
		"registration operation failed",
	)
}
