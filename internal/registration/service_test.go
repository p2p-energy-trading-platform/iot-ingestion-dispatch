
package registration

import (
	"context"
	"strings"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

type testRegistrationRepository struct{}

func (r *testRegistrationRepository) SaveMeterRegistration(
	ctx context.Context,
	registration postgres.MeterRegistration,
) error {
	return nil
}

type testDEWAClient struct{}

func (c *testDEWAClient) CreateLinkRequest(
	ctx context.Context,
	payload ClaimPayload,
) (*ClaimResponse, error) {
	return &ClaimResponse{
		Status:  "PENDING",
		Message: "Link request created",
	}, nil
}

func TestInitiateMeterClaimMissingMeterID(t *testing.T) {
	service := NewService(
		&testRegistrationRepository{},
		&testDEWAClient{},
	)

	_, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			UserReference: "USER-123",
		},
	)

	if err == nil {
		t.Fatal("expected error when meter ID is missing")
	}

	if !strings.Contains(err.Error(), "meter ID is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitiateMeterClaimMissingUserReference(t *testing.T) {
	service := NewService(
		&testRegistrationRepository{},
		&testDEWAClient{},
	)

	_, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			MeterID: "METER-001",
		},
	)

	if err == nil {
		t.Fatal("expected error when user reference is missing")
	}

	if !strings.Contains(err.Error(), "user reference is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}
