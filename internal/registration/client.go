
package registration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ClaimPayload represents the request sent to the DEWA Mock API.
type ClaimPayload struct {
	RegistrationID string `json:"registrationId"`
	MeterID        string `json:"meterId"`
	UserReference  string `json:"userReference"`
}

// ClaimResponse represents the response received from the DEWA Mock API.
type ClaimResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// DEWAClient sends HTTP requests to the DEWA Mock API.
type DEWAClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewDEWAClient creates a DEWA client with a 10-second timeout.
func NewDEWAClient(baseURL string) *DEWAClient {
	return &DEWAClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// CreateLinkRequest sends a smart meter link request to DEWA.
func (client *DEWAClient) CreateLinkRequest(
	ctx context.Context,
	payload ClaimPayload,
) (*ClaimResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal DEWA claim payload: %w", err)
	}

	requestURL := client.baseURL + "/api/link-requests"

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		requestURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create DEWA request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send DEWA link request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK ||
		response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("DEWA returned HTTP status %d", response.StatusCode)
	}

	var result ClaimResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode DEWA response: %w", err)
	}

	return &result, nil
}
