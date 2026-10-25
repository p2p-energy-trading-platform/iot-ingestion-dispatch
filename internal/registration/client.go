package registration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type LinkRequest struct {
	RegistrationID string `json:"registrationId"`
	MeterID        string `json:"meterId"`
	UserReference  string `json:"userReference"`
}

type LinkResponse struct {
	RequestID string `json:"requestId,omitempty"`
	Status    string `json:"status"`
	Message   string `json:"message"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *slog.Logger
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) CreateLinkRequest(
	ctx context.Context,
	request LinkRequest,
) (*LinkResponse, error) {

	payload, err := json.Marshal(request)

	if err != nil {
		return nil, fmt.Errorf("marshal DEWA link request: %w", err)
	}

	url := c.baseURL + "/api/link-requests"

	httpRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("create DEWA link request: %w", err)
	}

	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("send DEWA link request: %w", err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("Create link request failed", "error", closeErr)
		}
	}()

	if resp.StatusCode < http.StatusOK ||
		resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("DEWA returned HTTP status %d", resp.StatusCode)
	}

	var linkResponse LinkResponse
	if err := json.NewDecoder(resp.Body).Decode(&linkResponse); err != nil {
		return nil, fmt.Errorf("decode DEWA link response: %w", err)
	}

	return &linkResponse, nil

}
