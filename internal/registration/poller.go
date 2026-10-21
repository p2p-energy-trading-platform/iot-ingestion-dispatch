package registration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultPollInterval = 4 * time.Second
	defaultPollTimeout  = 10 * time.Minute
)

// Poller checks the status of a DEWA link request.
type Poller struct {
	client   *Client
	interval time.Duration
	timeout  time.Duration
}

// NewPoller creates a poller with the default interval and timeout.
func NewPoller(client *Client) *Poller {
	return &Poller{
		client:   client,
		interval: defaultPollInterval,
		timeout:  defaultPollTimeout,
	}
}

// PollLinkRequest checks DEWA until the request is approved,
// rejected, cancelled by the caller, or times out.
func (p *Poller) PollLinkRequest(
	ctx context.Context,
	requestID string,
) (*LinkResponse, error) {
	if p == nil || p.client == nil {
		return nil, fmt.Errorf("registration client is not configured")
	}

	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("request ID is required")
	}

	if p.interval <= 0 {
		return nil, fmt.Errorf("poll interval must be positive")
	}

	if p.timeout <= 0 {
		return nil, fmt.Errorf("poll timeout must be positive")
	}

	pollCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	endpoint := p.client.baseURL +
		"/api/link-requests/" + url.PathEscape(requestID)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		response, err := p.getStatus(pollCtx, endpoint)
		if err != nil {
			if pollCtx.Err() != nil {
				return nil, fmt.Errorf(
					"DEWA registration polling stopped: %w",
					pollCtx.Err(),
				)
			}

			return nil, err
		}

		switch strings.ToUpper(strings.TrimSpace(response.Status)) {
		case "APPROVED", "REJECTED":
			return response, nil

		case "PENDING", "PROCESSING":
			// The registration is not finished.
			// Wait before checking again.

		default:
			return nil, fmt.Errorf(
				"unexpected DEWA registration status: %q",
				response.Status,
			)
		}

		select {
		case <-pollCtx.Done():
			return nil, fmt.Errorf(
				"DEWA registration polling stopped: %w",
				pollCtx.Err(),
			)

		case <-ticker.C:
			// Time to check the status again.
		}
	}
}

// getStatus sends a GET request to the DEWA status endpoint.
func (p *Poller) getStatus(
	ctx context.Context,
	endpoint string,
) (*LinkResponse, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create DEWA status request: %w", err)
	}

	request.Header.Set("Accept", "application/json")

	resp, err := p.client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get DEWA registration status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK ||
		resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf(
			"DEWA status endpoint returned HTTP status %d",
			resp.StatusCode,
		)
	}

	var response LinkResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode DEWA status response: %w", err)
	}

	return &response, nil
}
