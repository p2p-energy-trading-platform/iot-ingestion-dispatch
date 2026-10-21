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
// rejected, cancelled, or times out.
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

	if p.client.httpClient == nil {
		return nil, fmt.Errorf("DEWA HTTP client is not configured")
	}

	if p.interval < 3*time.Second ||
		p.interval > 5*time.Second {
		return nil, fmt.Errorf(
			"poll interval must be between 3 and 5 seconds",
		)
	}

	if p.timeout <= 0 {
		return nil, fmt.Errorf("poll timeout must be positive")
	}

	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}

	pollCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	endpoint := strings.TrimRight(p.client.baseURL, "/") +
		"/api/link-requests/" + url.PathEscape(requestID)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		if err := pollCtx.Err(); err != nil {
			return nil, p.pollingError(err)
		}

		response, err := p.getStatus(pollCtx, endpoint)
		if err != nil {
			if pollCtx.Err() != nil {
				return nil, p.pollingError(pollCtx.Err())
			}

			return nil, err
		}

		switch strings.ToUpper(strings.TrimSpace(response.Status)) {
		case "APPROVED":
			response.Status = "APPROVED"
			return response, nil

		case "REJECTED":
			response.Status = "REJECTED"
			return response, nil

		case "PENDING", "PROCESSING":
			// Registration is still in progress.
			// Wait before checking DEWA again.

		default:
			return nil, fmt.Errorf(
				"unexpected DEWA registration status: %q",
				response.Status,
			)
		}

		select {
		case <-pollCtx.Done():
			return nil, p.pollingError(pollCtx.Err())

		case <-ticker.C:
			// Check the registration status again.
		}
	}
}

// pollingError describes why polling stopped.
func (p *Poller) pollingError(err error) error {
	if err == context.DeadlineExceeded {
		return fmt.Errorf(
			"DEWA registration polling timed out after %s: %w",
			p.timeout,
			err,
		)
	}

	return fmt.Errorf("DEWA registration polling stopped: %w", err)
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
		return nil, fmt.Errorf(
			"create DEWA status request: %w",
			err,
		)
	}

	request.Header.Set("Accept", "application/json")

	resp, err := p.client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf(
			"get DEWA registration status: %w",
			err,
		)
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
		return nil, fmt.Errorf(
			"decode DEWA status response: %w",
			err,
		)
	}

	if strings.TrimSpace(response.Status) == "" {
		return nil, fmt.Errorf(
			"DEWA status response contains an empty status",
		)
	}

	return &response, nil
}
