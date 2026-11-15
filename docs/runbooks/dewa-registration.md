
# DEWA Meter Registration Workflow

## 1. Overview

The DEWA registration workflow links a user's smart meter to GridX through the DEWA Mock API.

The registration service:
- Validates the meter ID and user reference.
- Generates a registration ID when one is not provided.
- Saves the registration with the initial status `DEWA_APPROVAL_PENDING`.
- Sends a link request to the DEWA Mock API.
- Returns the registration ID and DEWA response status.

## 2. Configuration

Create the DEWA client using the configured base URL:

```go
client := registration.NewClient(baseURL)
```

The base URL must point to the running DEWA Mock API. The client removes trailing slashes and uses a 10-second HTTP timeout.

### API Endpoint

```http
POST /api/link-requests
Content-Type: application/json
Accept: application/json
```

## 3. Request and Response

### Request Payload

```json
{
  "registrationId": "REG-123",
  "meterId": "METER-001",
  "userReference": "USER-123"
}
```

### Successful Response

```json
{
  "requestId": "REQ-123",
  "status": "PENDING",
  "message": "Link request created"
}
```

If the caller does not provide a registration ID, GridX generates a UUID.

## 4. Request Flow

1. The caller submits a meter registration request.
2. The service validates the meter ID and user reference.
3. The service generates a registration ID if necessary.
4. The service saves the registration in PostgreSQL with status `DEWA_APPROVAL_PENDING`.
5. The service sends a POST request to the DEWA Mock API.
6. The service returns the registration ID and DEWA response status.
7. The service polls DEWA and updates the local registration status.

A successful response with status `PENDING` does not mean the meter has been approved.

## 5. Database Persistence

The registration record is stored in:

`iot_data.meter_registrations`

Migration file:

`migrations/00011_create_meter_registrations.sql` and `migrations/0013_add_dewa_request_id.sql`

The table contains:
- `registration_id`
- `meter_id`
- `user_id` (the platform user UUID)
- `role` (new claims default to `PRIMARY_OWNER`)
- `request_id` (the DEWA ID used to resume polling)
- `approval_status`
- `created_at`
- `updated_at`

The initial approval status is `DEWA_APPROVAL_PENDING`.

Supported statuses:
- `DEWA_APPROVAL_PENDING`
- `APPROVED`
- `REJECTED`
- `EXPIRED`

Apply the project's database migrations before running the registration service against PostgreSQL. The IoT service stores the user UUID, but does not add a foreign key to a users table owned by another service.

## 6. Error Handling

The HTTP client handles:
- HTTP request creation errors.
- Network errors and context cancellation.
- Non-2xx HTTP responses.
- Malformed JSON responses.

The service handles:
- Missing repository or DEWA client.
- Missing meter ID or user reference.
- Database persistence failures.
- DEWA dispatch failures.
- Empty DEWA responses.

The registration is saved before dispatching the request to DEWA. If dispatch fails, the pending record remains in the database for later investigation or reconciliation.

## 7. Approval polling and reverification

After DEWA returns a `requestId`, the service stores it and polls
`GET /api/link-requests/{requestId}` every four seconds. Polling stops on
`APPROVED`, `REJECTED`, or after ten minutes; a timeout changes the local
status to `EXPIRED`. Approval activates the meter for the ingestion flow.

`ReverifyRegistration` resumes polling with the stored DEWA request ID, without
creating another DEWA request. If the registration or its request ID is absent,
the service creates a new DEWA link request and stores its ID. Reverification
can include the user reference and meter ID; supplied values must match the
stored registration.

## 8. Local Testing

### Start the DEWA Mock API

Start the mock service using its development instructions and confirm the configured base URL.

### Test the Registration Package

From the repository root:

```powershell
go test ./internal/registration
```

### Test the PostgreSQL Package

```powershell
go test ./internal/store/postgres
```

### Run All Go Tests

```powershell
go test ./...
```

Run these commands and check the results before considering the implementation verified.

The HTTP client tests use Go's `httptest` package to test the request payload, endpoint, HTTP errors, and malformed responses.

The service tests use fake dependencies to test input validation, registration ID generation, pending-status persistence, and dispatch outcomes.

## 9. Troubleshooting

| Problem | What to check |
|---|---|
| Connection refused | Confirm the DEWA Mock API is running and the base URL is correct. |
| HTTP error | Check the mock service logs and response status. |
| Invalid JSON response | Verify the mock API response format. |
| Database error | Confirm PostgreSQL is running and migrations have been applied. |
| Registration remains pending | Check DEWA approval state and the stored DEWA request ID; polling expires after ten minutes. |

## 10. Scope and Limitations

The DEWA mock remains the authority for approval. The service persists each
registration attempt and its local state; administrative history and cleanup
policies are separate concerns.
