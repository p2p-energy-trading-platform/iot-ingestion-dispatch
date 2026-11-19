//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/registration"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

// An account has one smart meter; a house is identified by its meter.
const owner = "44444444-4444-4444-4444-444444444444"

// newRegistrationEnv migrates a fresh database and opens the Postgres store.
func newRegistrationEnv(t *testing.T) (*postgresstore.Store, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	dsn := newTimescale(t)
	if _, err := gooseProvider(t, dsn).Up(ctx); err != nil {
		t.Fatal(err)
	}
	pg, err := postgresstore.NewWithPool(ctx, dsn, postgresstore.PoolConfig{MaxConns: 12})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	return pg, openDB(t, dsn)
}

func seedRegistrations(t *testing.T, db *sql.DB, rows string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO iot_data.meter_registrations (registration_id, meter_id, user_id, approval_status, role, created_at) VALUES `+rows); err != nil {
		t.Fatal(err)
	}
}

func claim(registrationID, meterID string) registration.RegistrationRecord {
	return registration.RegistrationRecord{
		UserID:         owner,
		MeterID:        meterID,
		RegistrationID: registrationID,
		Role:           "PRIMARY_OWNER",
		ApprovalStatus: "DEWA_APPROVAL_PENDING",
	}
}

func TestActiveRegistrationIsThePendingOrApprovedOne(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	ctx := context.Background()

	none, err := pg.GetActiveRegistrationByUserID(ctx, owner)
	if err != nil || none != nil {
		t.Fatalf("no registrations: got %+v, %v; want nil, nil", none, err)
	}

	seedRegistrations(t, db, `
		('reg-old', 'meter-a', '`+owner+`', 'REJECTED', 'PRIMARY_OWNER', now() - interval '2 days'),
		('reg-now', 'meter-b', '`+owner+`', 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER', now() - interval '1 day'),
		('reg-new', 'meter-c', '`+owner+`', 'EXPIRED', 'PRIMARY_OWNER', now())`)

	active, err := pg.GetActiveRegistrationByUserID(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if active == nil || active.RegistrationID != "reg-now" {
		t.Fatalf("active = %+v, want the pending reg-now", active)
	}
}

func TestRegistrationByUserPrefersTheActiveOne(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	seedRegistrations(t, db, `
		('reg-approved', 'meter-a', '`+owner+`', 'APPROVED', 'PRIMARY_OWNER', now() - interval '3 days'),
		('reg-retry', 'meter-b', '`+owner+`', 'REJECTED', 'PRIMARY_OWNER', now())`)

	got, err := pg.GetRegistrationByUserID(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if got.RegistrationID != "reg-approved" {
		t.Fatalf("registration = %s, want the approved one even though a rejected one is newer", got.RegistrationID)
	}
}

func TestAccountCannotHoldTwoActiveMeters(t *testing.T) {
	for _, status := range []string{"APPROVED", "DEWA_APPROVAL_PENDING"} {
		t.Run(status, func(t *testing.T) {
			pg, db := newRegistrationEnv(t)
			seedRegistrations(t, db, `('reg-first', 'meter-a', '`+owner+`', '`+status+`', 'PRIMARY_OWNER', now())`)

			err := pg.SaveMeterRegistration(context.Background(), claim("reg-second", "meter-b"))
			if !errors.Is(err, domain.ErrMeterAlreadyRegistered) {
				t.Fatalf("err = %v, want ErrMeterAlreadyRegistered", err)
			}
		})
	}
}

func TestRejectedOrExpiredMetersDoNotBlockANewClaim(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	seedRegistrations(t, db, `
		('reg-rejected', 'meter-a', '`+owner+`', 'REJECTED', 'PRIMARY_OWNER', now()),
		('reg-expired', 'meter-b', '`+owner+`', 'EXPIRED', 'PRIMARY_OWNER', now())`)

	if err := pg.SaveMeterRegistration(context.Background(), claim("reg-new", "meter-c")); err != nil {
		t.Fatalf("err = %v, want the claim to be saved", err)
	}
}

func TestConcurrentClaimsLeaveOneActiveMeter(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	const claims = 8

	var wg sync.WaitGroup
	errs := make([]error, claims)
	for i := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = pg.SaveMeterRegistration(context.Background(), claim(fmt.Sprintf("reg-%d", i), fmt.Sprintf("meter-%d", i)))
		}()
	}
	wg.Wait()

	saved := 0
	for _, err := range errs {
		switch {
		case err == nil:
			saved++
		case !errors.Is(err, domain.ErrMeterAlreadyRegistered):
			t.Errorf("unexpected error: %v", err)
		}
	}
	if saved != 1 {
		t.Fatalf("%d claims were saved, want exactly 1", saved)
	}

	var active int
	if err := db.QueryRowContext(context.Background(), `
		SELECT count(*) FROM iot_data.meter_registrations
		WHERE user_id = $1 AND approval_status IN ('APPROVED', 'DEWA_APPROVAL_PENDING')`, owner).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("%d active registrations, want 1", active)
	}
}

func TestOneMeterPerAccountMigrationRollsBack(t *testing.T) {
	ctx := context.Background()
	dsn := newTimescale(t)
	provider := gooseProvider(t, dsn)
	if _, err := provider.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(ctx, 12); err != nil {
		t.Fatalf("down: %v", err)
	}
	db := openDB(t, dsn)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO iot_data.meter_registrations (registration_id, meter_id, user_id, approval_status, role) VALUES
		('reg-1', 'meter-a', '`+owner+`', 'APPROVED', 'PRIMARY_OWNER'),
		('reg-2', 'meter-b', '`+owner+`', 'APPROVED', 'PRIMARY_OWNER')`); err != nil {
		t.Fatalf("after rollback two approved meters should be allowed again: %v", err)
	}
	if _, err := provider.Up(ctx); err == nil {
		t.Fatal("up succeeded although the account has two active meters")
	}
}
