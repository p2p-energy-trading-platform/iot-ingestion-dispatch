//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/registration"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

// An account has one smart meter; a house is identified by its meter.
const (
	owner   = "44444444-4444-4444-4444-444444444444"
	someone = "55555555-5555-5555-5555-555555555555"
)

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

func claim(userID, registrationID, meterID string) registration.RegistrationRecord {
	return registration.RegistrationRecord{
		UserID:         userID,
		MeterID:        meterID,
		RegistrationID: registrationID,
		Role:           "PRIMARY_OWNER",
		ApprovalStatus: "DEWA_APPROVAL_PENDING",
	}
}

func approvalStatus(t *testing.T, db *sql.DB, registrationID string) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT approval_status FROM iot_data.meter_registrations WHERE registration_id = $1`, registrationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestOnlyAnApprovedRegistrationCountsAsTheAccountsMeter(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	ctx := context.Background()

	seedRegistrations(t, db, `
		('reg-pending', 'meter-a', '`+owner+`', 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER', now()),
		('reg-rejected', 'meter-b', '`+owner+`', 'REJECTED', 'PRIMARY_OWNER', now()),
		('reg-expired', 'meter-c', '`+owner+`', 'EXPIRED', 'PRIMARY_OWNER', now())`)
	none, err := pg.GetApprovedRegistrationByUserID(ctx, owner)
	if err != nil || none != nil {
		t.Fatalf("no approved registration: got %+v, %v; want nil, nil", none, err)
	}

	seedRegistrations(t, db, `('reg-approved', 'meter-d', '`+owner+`', 'APPROVED', 'PRIMARY_OWNER', now() - interval '1 day')`)
	approved, err := pg.GetApprovedRegistrationByUserID(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if approved == nil || approved.RegistrationID != "reg-approved" {
		t.Fatalf("approved = %+v, want reg-approved", approved)
	}
}

// A pending claim is not ownership: anyone can type a meter number, and DEWA
// approval can fail or never happen. Pending claims must not block new ones.
func TestPendingClaimsDoNotBlockNewClaims(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	ctx := context.Background()
	seedRegistrations(t, db, `
		('reg-mine', 'meter-a', '`+owner+`', 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER', now()),
		('reg-theirs', 'meter-b', '`+someone+`', 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER', now())`)

	for _, c := range []registration.RegistrationRecord{
		claim(owner, "reg-mine-again", "meter-a"),  // same user, same meter, new request
		claim(owner, "reg-mine-other", "meter-c"),  // same user, another meter
		claim(owner, "reg-mine-theirs", "meter-b"), // a meter someone else has only claimed
		claim(someone, "reg-theirs-a", "meter-a"),  // another user, a meter I have only claimed
	} {
		if err := pg.SaveMeterRegistration(ctx, c); err != nil {
			t.Errorf("claim %s for %s: err = %v, want it saved", c.RegistrationID, c.MeterID, err)
		}
	}
}

func TestAccountCannotHaveTwoApprovedMeters(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	seedRegistrations(t, db, `
		('reg-first', 'meter-a', '`+owner+`', 'APPROVED', 'PRIMARY_OWNER', now()),
		('reg-second', 'meter-b', '`+owner+`', 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER', now())`)

	if err := pg.UpdateMeterRegistrationStatus(context.Background(), "reg-second", "APPROVED"); err == nil {
		t.Fatal("a second meter was approved for the account")
	}
	if got := approvalStatus(t, db, "reg-second"); got != "DEWA_APPROVAL_PENDING" {
		t.Fatalf("reg-second status = %s, want it left pending", got)
	}
}

func TestConcurrentApprovalsLeaveOneApprovedMeter(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	const claims = 8
	for i := range claims {
		if err := pg.SaveMeterRegistration(context.Background(), claim(owner, fmt.Sprintf("reg-%d", i), fmt.Sprintf("meter-%d", i))); err != nil {
			t.Fatalf("pending claim %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, claims)
	for i := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = pg.UpdateMeterRegistrationStatus(context.Background(), fmt.Sprintf("reg-%d", i), "APPROVED")
		}()
	}
	wg.Wait()

	approved := 0
	for _, err := range errs {
		if err == nil {
			approved++
		}
	}
	if approved != 1 {
		t.Fatalf("%d approvals succeeded, want exactly 1", approved)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT count(*) FROM iot_data.meter_registrations
		WHERE user_id = $1 AND approval_status = 'APPROVED'`, owner).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%d approved registrations, want 1", count)
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
		t.Fatal("up succeeded although the account has two approved meters")
	}
}

func TestDeleteStaleMeterRegistrationsInBatches(t *testing.T) {
	pg, db := newRegistrationEnv(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO iot_data.meter_registrations
			(registration_id, meter_id, user_id, approval_status, role, created_at, updated_at)
		VALUES
			('cleanup-pending', 'meter-clean-pending', $1, 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER',
			 now() - interval '10 days', now() - interval '6 days'),
			('cleanup-rejected', 'meter-clean-rejected', $1, 'REJECTED', 'PRIMARY_OWNER',
			 now() - interval '10 days', now() - interval '6 days'),
			('cleanup-expired', 'meter-clean-expired', $1, 'EXPIRED', 'PRIMARY_OWNER',
			 now() - interval '10 days', now() - interval '6 days'),
			('cleanup-recent', 'meter-clean-recent', $1, 'DEWA_APPROVAL_PENDING', 'PRIMARY_OWNER',
			 now(), now()),
			('cleanup-approved', 'meter-clean-approved', $1, 'APPROVED', 'PRIMARY_OWNER',
			 now() - interval '10 days', now() - interval '6 days')`, owner); err != nil {
		t.Fatalf("seed cleanup registrations: %v", err)
	}

	cutoff := time.Now().Add(-5 * 24 * time.Hour)
	firstBatch, err := pg.DeleteStaleMeterRegistrations(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("delete first cleanup batch: %v", err)
	}
	if firstBatch != 2 {
		t.Fatalf("first batch deleted %d registrations, want 2", firstBatch)
	}

	secondBatch, err := pg.DeleteStaleMeterRegistrations(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("delete second cleanup batch: %v", err)
	}
	if secondBatch != 1 {
		t.Fatalf("second batch deleted %d registrations, want 1", secondBatch)
	}

	var eligibleRemaining int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM iot_data.meter_registrations
		WHERE registration_id IN ('cleanup-pending', 'cleanup-rejected', 'cleanup-expired')`,
	).Scan(&eligibleRemaining); err != nil {
		t.Fatalf("count stale registrations: %v", err)
	}
	if eligibleRemaining != 0 {
		t.Fatalf("%d stale non-approved registrations remain, want 0", eligibleRemaining)
	}

	var protectedRemaining int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM iot_data.meter_registrations
		WHERE registration_id IN ('cleanup-recent', 'cleanup-approved')`,
	).Scan(&protectedRemaining); err != nil {
		t.Fatalf("count protected registrations: %v", err)
	}
	if protectedRemaining != 2 {
		t.Fatalf("%d recent or approved registrations remain, want 2", protectedRemaining)
	}
}
