// Command register-houses provisions houses in iot_data.houses.
//
// Heartbeats never create houses (06-heartbeat-processing.md); a house must be
// registered first. Until the METER_VERIFIED registration consumer exists, this
// administrative command is how houses are provisioned, the same way grids are
// provisioned by migration (05-startup-registry.md).
//
//	register-houses -file houses.json            # JSON array or one object per line
//	register-houses -file houses.json -dry-run   # validate only
//
// Each entry needs house_id, grid_id, meter_id and device_class; rated_solar_kw
// is optional. Heartbeat payloads have exactly these fields, so captured
// heartbeats can be fed in directly (extra fields are ignored).
//
// Registration is idempotent and never changes an existing house: re-running
// with the same data reports "unchanged", and a different grid, meter or device
// class for an existing house is refused.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

const maxEntries = 10000

type entry struct {
	HouseID      string   `json:"house_id"`
	GridID       string   `json:"grid_id"`
	MeterID      string   `json:"meter_id"`
	DeviceClass  string   `json:"device_class"`
	RatedSolarKw *float64 `json:"rated_solar_kw"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(); err != nil {
		logger.Error("register-houses failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	file := flag.String("file", "", "JSON file with houses (array, or one object per line); - for stdin")
	dryRun := flag.Bool("dry-run", false, "validate the file without writing")
	flag.Parse()
	if *file == "" {
		return errors.New("-file is required")
	}

	entries, err := readEntries(*file)
	if err != nil {
		return err
	}

	houses, err := validate(entries)
	if err != nil {
		return err
	}
	if *dryRun {
		fmt.Printf("%d house(s) valid; nothing written (dry run)\n", len(houses))
		return nil
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	pg, err := postgresstore.NewWithPool(ctx, cfg.Postgres.URL, postgresstore.PoolConfig{MaxConns: 2})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pg.Close()

	counts := map[postgresstore.RegistrationOutcome]int{}
	failed := 0
	for _, h := range houses {
		outcome, err := pg.RegisterHouse(ctx, h)
		if err != nil {
			fmt.Printf("%s refused: %v\n", h.HouseID, err)
			failed++
			continue
		}
		counts[outcome]++
	}
	fmt.Printf("created %d, unchanged %d, refused %d\n",
		counts[postgresstore.HouseCreated], counts[postgresstore.HouseUnchanged], failed)
	if failed > 0 {
		return fmt.Errorf("%d house(s) were not registered", failed)
	}
	return nil
}

func readEntries(path string) ([]entry, error) {
	var r io.Reader = os.Stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	data, err := io.ReadAll(io.LimitReader(r, 64<<20))
	if err != nil {
		return nil, err
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var entries []entry
		if err := json.Unmarshal(trimmed, &entries); err != nil {
			return nil, fmt.Errorf("parse JSON array: %w", err)
		}
		return entries, nil
	}

	var entries []entry
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for line := 1; scanner.Scan(); line++ {
		text := bytes.TrimSpace(scanner.Bytes())
		if len(text) == 0 {
			continue
		}
		var e entry
		if err := json.Unmarshal(text, &e); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		entries = append(entries, e)
	}
	return entries, scanner.Err()
}

// validate checks every entry before anything is written, and collapses exact
// duplicates (a heartbeat capture repeats each house once per interval).
func validate(entries []entry) ([]postgresstore.HouseRegistration, error) {
	if len(entries) == 0 {
		return nil, errors.New("no houses in input")
	}
	if len(entries) > maxEntries {
		return nil, fmt.Errorf("at most %d entries per run", maxEntries)
	}

	seen := map[string]postgresstore.HouseRegistration{}
	var out []postgresstore.HouseRegistration
	var errs []error
	for i, e := range entries {
		class, err := ingestion.ValidateHouseIdentity(e.GridID, e.HouseID, e.MeterID, e.DeviceClass)
		if err != nil {
			errs = append(errs, fmt.Errorf("entry %d (%s): %w", i+1, e.HouseID, err))
			continue
		}
		h := postgresstore.HouseRegistration{
			HouseID: e.HouseID, GridID: e.GridID, MeterID: e.MeterID,
			DeviceClass: class, RatedSolarKw: e.RatedSolarKw,
		}
		if prev, ok := seen[h.HouseID]; ok {
			if prev.GridID != h.GridID || prev.MeterID != h.MeterID || prev.DeviceClass != h.DeviceClass {
				errs = append(errs, fmt.Errorf("entry %d: house %s appears twice with different identities", i+1, h.HouseID))
			}
			continue
		}
		seen[h.HouseID] = h
		out = append(out, h)
	}
	return out, errors.Join(errs...)
}
