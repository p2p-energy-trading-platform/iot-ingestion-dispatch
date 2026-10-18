package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "houses.json")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const heartbeatLine = `{"schema_version":"1.0","grid_id":"grid01","house_id":"grid01-house0001","meter_id":"meter-grid01-house0001","status":"online","device_class":"consumer","rated_solar_kw":0,"flexible_assets":[]}`

func TestReadsArraysAndHeartbeatLines(t *testing.T) {
	arr, err := readEntries(writeFile(t, `[{"house_id":"grid01-house0001","grid_id":"grid01","meter_id":"meter-grid01-house0001","device_class":"consumer"}]`))
	if err != nil || len(arr) != 1 {
		t.Fatalf("array: %v %v", arr, err)
	}
	lines, err := readEntries(writeFile(t, heartbeatLine+"\n\n"+heartbeatLine+"\n"))
	if err != nil || len(lines) != 2 {
		t.Fatalf("lines: %v %v", lines, err)
	}
}

func TestValidateCollapsesRepeatedHeartbeats(t *testing.T) {
	entries, _ := readEntries(writeFile(t, heartbeatLine+"\n"+heartbeatLine))
	houses, err := validate(entries)
	if err != nil || len(houses) != 1 {
		t.Fatalf("houses = %v, err = %v; want one house", houses, err)
	}
}

func TestValidateRejectsBadIdentities(t *testing.T) {
	tests := map[string]string{
		"house not under grid": `{"house_id":"grid02-house0001","grid_id":"grid01","meter_id":"meter-grid02-house0001","device_class":"consumer"}`,
		"meter not for house":  `{"house_id":"grid01-house0001","grid_id":"grid01","meter_id":"meter-grid01-house0002","device_class":"consumer"}`,
		"unknown class":        `{"house_id":"grid01-house0001","grid_id":"grid01","meter_id":"meter-grid01-house0001","device_class":"factory"}`,
		"conflicting repeat": `{"house_id":"grid01-house0001","grid_id":"grid01","meter_id":"meter-grid01-house0001","device_class":"consumer"}
{"house_id":"grid01-house0001","grid_id":"grid01","meter_id":"meter-grid01-house0001","device_class":"commercial"}`,
	}
	for name, content := range tests {
		t.Run(name, func(t *testing.T) {
			entries, err := readEntries(writeFile(t, content))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validate(entries); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestValidateRejectsEmptyInput(t *testing.T) {
	if _, err := validate(nil); err == nil || !strings.Contains(err.Error(), "no houses") {
		t.Fatalf("error = %v", err)
	}
}
