package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	return rec.Body.String()
}

func TestMetricsExposeRequiredSignals(t *testing.T) {
	m := NewMetrics()
	m.ObserveRecord("iot.meter-readings", "ok", 5*time.Millisecond)
	m.ObserveRecord("iot.heartbeats", "permanent", time.Millisecond)
	m.ObserveRetry("iot.meter-readings")
	m.ObserveFailureRecorded("iot.heartbeats", "grid_validation")
	m.ObserveCommit(2*time.Millisecond, true)
	m.ObserveCommit(2*time.Millisecond, false)
	m.ObserveRebalance("assigned")
	m.ObserveCacheDegraded("iot.meter-readings")
	m.ObserveRegistryRefresh(true, 3, time.Millisecond)
	m.ObserveRegistryRefresh(false, 0, time.Millisecond)
	m.SetFailureBacklog(4, 90*time.Second)
	m.SetDependencyUp("redis", false)

	out := scrape(t, m)
	for _, want := range []string{
		`iot_ingestion_records_total{result="ok",topic="iot.meter-readings"} 1`,
		`iot_ingestion_records_total{result="permanent",topic="iot.heartbeats"} 1`,
		`iot_ingestion_record_retries_total{topic="iot.meter-readings"} 1`,
		`iot_ingestion_failures_recorded_total{stage="grid_validation",topic="iot.heartbeats"} 1`,
		`iot_ingestion_offset_commits_total{outcome="error"} 1`,
		`iot_ingestion_rebalance_events_total{kind="assigned"} 1`,
		`iot_ingestion_cache_degraded_total{topic="iot.meter-readings"} 1`,
		`iot_ingestion_grid_registry_size 3`,
		`iot_ingestion_grid_registry_refreshes_total{outcome="error"} 1`,
		`iot_ingestion_unresolved_failures 4`,
		`iot_ingestion_oldest_unresolved_failure_age_seconds 90`,
		`iot_ingestion_dependency_up{dependency="redis"} 0`,
		`go_goroutines`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}
}

func TestMetricsFailedRefreshKeepsLastSuccessfulState(t *testing.T) {
	m := NewMetrics()
	m.ObserveRegistryRefresh(true, 3, time.Millisecond)
	m.ObserveRegistryRefresh(false, 0, time.Millisecond)

	if out := scrape(t, m); !strings.Contains(out, "iot_ingestion_grid_registry_size 3") {
		t.Error("a failed refresh must not reset the reported registry size")
	}
}

func TestTracingDisabledWithoutEndpoint(t *testing.T) {
	stop, err := SetupTracing(t.Context(), "svc", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}
