package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns the service's Prometheus registry. No label value is ever a
// house, grid, meter, asset, request or Kafka-key value: only topic names,
// result classes and failure stages, all small fixed sets.
type Metrics struct {
	registry *prometheus.Registry

	records         *prometheus.CounterVec
	recordDuration  *prometheus.HistogramVec
	retries         *prometheus.CounterVec
	failuresRecords *prometheus.CounterVec
	commits         *prometheus.CounterVec
	commitDuration  prometheus.Histogram
	rebalances      *prometheus.CounterVec
	cacheDegraded   *prometheus.CounterVec

	registryGrids       prometheus.Gauge
	registryRefreshes   *prometheus.CounterVec
	registryRefreshTime prometheus.Histogram
	registryLastSuccess prometheus.Gauge

	unresolvedFailures prometheus.Gauge
	oldestUnresolved   prometheus.Gauge
	dependencyUp       *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "records_total",
			Help: "Kafka records finished, by topic and result (ok, permanent, retry_exhausted).",
		}, []string{"topic", "result"}),
		recordDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "iot_ingestion", Name: "record_processing_seconds",
			Help:    "Time from first attempt to finished record, including retries.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"topic"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "record_retries_total",
			Help: "Retry attempts after a transient failure, by topic.",
		}, []string{"topic"}),
		failuresRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "failures_recorded_total",
			Help: "Failures durably recorded in ingestion_failures, by topic and stage.",
		}, []string{"topic", "stage"}),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "offset_commits_total",
			Help: "Offset commit calls by outcome (ok, error).",
		}, []string{"outcome"}),
		commitDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "iot_ingestion", Name: "offset_commit_seconds",
			Help:    "Offset commit latency.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		rebalances: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "rebalance_events_total",
			Help: "Partition assignment events (assigned, revoked, lost).",
		}, []string{"kind"}),
		cacheDegraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "cache_degraded_total",
			Help: "Records acknowledged after a failed Redis update under the async-repair policy.",
		}, []string{"topic"}),

		registryGrids: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "iot_ingestion", Name: "grid_registry_size",
			Help: "Grids in the live admission snapshot.",
		}),
		registryRefreshes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot_ingestion", Name: "grid_registry_refreshes_total",
			Help: "Grid registry refresh attempts by outcome (ok, error).",
		}, []string{"outcome"}),
		registryRefreshTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "iot_ingestion", Name: "grid_registry_refresh_seconds",
			Help:    "Grid registry refresh duration.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		registryLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "iot_ingestion", Name: "grid_registry_last_successful_refresh_timestamp_seconds",
			Help: "Unix time of the last successful grid registry refresh; alert on its age.",
		}),

		unresolvedFailures: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "iot_ingestion", Name: "unresolved_failures",
			Help: "Rows in ingestion_failures with resolution=unresolved.",
		}),
		oldestUnresolved: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "iot_ingestion", Name: "oldest_unresolved_failure_age_seconds",
			Help: "Age of the oldest unresolved ingestion failure.",
		}),
		dependencyUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "iot_ingestion", Name: "dependency_up",
			Help: "1 when the dependency (postgres, redis) answered its last probe.",
		}, []string{"dependency"}),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.records, m.recordDuration, m.retries, m.failuresRecords, m.commits, m.commitDuration,
		m.rebalances, m.cacheDegraded,
		m.registryGrids, m.registryRefreshes, m.registryRefreshTime, m.registryLastSuccess,
		m.unresolvedFailures, m.oldestUnresolved, m.dependencyUp,
	)
	return m
}

// Registry lets callers add collectors (for example pool statistics).
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// --- ingestion.Metrics ---

func (m *Metrics) ObserveRecord(topic, result string, took time.Duration) {
	m.records.WithLabelValues(topic, result).Inc()
	m.recordDuration.WithLabelValues(topic).Observe(took.Seconds())
}

func (m *Metrics) ObserveFailureRecorded(topic, stage string) {
	m.failuresRecords.WithLabelValues(topic, stage).Inc()
}

func (m *Metrics) ObserveRetry(topic string) { m.retries.WithLabelValues(topic).Inc() }

func (m *Metrics) ObserveCommit(took time.Duration, ok bool) {
	outcome := "ok"
	if !ok {
		outcome = "error"
	}
	m.commits.WithLabelValues(outcome).Inc()
	m.commitDuration.Observe(took.Seconds())
}

func (m *Metrics) ObserveRebalance(kind string) { m.rebalances.WithLabelValues(kind).Inc() }

func (m *Metrics) ObserveCacheDegraded(topic string) { m.cacheDegraded.WithLabelValues(topic).Inc() }

// --- admission.RegistryMetrics ---

func (m *Metrics) ObserveRegistryRefresh(ok bool, gridCount int, took time.Duration) {
	outcome := "ok"
	if !ok {
		outcome = "error"
	}
	m.registryRefreshes.WithLabelValues(outcome).Inc()
	m.registryRefreshTime.Observe(took.Seconds())
	if ok {
		m.registryGrids.Set(float64(gridCount))
		m.registryLastSuccess.SetToCurrentTime()
	}
}

// --- probes ---

func (m *Metrics) SetFailureBacklog(unresolved int64, oldestAge time.Duration) {
	m.unresolvedFailures.Set(float64(unresolved))
	m.oldestUnresolved.Set(oldestAge.Seconds())
}

func (m *Metrics) SetDependencyUp(name string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	m.dependencyUp.WithLabelValues(name).Set(v)
}
