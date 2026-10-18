//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/admission"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/ingestion"
	postgresstore "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

type countingHandler struct {
	inner ingestion.RecordHandler
	calls atomic.Int64
}

func (c *countingHandler) HandleRecord(ctx context.Context, key, value []byte) error {
	c.calls.Add(1)
	return c.inner.HandleRecord(ctx, key, value)
}

func createTopics(t *testing.T, broker string, partitions int32, topics ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	for _, name := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, partitions, 1
		req.Topics = append(req.Topics, rt)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := req.RequestWith(context.Background(), cl)
		if err == nil {
			ok := true
			for _, rt := range resp.Topics {
				if rt.ErrorCode != 0 && rt.ErrorCode != 36 { // 36 = TOPIC_ALREADY_EXISTS
					ok = false
					err = fmt.Errorf("create %s: error code %d", rt.Topic, rt.ErrorCode)
				}
			}
			if ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topics: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func produce(t *testing.T, broker string, records ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func rec(topic, key, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(value)}
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestConsumerEndToEndWithRestart drives real Kafka -> consumer -> handlers ->
// TimescaleDB/Redis, including a poison record and an unknown grid, then
// restarts the consumer in the same group to prove only finished, contiguous
// offsets were committed (nothing is processed twice).
func TestConsumerEndToEndWithRestart(t *testing.T) {
	e := newEnv(t)
	broker := newKafka(t)
	createTopics(t, broker, 2, meterTopic, heartbeatTopic)
	ctx := context.Background()

	meterH := &countingHandler{inner: ingestion.NewMeterHandler(mustRegistry(t, e.pg), e.pg, e.rd, quietLogger())}
	hbH := &countingHandler{inner: ingestion.NewHeartbeatHandler(mustRegistry(t, e.pg), e.pg, e.rd, quietLogger())}
	router := ingestion.NewRouter()
	router.Register(meterTopic, meterH)
	router.Register(heartbeatTopic, hbH)

	start := func() (stop func()) {
		c, err := ingestion.NewConsumer(ingestion.Config{
			Brokers: []string{broker}, ConsumerGroup: "it-" + t.Name(),
			MeterTopic: meterTopic, HeartbeatTopic: heartbeatTopic,
		}, router, e.pg, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Run(runCtx) }()
		return func() { cancel(); wg.Wait(); c.Close() }
	}

	at := time.Now().UTC().Truncate(time.Second)
	house := "grid01-house0042"
	produce(t, broker,
		rec(heartbeatTopic, house, heartbeatJSON(42, "grid01", "bat_0042", 10)),
		rec(meterTopic, house, meterJSON(42, 1, at, 1.5)),
		rec(meterTopic, house, `{"this is": "not a reading"`), // poison: permanent decode failure
		rec(meterTopic, house, meterJSON(42, 2, at.Add(5*time.Second), 2.5)),
		rec(meterTopic, "grid99-house0001", strings.ReplaceAll(meterJSON(42, 3, at, 1), "grid01", "grid99")), // unknown grid
		rec(meterTopic, house, meterJSON(42, 3, at.Add(10*time.Second), 3.5)),
	)

	stop := start()
	eventually(t, "records to be processed", 90*time.Second, func() bool {
		return scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.meter_readings`) == 3 &&
			scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.ingestion_failures`) == 2
	})

	// Ordered, correct final state: the poison record neither blocked the
	// partition nor was lost.
	latest, err := e.rd.GetLatest(ctx, "grid01", house)
	if err != nil || latest.Seq != 3 || latest.NetKw != 3.5 {
		t.Fatalf("latest = %+v, err = %v; want seq 3", latest, err)
	}
	stages := scalar[string](t, e.dsn, `SELECT string_agg(failure_stage, ',' ORDER BY failure_stage) FROM iot_data.ingestion_failures`)
	if stages != "decode,grid_validation" {
		t.Errorf("failure stages = %q, want decode,grid_validation", stages)
	}
	if _, err := e.rd.GetHouseStatus(ctx, house); err != nil {
		t.Errorf("heartbeat did not reach redis: %v", err)
	}

	// Graceful stop commits finished work. Give the last commit a moment.
	time.Sleep(time.Second)
	stop()
	processed := meterH.calls.Load() + hbH.calls.Load()

	// New record while the consumer is down, then restart in the same group.
	produce(t, broker, rec(meterTopic, house, meterJSON(42, 4, at.Add(15*time.Second), 4.5)))
	stop = start()
	defer stop()

	eventually(t, "the new record after restart", 90*time.Second, func() bool {
		return scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.meter_readings`) == 4
	})
	time.Sleep(2 * time.Second)

	if again := meterH.calls.Load() + hbH.calls.Load() - processed; again != 1 {
		t.Errorf("after restart %d record(s) were handled, want exactly the 1 new one (committed offsets were not honoured)", again)
	}
	if n := scalar[int](t, e.dsn, `SELECT count(*) FROM iot_data.ingestion_failures`); n != 2 {
		t.Errorf("failures = %d after restart, want still 2", n)
	}
}

func mustRegistry(t *testing.T, pg *postgresstore.Store) *admission.Registry {
	t.Helper()
	r := admission.NewRegistry()
	if err := admission.NewRefresher(r, pg, admission.RefresherConfig{}, quietLogger()).Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}
