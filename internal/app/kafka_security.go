package app

import (
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/config"
)

// kafkaSecurityOpts maps TLS/SASL settings to franz-go options. Plaintext is
// a development-only default; production validation rejects it.
func kafkaSecurityOpts(c config.KafkaConfig) ([]kgo.Opt, error) {
	var opts []kgo.Opt

	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}

	switch c.SASLMech {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", c.SASLMech)
	}
	return opts, nil
}
