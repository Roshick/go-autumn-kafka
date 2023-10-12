package aukafka

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func defaultTopicOptions(config TopicConfig) []kgo.Opt {
	tlsDialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config:    &tls.Config{InsecureSkipVerify: true},
	}

	var mechanism sasl.Mechanism
	switch config.AuthType {
	case "sha256":
		mechanism = scram.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsSha256Mechanism()
	case "plain":
		fallthrough
	default:
		mechanism = plain.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsMechanism()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.SASL(mechanism),
		kgo.Dialer(tlsDialer.DialContext),
		kgo.SessionTimeout(30 * time.Second),
		kgo.WithLogger(Logger{}),
	}

	return opts
}
