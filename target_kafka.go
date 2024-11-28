package datasyncer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/tidwall/gjson"
	"os"
	"time"
)

type TargetKafkaOption func(*targetKafka)

type targetKafka struct {
	producer     *kafka.Writer
	batchTimeout time.Duration
}

// TODO:adam düz text verirse dosyadan doğrudan value olacak şekilde geliştirme yapalım
func NewTargetKafka(options ...TargetKafkaOption) DataTarget {
	t := &targetKafka{
		producer: &kafka.Writer{
			BatchSize:    100,
			BatchTimeout: time.Nanosecond,
			Transport:    &kafka.Transport{},
		},
		batchTimeout: time.Second,
	}

	for _, opt := range options {
		opt(t)
	}

	return t
}

func WithBrokers(brokers []string) TargetKafkaOption {
	return func(t *targetKafka) {
		t.producer.Addr = kafka.TCP(brokers...)
	}
}

func WithSASL(algorithm scram.Algorithm, username string, password string) TargetKafkaOption {
	mechanism, _ := scram.Mechanism(algorithm, username, password)

	return func(t *targetKafka) {
		t.producer.Transport.(*kafka.Transport).SASL = mechanism
	}
}

func WithTLSConfig(rootCAPath string, intermediateCAPath string) TargetKafkaOption {
	rootCA, _ := os.ReadFile(rootCAPath)
	interCA, _ := os.ReadFile(intermediateCAPath)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)
	caCertPool.AppendCertsFromPEM(interCA)

	return func(t *targetKafka) {
		t.producer.Transport.(*kafka.Transport).TLS = &tls.Config{
			RootCAs: caCertPool,
		}
	}
}

func WithTopic(topic string) TargetKafkaOption {
	return func(t *targetKafka) {
		t.producer.Topic = topic
	}
}

func (t *targetKafka) Send(input <-chan []byte) error {
	ticker := time.Tick(t.batchTimeout)

	var toBeProduced []kafka.Message
	for {
		select {
		case data, isOpened := <-input:
			if !isOpened {
				if len(toBeProduced) != 0 {
					err := t.producer.WriteMessages(context.Background(), toBeProduced...)
					_ = err
				}
				return nil
			}

			toBeProduced = append(toBeProduced, kafka.Message{
				Key:   []byte(gjson.GetBytes(data, "key").String()),
				Value: []byte(gjson.GetBytes(data, "value").String()),
			})

			if len(toBeProduced) == t.producer.BatchSize {
				t.producer.WriteMessages(context.Background(), toBeProduced...)
				toBeProduced = []kafka.Message{}
			}
		case <-ticker:
			if len(toBeProduced) == 0 {
				continue
			}

			t.producer.WriteMessages(context.Background(), toBeProduced...)
			toBeProduced = []kafka.Message{}
		}
	}
}
