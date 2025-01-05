package datasyncer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
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

func NewTargetKafka(options ...TargetKafkaOption) DataTarget {
	t := &targetKafka{
		producer: &kafka.Writer{
			BatchSize:    100,
			BatchTimeout: time.Nanosecond,
			Transport:    &kafka.Transport{},
			Completion: func(messages []kafka.Message, err error) {
				if err != nil {
					fmt.Printf("Failed to delivered %d number of messages \n", len(messages))
					return
				}
				fmt.Printf("Successfully delivered %d number of messages \n", len(messages))
			},
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

	flush := func() error {
		if len(toBeProduced) == 0 {
			return nil
		}

		if err := t.producer.WriteMessages(context.Background(), toBeProduced...); err != nil {
			return fmt.Errorf("failed to write messages: %w", err)
		}

		toBeProduced = toBeProduced[:0]
		return nil
	}

	for {
		select {
		case data, isOpened := <-input:
			if !isOpened {
				for leftData := range input {
					toBeProduced = append(toBeProduced, kafka.Message{Value: leftData})
				}

				return flush()
			}

			kafkaMsg := kafka.Message{
				Key: []byte(gjson.GetBytes(data, "key").String()),
			}

			value := gjson.GetBytes(data, "value")
			if value.Exists() {
				kafkaMsg.Value = []byte(value.String())
			} else {
				kafkaMsg.Value = data
			}

			toBeProduced = append(toBeProduced, kafkaMsg)

			if len(toBeProduced) >= t.producer.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
