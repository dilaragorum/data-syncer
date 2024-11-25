package datasyncer

import (
	"context"
	"github.com/segmentio/kafka-go"
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
					t.producer.WriteMessages(context.Background(), toBeProduced...)
				}
				return nil
			}

			toBeProduced = append(toBeProduced, kafka.Message{Value: data})

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
