package datasyncer

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchTimeout time.Duration
}

type targetKafka struct {
	producer     *kafka.Writer
	batchTimeout time.Duration
}

func NewTargetKafka(producerConfig *KafkaProducerConfig) DataTarget {
	if producerConfig.BatchSize <= 0 {
		producerConfig.BatchSize = 100
	}

	producer := &kafka.Writer{
		Topic:        producerConfig.Topic,
		Addr:         kafka.TCP(producerConfig.Brokers...),
		BatchSize:    producerConfig.BatchSize,
		BatchTimeout: time.Nanosecond,
	}

	target := &targetKafka{producer: producer}

	if producerConfig.BatchTimeout <= 0 {
		target.batchTimeout = time.Second
	} else {
		target.batchTimeout = producerConfig.BatchTimeout
	}

	return target
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
