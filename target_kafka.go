package datasyncer

import (
	"context"
	"fmt"
	"github.com/Trendyol/kafka-konsumer/v2"
)

const MAX_BATCH_SIZE int = 100

type KafkaProducerConfig struct {
	Writer                    WriterConfig
	DistributedTracingConfig  DistributedTracingConfig
	Transport                 TransportConfig
	SASL                      SASLConfig
	TLS                       TLSConfig
	ClientID                  string
	DistributedTracingEnabled bool
}

type (
	DistributedTracingConfig kafka.DistributedTracingConfiguration
	WriterConfig             kafka.WriterConfig
	TransportConfig          kafka.TransportConfig
	SASLConfig               kafka.SASLConfig
	TLSConfig                kafka.TLSConfig
)

type targetKafka struct {
	producer kafka.Producer
}

func NewTargetKafka(producerConfig *KafkaProducerConfig) DataTarget {
	producer, _ := kafka.NewProducer(&kafka.ProducerConfig{
		Writer: kafka.WriterConfig(producerConfig.Writer),
	})
	return &targetKafka{producer: producer}
}

// TODO: bu error client callback vs. verilip devam edilebilir, return ile tamamen kesiyoruz.
func (t *targetKafka) Send(input <-chan []byte) error {
	messages := make([]kafka.Message, 0, MAX_BATCH_SIZE)

	for data := range input {
		message := kafka.Message{
			Value: data,
		}

		messages = append(messages, message)

		if len(messages) > MAX_BATCH_SIZE {
			err := t.producer.ProduceBatch(context.Background(), messages)
			if err != nil {
				return err
			}
			messages = make([]kafka.Message, 0, 100)
		}
	}

	err := t.producer.ProduceBatch(context.Background(), messages)
	if err != nil {
		return fmt.Errorf("cannot produce batch messages: %w", err)
	}

	return nil
}
