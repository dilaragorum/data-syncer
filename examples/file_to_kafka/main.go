package main

import (
	"datasyncer"
	"log"
)

func main() {
	syncer := datasyncer.New(
		datasyncer.NewSourceFile(&datasyncer.SourceFileConfig{
			Path: "examples/file_to_kafka/example.txt",
		}),
		datasyncer.NewTargetKafka(&datasyncer.KafkaProducerConfig{
			Writer: datasyncer.WriterConfig{
				Brokers: []string{"localhost:29092"},
				Topic:   "file-to-kafka-test-topic",
			},
		}),
		nil,
	)

	if err := syncer.Sync(); err != nil {
		log.Fatal(err)
	}
}
