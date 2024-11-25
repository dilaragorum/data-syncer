package main

import (
	"datasyncer"
	"log"
)

func main() {
	syncer := datasyncer.New(
		datasyncer.NewSourceFile(
			datasyncer.WithPath("examples/file_to_kafka/example.txt"),
		),
		datasyncer.NewTargetKafka(
			datasyncer.WithBrokers([]string{"localhost:29092"}),
			datasyncer.WithTopic("file-to-kafka-test-topic"),
		),
	)

	if err := syncer.Sync(); err != nil {
		log.Fatal(err)
	}
}
