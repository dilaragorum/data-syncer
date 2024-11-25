package main

import (
	"datasyncer"
	"encoding/json"
	"log"
	"strconv"
)

func main() {
	syncer := datasyncer.NewWithTransformFunc(
		datasyncer.NewSourceFile(
			datasyncer.WithPath("examples/file_to_kafka_with_transform/example.txt"),
		),
		datasyncer.NewTargetKafka(
			datasyncer.WithBrokers([]string{"localhost:29092"}),
			datasyncer.WithTopic("file-to-kafka-test-topic"),
		),
		CustomTransformFunc,
	)

	if err := syncer.Sync(); err != nil {
		log.Fatal(err)
	}
}

func CustomTransformFunc(incomingMessage []byte) ([]byte, error) {
	messageInt, _ := strconv.Atoi(string(incomingMessage))
	messageInt *= 10

	return json.Marshal(messageInt)
}
