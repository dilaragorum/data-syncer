package main

import (
	"datasyncer"
	"encoding/json"
	"log"
	"strconv"
)

func main() {
	syncer := datasyncer.New(
		datasyncer.NewSourceFile(&datasyncer.SourceFileConfig{
			Path: "examples/file_to_kafka_with_transform/example.txt",
		}),
		datasyncer.NewTargetKafka(&datasyncer.KafkaProducerConfig{
			Writer: datasyncer.WriterConfig{
				Brokers: []string{"localhost:29092"},
				Topic:   "file-to-kafka-test-topic",
			},
		}),
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
