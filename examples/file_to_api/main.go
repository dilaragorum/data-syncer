package main

import (
	"datasyncer"
	"log"
)

func main() {
	errFunc := func(data []byte, err error) {
		log.Printf("Error sending data: %v", err)
	}

	syncer := datasyncer.New(
		datasyncer.NewSourceFile(
			datasyncer.WithPath("examples/file_to_api/example.txt"),
		),
		datasyncer.NewTargetApi("https://jsonplaceholder.typicode.com/posts",
			datasyncer.WithErrorHandler(errFunc),
		),
	)

	if err := syncer.Sync(); err != nil {
		log.Fatal(err)
	}
}
