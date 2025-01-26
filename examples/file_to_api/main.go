package main

import (
	"datasyncer"
	"log"
	"strings"
)

func main() {
	errFunc := func(data []byte, err error) {
		log.Printf("Error sending data: %v", err)
	}

	templateFunc := func(data []byte) []interface{} {
		parts := strings.Split(strings.TrimSpace(string(data)), ",")
		return []interface{}{parts[0], parts[1], parts[2]}
	}

	syncer := datasyncer.New(
		datasyncer.NewSourceFile(
			datasyncer.WithPath("examples/file_to_api/example.txt"),
		),
		datasyncer.NewTargetApi("https://jsonplaceholder.typicode.com/posts",
			datasyncer.WithErrorHandler(errFunc),
			datasyncer.WithTemplateHandler(
				`{"userId":%s, "title":%s,"body":%s}`,
				templateFunc,
			),
		),
	)

	if err := syncer.Sync(); err != nil {
		log.Fatal(err)
	}
}
