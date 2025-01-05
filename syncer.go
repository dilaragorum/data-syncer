package datasyncer

import "fmt"

type DataSource interface {
	Fetch(chan<- []byte) error
}

type DataTarget interface {
	Send(<-chan []byte) error
}

type TransformFunc func([]byte) ([]byte, error)

type ErrorHandlingFunc func([]byte) ([]byte, error)

type DataSyncer struct {
	source        DataSource
	target        DataTarget
	transform     func([]byte) ([]byte, error)
	errorHandling func([]byte) ([]byte, error)
}

func New(source DataSource, target DataTarget) *DataSyncer {
	return &DataSyncer{
		source: source,
		target: target,
	}
}

func NewWithTransformFunc(source DataSource, target DataTarget, transform TransformFunc) *DataSyncer {
	return &DataSyncer{
		source:    source,
		target:    target,
		transform: transform,
	}
}

func (ds *DataSyncer) Sync() error {
	dataChan := make(chan []byte)
	transformedChan := make(chan []byte)

	// Start fetching data
	// TODO: concurrency parametresi buraya nasıl yediririz, düşünelim.
	go func() {
		err := ds.source.Fetch(dataChan)
		if err != nil {
			close(dataChan)
			fmt.Printf("Error fetching data: %s\n", err)
		}
	}()

	// Start transforming data
	go func() {
		for data := range dataChan {
			if ds.transform != nil {
				transformedData, err := ds.transform(data)
				if err != nil {
					fmt.Printf("Error transforming data: %s\n", err)
					continue
				}
				transformedChan <- transformedData
			} else {
				transformedChan <- data
			}
		}
		close(transformedChan)
	}()

	// Start sending data
	err := ds.target.Send(transformedChan)
	if err != nil {
		return err
	}

	return nil
}
