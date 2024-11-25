package datasyncer

import (
	"bufio"
	"fmt"
	"os"
)

type SourceFileConfig struct {
	Path string
}

type SourceFileOption func(*sourceFile)

type sourceFile struct {
	Path string
}

func NewSourceFile(options ...SourceFileOption) DataSource {
	sf := &sourceFile{}

	for _, opt := range options {
		opt(sf)
	}

	return sf
}

func WithPath(path string) SourceFileOption {
	return func(sf *sourceFile) {
		sf.Path = path
	}
}

func (s *sourceFile) Fetch(c chan<- []byte) error {
	file, err := os.Open(s.Path)
	if err != nil {
		return fmt.Errorf("error opening the file %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		c <- scanner.Bytes()
	}

	close(c)
	return nil
}
