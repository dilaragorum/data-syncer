package datasyncer

import (
	"bufio"
	"fmt"
	"os"
)

type SourceFileConfig struct {
	Path string
}

type sourceFile struct {
	sourceFileConfig *SourceFileConfig
}

func NewSourceFile(config *SourceFileConfig) DataSource {
	return &sourceFile{sourceFileConfig: config}
}

func (s *sourceFile) Fetch(c chan<- []byte) error {
	file, err := os.Open(s.sourceFileConfig.Path)
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
