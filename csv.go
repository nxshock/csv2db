package main

import (
	"fmt"
	"os"
)

func processCsvFile(filePath string) error {
	f, err := os.Open(opts.FilePath)
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	defer f.Close()

	return processReader(f)
}
