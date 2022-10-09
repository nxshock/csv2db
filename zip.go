package main

import (
	"archive/zip"
	"fmt"

	"github.com/urfave/cli/v2"
)

func processZipFile(c *cli.Context, filePath string) error {
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return fmt.Errorf("open ZIP file: %v", err)
	}

	if len(r.File) != 1 {
		return fmt.Errorf("supported only one file in archive, got %d files", len(r.File))
	}

	zipFileReader, err := r.File[0].Open()
	if err != nil {
		return fmt.Errorf("open file from ZIP archive: %v", err)
	}
	defer zipFileReader.Close()

	return processReader(c, zipFileReader)
}
