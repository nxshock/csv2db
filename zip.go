package main

import (
	"archive/zip"
	"fmt"
)

func processZipFile(filePath string) error {
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return err
	}

	if len(r.File) != 1 {
		return fmt.Errorf("supported only one file in archive, got %d files", len(r.File))
	}

	zipFileReader, err := r.File[0].Open()
	if err != nil {
		return err
	}
	defer zipFileReader.Close()

	return processReader(zipFileReader)
}
