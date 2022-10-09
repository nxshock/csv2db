package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func processCsvFile(c *cli.Context, filePath string) error {
	f, err := os.Open(c.String("filepath"))
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	defer f.Close()

	return processReader(c, f)
}
