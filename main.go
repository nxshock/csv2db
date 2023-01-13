package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/urfave/cli/v2"
)

var db *sql.DB

var app = &cli.App{
	Usage:    "bulk CSV files uploader into Microsoft SQL Server",
	HideHelp: true,
	Version:  VERSION,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:      "filepath",
			Usage:     "CSV file path",
			Required:  true,
			TakesFile: true},
		&cli.StringFlag{
			Name:  "server",
			Usage: "database server address",
			Value: "127.0.0.1"},
		&cli.StringFlag{
			Name:     "database",
			Usage:    "database name",
			Required: true},
		&cli.StringFlag{
			Name:     "table",
			Usage:    "table name in schema.name format",
			Required: true},
		&cli.StringFlag{
			Name:     "fields",
			Usage:    "list of field types in [sifdt ]+ format",
			Required: true},
		&cli.StringFlag{
			Name:  "comma",
			Usage: `CSV file comma character (use 't' for tabs)`,
			Value: ","},
		&cli.BoolFlag{
			Name:  "create",
			Usage: "create table"},
		&cli.BoolFlag{
			Name:  "overwrite",
			Usage: "overwrite existing table"},
		&cli.StringFlag{
			Name:  "encoding",
			Usage: `CSV file charset ("utf8", "win1251")`,
			Value: "utf8"},
		&cli.IntFlag{
			Name:  "skiprows",
			Usage: "number of rows to skip before read CSV file header"},
		&cli.StringFlag{
			Name:  "dateformat",
			Usage: "date format (Go style)",
			Value: "02.01.2006"},
		&cli.StringFlag{
			Name:  "timestampformat",
			Usage: "timestamp format (Go style)",
			Value: "02.01.2006 15:04:05"},
		&cli.BoolFlag{
			Name:  "unknowncolumnnames",
			Usage: "insert to table with unknown column names",
		},
	},
	Action: func(c *cli.Context) error {
		if len(strings.Split(c.String("table"), ".")) != 2 {
			return errors.New("table name must be in schema.name format")
		}

		var err error

		db, err = sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s?database=%s", c.String("server"), c.String("database")))
		if err != nil {
			return fmt.Errorf("open database: %v", err)
		}
		defer db.Close()

		filePath := c.String("filepath")
		switch strings.ToLower(filepath.Ext(filePath)) {
		case ".zip":
			err = processZipFile(c, filePath)
		case ".csv":
			err = processCsvFile(c, filePath)
		}
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	log.SetFlags(0)
}

func main() {
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func processReader(c *cli.Context, r io.Reader) error {
	var encoding Encoding

	err := encoding.UnmarshalText([]byte(c.String("encoding")))
	if err != nil {
		return fmt.Errorf("get decoder: %v", c.String("encoding"))
	}

	decoder, err := encoding.Translate(r)
	if err != nil {
		return fmt.Errorf("enable decoder: %v", c.String("encoding"))
	}

	bufReader := bufio.NewReaderSize(decoder, 4*1024*1024)

	for i := 0; i < c.Int("skiprows"); i++ {
		_, _, err := bufReader.ReadLine()
		if err != nil {
			return fmt.Errorf("skip rows: %v", err)
		}
	}

	reader := csv.NewReader(bufReader)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = false
	reader.FieldsPerRecord = len(c.String("fields"))

	if runes := []rune(c.String("comma")); len(runes) > 0 && runes[0] == 't' {
		reader.Comma = '\t'
	} else {
		reader.Comma = []rune(c.String("comma"))[0]
	}

	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %v", err)
	}

	headerList := `"`
	for i, v := range header {
		if c.String("fields")[i] == ' ' {
			continue
		}

		headerList += v

		if i+1 < len(header) {
			headerList += `", "`
		} else {
			headerList += `"`
		}
	}

	var neededHeader []string

	if c.Bool("unknowncolumnnames") {
		sql := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION", c.String("table"))
		rows, err := db.Query(sql)
		if err != nil {
			return fmt.Errorf("get column names from database: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			if rows.Err() != nil {
				return fmt.Errorf("get column names from database: %v", err)
			}
			var columnName string
			err = rows.Scan(&columnName)
			if err != nil {
				return fmt.Errorf("get column names from database: %v", err)
			}
			neededHeader = append(neededHeader, columnName)
		}
	} else {
		for i, v := range header {
			if c.String("fields")[i] == ' ' {
				continue
			}

			neededHeader = append(neededHeader, v)
		}
	}

	if len(neededHeader) == 0 {
		return fmt.Errorf("no columns to process (check table name or field types)")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("start transaction: %v", err)
	}

	if c.Bool("create") {
		err = createTable(tx, c.String("table"), header, c.String("fields"), c.Bool("overwrite"))
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("create table: %v", err)
		}
	}

	sql := mssql.CopyIn(c.String("table"), mssql.BulkOptions{Tablock: true}, neededHeader...)

	stmt, err := tx.Prepare(sql)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare statement: %v", err)
	}

	n := 0
	for {
		if n%100000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed %d records...\r", n)
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read record: %v", err)
		}

		var args []any

		for i, v := range record {
			var fieldType FieldType
			err = fieldType.UnmarshalText([]byte{c.String("fields")[i]})
			if err != nil {
				return fmt.Errorf("get record type: %v", err)
			}
			if fieldType == Skip {
				continue
			}

			parsedValue, err := fieldType.ParseValue(c, v)
			if err != nil {
				return fmt.Errorf("parse value: %v", err)
			}

			args = append(args, parsedValue)
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return fmt.Errorf("execute statement: %v", err)
		}
		n++
	}
	result, err := stmt.Exec()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("execute statement: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("calc rows affected: %v", err)
	}
	fmt.Fprintf(os.Stderr, "Processed %d records.  \n", rowsAffected)

	err = stmt.Close()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close statement: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit: %v", err)
	}

	return nil
}
