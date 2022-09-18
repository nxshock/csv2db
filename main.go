package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/jessevdk/go-flags"
)

var db *sql.DB
var opts struct {
	FilePath       string `short:"f" long:"file" description:"CSV file path" required:"true"`
	ServerAddress  string `short:"s" long:"server" description:"server address" default:"127.0.0.1"`
	DatabaseName   string `short:"d" long:"database" description:"database name" required:"true"`
	TableName      string `short:"t" long:"table" description:"table name" required:"true"`
	FieldTypes     string `short:"l" long:"fields" description:"field types" required:"true"`
	Comma          string `short:"c" long:"comma" description:"CSV file comma character" choice:"," choice:";" choice:"t" default:","`
	CreateTable    bool   `short:"x" long:"create" description:"create table"`
	OverwriteTable bool   `short:"o" long:"overwrite" description:"overwrite existing table"`
	Encoding       string `short:"e" long:"encoding" description:"CSV file charset" choice:"utf8" choice:"win1251" default:"utf8"`
	SkipRows       int    `short:"r" long:"skiprows" description:"number of rows to skip"`
}

func init() {
	log.SetFlags(0)
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		os.Exit(1)
	}

	db, err = sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s?database=%s", opts.ServerAddress, opts.DatabaseName))
	if err != nil {
		log.Fatalln(fmt.Errorf("open database: %v", err))
	}
	defer db.Close()

	switch strings.ToLower(filepath.Ext(opts.FilePath)) {
	case ".zip":
		err = processZipFile(opts.FilePath)
	case ".csv":
		err = processCsvFile(opts.FilePath)
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func processReader(r io.Reader) error {
	var encoding Encoding
	err := encoding.UnmarshalText([]byte(opts.Encoding))
	if err != nil {
		return fmt.Errorf("get decoder: %v", opts.Encoding)
	}

	decoder, err := encoding.Translate(r)
	if err != nil {
		return fmt.Errorf("enable decoder: %v", opts.Encoding)
	}

	bufReader := bufio.NewReaderSize(decoder, 4*1024*1024)

	reader := csv.NewReader(bufReader)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = len(opts.FieldTypes)

	if []rune(opts.Comma)[0] == 't' {
		reader.Comma = '\t'
	} else {
		reader.Comma = []rune(opts.Comma)[0]
	}

	for i := 0; i < opts.SkipRows; i++ {
		_, err := reader.Read()
		if err == csv.ErrFieldCount {
			continue
		}
		if err != nil {
			return fmt.Errorf("skip rows: %v", err)
		}
	}

	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %v", err)
	}

	headerList := `"`
	for i, v := range header {
		if opts.FieldTypes[i] == ' ' {
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
	for i, v := range header {
		if opts.FieldTypes[i] == ' ' {
			continue
		}

		neededHeader = append(neededHeader, v)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("start transaction: %v", err)
	}

	if opts.CreateTable {
		err = createTable(tx, header)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("create table: %v", err)
		}
	}

	sql := mssql.CopyIn(opts.TableName, mssql.BulkOptions{Tablock: true}, neededHeader...)

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
			err = fieldType.UnmarshalText([]byte{opts.FieldTypes[i]})
			if err != nil {
				return fmt.Errorf("get record type: %v", err)
			}
			if fieldType == Skip {
				continue
			}

			parsedValue, err := fieldType.ParseValue(v)
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
