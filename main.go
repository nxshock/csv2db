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
	FilePath           string `long:"filepath" description:"CSV file path" required:"true"`
	ServerAddress      string `long:"server" description:"server address" default:"127.0.0.1"`
	DatabaseName       string `long:"database" description:"database name" required:"true"`
	TableName          string `long:"table" description:"table name in schema.name format" required:"true"`
	FieldTypes         string `long:"fields" description:"field types in [sifdt ] format" required:"true"`
	Comma              string `long:"comma" description:"CSV file comma character" choice:"," choice:";" choice:"t" default:","`
	CreateTable        bool   `long:"create" description:"create table"`
	OverwriteTable     bool   `long:"overwrite" description:"overwrite existing table"`
	Encoding           string `long:"encoding" description:"CSV file charset" choice:"utf8" choice:"win1251" default:"utf8"`
	SkipRows           int    `long:"skiprows" description:"number of rows to skip"`
	DateFormat         string `long:"dateformat" description:"date format (Go style)" default:"02.01.2006"`
	TimestampFormat    string `long:"timestampformat" description:"timestamp format  (Go style)" default:"02.01.2006 15:04:05"`
	UnknownColumnNames bool   `long:"unknowncolumnnames" description:"insert to table with unknown column names"`
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

	for i := 0; i < opts.SkipRows; i++ {
		_, _, err := bufReader.ReadLine()
		if err != nil {
			return fmt.Errorf("skip rows: %v", err)
		}
	}

	reader := csv.NewReader(bufReader)
	reader.TrimLeadingSpace = false
	reader.FieldsPerRecord = len(opts.FieldTypes)

	if []rune(opts.Comma)[0] == 't' {
		reader.Comma = '\t'
	} else {
		reader.Comma = []rune(opts.Comma)[0]
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

	if opts.UnknownColumnNames {
		sql := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION", opts.TableName)
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
			if opts.FieldTypes[i] == ' ' {
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

	if opts.CreateTable {
		err = createTable(tx, header, opts.FieldTypes)
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
