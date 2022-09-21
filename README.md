# csv2db

Bulk CSV files uploader into Microsoft SQL Server.

## Usage

```
Usage:
  csv2db [OPTIONS]

Application Options:
      /filepath:                CSV file path
      /server:                  server address (default: 127.0.0.1)
      /database:                database name
      /table:                   table name in schema.name format
      /fields:                  field types in [sifdt ] format
      /comma:[,|;|t]            CSV file comma character (default: ,)
      /create                   create table
      /overwrite                overwrite existing table
      /encoding:[utf8|win1251]  CSV file charset (default: utf8)
      /skiprows:                number of rows to skip
      /dateformat:              date format (Go style) (default: 02.01.2006)
      /timestampformat:         timestamp format  (Go style) (default: 02.01.2006 15:04:05)
      /unknowncolumnnames       insert to table with unknown column names
```

## Build

Use `make.bat` file to build `csv2db.exe` executable.
