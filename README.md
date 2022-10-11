# csv2db

Bulk CSV files uploader into Microsoft SQL Server.

## Usage

```
NAME:
   csv2db.exe - bulk CSV files uploader into Microsoft SQL Server

USAGE:
   csv2db.exe [global options] [arguments...]

VERSION:
   0.1.1

GLOBAL OPTIONS:
   --comma value            CSV file comma character (use 't' for tabs) (default: ",")
   --create                 create table (default: false)
   --database value         database name
   --dateformat value       date format (Go style) (default: "02.01.2006")
   --encoding value         CSV file charset ("utf8", "win1251") (default: "utf8")
   --fields value           list of field types in [sifdt ]+ format
   --filepath value         CSV file path
   --overwrite              overwrite existing table (default: false)
   --server value           database server address (default: "127.0.0.1")
   --skiprows value         number of rows to skip before read CSV file header (default: 0)
   --table value            table name in schema.name format
   --timestampformat value  timestamp format (Go style) (default: "02.01.2006 15:04:05")
   --unknowncolumnnames     insert to table with unknown column names (default: false)
   --version, -v            print the version (default: false)
```

## Build

Use `make.bat` file to build `csv2db.exe` executable.
