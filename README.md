# csv2db

Bulk CSV files uploader into Microsoft SQL Server.

## Usage

```
Usage:
  csv2db [OPTIONS]

Application Options:
  /f, /file:                    CSV file path
  /s, /server:                  server address (default: 127.0.0.1)
  /d, /database:                database name
  /t, /table:                   table name
  /l, /fields:                  field types
  /c, /comma:[,|;|t]            CSV file comma character (default: ,)
  /x, /create                   create table
  /o, /overwrite                overwrite existing table
  /e, /encoding:[utf8|win1251]  CSV file charset (default: utf8)
  /r, /skiprows:                number of rows to skip
```

## Build

Use `make.bat` file to build `csv2db.exe` executable.