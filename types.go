package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type FieldType int

const (
	Skip FieldType = iota
	Integer
	String
	Float
	Money
	Date
	Timestamp
)

func (ft FieldType) ParseValue(s string) (any, error) {
	s = strings.TrimSpace(s)

	if s == "" {
		return nil, nil
	}

	switch ft {
	case String:
		return s, nil
	case Integer:
		return strconv.ParseInt(s, 10, 64)
	case Float:
		return strconv.ParseFloat(strings.ReplaceAll(s, ",", "."), 64)
	case Date:
		return time.Parse(opts.DateFormat, s)
	case Timestamp:
		return time.Parse(opts.TimestampFormat, s)
	}

	return nil, fmt.Errorf("unknown type id = %d", ft)
}

func (ft FieldType) SqlFieldType() string {
	switch ft {
	case Integer:
		return "bigint"
	case String:
		return "nvarchar(255)"
	case Float:
		return "float"
	case Money:
		panic("do not implemented - see https://github.com/denisenkom/go-mssqldb/issues/460") // TODO: https://github.com/denisenkom/go-mssqldb/issues/460
	case Date:
		return "date"
	case Timestamp:
		return "datetime2"
	}

	return ""
}

func (ft FieldType) MarshalText() (text []byte, err error) {
	switch ft {
	case Skip:
		return []byte(" "), nil
	case Integer:
		return []byte("i"), nil
	case String:
		return []byte("s"), nil
	case Float:
		return []byte("f"), nil
	case Money:
		return []byte("m"), nil
	case Date:
		return []byte("d"), nil
	case Timestamp:
		return []byte("t"), nil
	}

	return nil, fmt.Errorf("unknown type id = %d", ft)
}

func (ft *FieldType) UnmarshalText(text []byte) error {
	switch string(text) {
	case " ":
		*ft = Skip
		return nil
	case "i":
		*ft = Integer
		return nil
	case "s":
		*ft = String
		return nil
	case "f":
		*ft = Float
		return nil
	case "m":
		*ft = Money
		return nil
	case "d":
		*ft = Date
		return nil
	case "t":
		*ft = Timestamp
		return nil
	}

	return fmt.Errorf(`unknown format code "%s"`, string(text))
}
