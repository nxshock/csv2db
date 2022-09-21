package main

import (
	"fmt"
	"io"

	"github.com/dimchansky/utfbom"
	"golang.org/x/text/encoding/charmap"
)

type Encoding int

const (
	Utf8 Encoding = iota
	Win1251
)

func (e Encoding) Translate(r io.Reader) (io.Reader, error) {
	switch e {
	case Utf8:
		return utfbom.SkipOnly(r), nil
	case Win1251:
		return charmap.Windows1251.NewDecoder().Reader(r), nil
	}

	return nil, fmt.Errorf("unknown encoding id = %d", e)
}

func (e Encoding) MarshalText() (text []byte, err error) {
	switch e {
	case Utf8:
		return []byte("utf8"), nil
	case Win1251:
		return []byte("win1251"), nil
	}

	return nil, fmt.Errorf("unknown encoding id = %d", e)
}

func (e *Encoding) UnmarshalText(text []byte) error {
	switch string(text) {
	case "utf8":
		*e = Utf8
		return nil
	case "win1251":
		*e = Win1251
		return nil
	}

	return fmt.Errorf("unknown encoding: %s", string(text))
}
