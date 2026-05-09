package exporter

import "io"

type TableWriter interface {
	SetupSheet(sheetName string, headers []string) error
	WriteRow(row []any) error
	Flush() error
	Save(w io.Writer) error
}
