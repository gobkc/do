package exporter

import "io"

type DocumentExporter interface {
	WriteHeaders(headers []interface{}) error
	WriteRow(values []interface{}) error
	Flush(w io.Writer) error
	Close() error
}
