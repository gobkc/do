package excel

import (
	"fmt"
	"io"

	"github.com/xuri/excelize/v2"
)

const maxRowsPerSheet = 1048576

type StreamExport struct {
	file       *excelize.File
	stream     *excelize.StreamWriter
	sheetIndex int
	currentRow int
	headers    []any
}

func New() *StreamExport {
	return &StreamExport{
		file:       excelize.NewFile(),
		sheetIndex: 1,
	}
}

func (e *StreamExport) initSheet() error {
	sheetName := fmt.Sprintf("Sheet%d", e.sheetIndex)
	if e.sheetIndex == 1 {
		e.file.SetSheetName("Sheet1", sheetName)
	} else {
		_, err := e.file.NewSheet(sheetName)
		if err != nil {
			return err
		}
	}

	sw, err := e.file.NewStreamWriter(sheetName)
	if err != nil {
		return err
	}
	e.stream = sw
	e.currentRow = 1

	if len(e.headers) > 0 {
		cell, _ := excelize.CoordinatesToCellName(1, e.currentRow)
		if err := e.stream.SetRow(cell, e.headers); err != nil {
			return err
		}
		e.currentRow++
	}
	return nil
}

func (e *StreamExport) WriteHeaders(headers []interface{}) error {
	e.headers = headers
	return e.initSheet()
}

func (e *StreamExport) WriteRow(values []interface{}) error {
	if e.stream == nil {
		if err := e.initSheet(); err != nil {
			return err
		}
	}

	if e.currentRow > maxRowsPerSheet {
		if err := e.stream.Flush(); err != nil {
			return err
		}
		e.sheetIndex++
		if err := e.initSheet(); err != nil {
			return err
		}
	}

	cell, _ := excelize.CoordinatesToCellName(1, e.currentRow)
	if err := e.stream.SetRow(cell, values); err != nil {
		return err
	}
	e.currentRow++
	return nil
}

func (e *StreamExport) Flush(w io.Writer) error {
	if e.stream != nil {
		if err := e.stream.Flush(); err != nil {
			return err
		}
	}
	return e.file.Write(w)
}

func (e *StreamExport) Close() error {
	return e.file.Close()
}
