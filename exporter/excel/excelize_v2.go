package excelizev2

import (
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/xuri/excelize/v2"
)

const maxRowsPerSheet = 1000000

type StreamExport struct {
	file          *excelize.File
	stream        *excelize.StreamWriter
	sheetIndex    int
	currentRow    int
	headers       []any
	colWidths     map[int]float64
	headerStyleID int
	bodyStyleID   int
}

func New() *StreamExport {
	f := excelize.NewFile()

	headerStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold:  true,
			Color: "#FFFFFF",
			Size:  14,
		},
		Border: []excelize.Border{
			{Type: "left", Color: "#0188fb", Style: 2},
			{Type: "top", Color: "#0188fb", Style: 2},
			{Type: "right", Color: "#0188fb", Style: 2},
			{Type: "bottom", Color: "#0188fb", Style: 2},
		},
		Fill: excelize.Fill{
			Type:    "pattern",
			Color:   []string{"#0188fb"},
			Pattern: 1,
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
			WrapText:   true,
		},
	})

	bodyStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Color: "#000000",
			Size:  12,
		},
		Border: []excelize.Border{
			{Type: "left", Color: "#000000", Style: 3},
			{Type: "top", Color: "#000000", Style: 3},
			{Type: "right", Color: "#000000", Style: 3},
			{Type: "bottom", Color: "#000000", Style: 3},
		},
		Alignment: &excelize.Alignment{
			Horizontal: "left",
			Vertical:   "center",
			WrapText:   true,
		},
	})

	return &StreamExport{
		file:          f,
		sheetIndex:    1,
		colWidths:     make(map[int]float64),
		headerStyleID: headerStyle,
		bodyStyleID:   bodyStyle,
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
	e.colWidths = make(map[int]float64)
	if len(e.headers) > 0 {
		return e.writeRowWithStyle(e.headers, e.headerStyleID, 25.0)
	}
	return nil
}

func (e *StreamExport) WriteHeaders(headers []any) error {
	e.headers = headers
	return e.initSheet()
}

func (e *StreamExport) WriteRow(values []any) error {
	if e.stream == nil {
		if err := e.initSheet(); err != nil {
			return err
		}
	}

	if e.currentRow > maxRowsPerSheet {
		if err := e.applyColumnWidths(); err != nil {
			return err
		}
		if err := e.stream.Flush(); err != nil {
			return err
		}
		e.sheetIndex++
		if err := e.initSheet(); err != nil {
			return err
		}
	}

	return e.writeRowWithStyle(values, e.bodyStyleID, 22.0)
}

func (e *StreamExport) writeRowWithStyle(values []any, styleID int, height float64) error {
	for i, val := range values {
		strVal := fmt.Sprintf("%v", val)
		width := float64(utf8.RuneCountInString(strVal)) * 1.2
		if len(strVal) > utf8.RuneCountInString(strVal) {
			width += float64(len(strVal)-utf8.RuneCountInString(strVal)) * 0.6
		}

		colIdx := i + 1
		if width < 12 {
			width = 12
		}
		if width > e.colWidths[colIdx] {
			e.colWidths[colIdx] = width
		}
	}

	cell, _ := excelize.CoordinatesToCellName(1, e.currentRow)
	if err := e.stream.SetRow(cell, values, excelize.RowOpts{
		Height:  height,
		StyleID: styleID,
	}); err != nil {
		return err
	}

	e.currentRow++
	return nil
}

func (e *StreamExport) applyColumnWidths() error {
	sheetName := fmt.Sprintf("Sheet%d", e.sheetIndex)
	for colIdx, width := range e.colWidths {
		finalWidth := width
		if finalWidth > 80 {
			finalWidth = 80
		}
		colName, _ := excelize.ColumnNumberToName(colIdx)
		_ = e.file.SetColWidth(sheetName, colName, colName, finalWidth)
	}
	return nil
}

func (e *StreamExport) Flush(w io.Writer) error {
	if e.stream != nil {
		if err := e.applyColumnWidths(); err != nil {
			return err
		}
		if err := e.stream.Flush(); err != nil {
			return err
		}
	}
	return e.file.Write(w)
}

func (e *StreamExport) Close() error {
	return e.file.Close()
}
