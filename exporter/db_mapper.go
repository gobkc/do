package exporter

import (
	"database/sql"
	"reflect"
)

func StreamFromRows[T any](rows *sql.Rows, exp DocumentExporter) error {
	defer rows.Close()

	var dummy T
	typ := reflect.TypeOf(dummy)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	var headers []any
	dbColToStructIdx := make(map[string]int)
	structIdxToHeaderIdx := make(map[int]int)

	headerIdx := 0
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		xlsxTag := f.Tag.Get("xlsx")
		dbTag := f.Tag.Get("db")
		if dbTag == "" {
			dbTag = f.Name
		}
		if xlsxTag != "" {
			headers = append(headers, xlsxTag)
			structIdxToHeaderIdx[i] = headerIdx
			dbColToStructIdx[dbTag] = i
			headerIdx++
		}
	}

	if err := exp.WriteHeaders(headers); err != nil {
		return err
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	colToHeaderIdx := make([]int, len(cols))
	for i, col := range cols {
		colToHeaderIdx[i] = -1
		if sIdx, ok := dbColToStructIdx[col]; ok {
			if hIdx, ok := structIdxToHeaderIdx[sIdx]; ok {
				colToHeaderIdx[i] = hIdx
			}
		}
	}

	scanArgs := make([]any, len(cols))
	scanValues := make([]any, len(cols))
	for i := range scanArgs {
		scanArgs[i] = &scanValues[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		rowValues := make([]any, len(headers))
		for i, val := range scanValues {
			hIdx := colToHeaderIdx[i]
			if hIdx != -1 {
				if b, ok := val.([]byte); ok {
					rowValues[hIdx] = string(b)
				} else {
					rowValues[hIdx] = val
				}
			}
		}

		if err := exp.WriteRow(rowValues); err != nil {
			return err
		}
	}

	return rows.Err()
}
