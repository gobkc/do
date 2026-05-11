package exporter

import (
	"database/sql"
	"reflect"
)

func StreamFromRows[T any](rows *sql.Rows, exp DocumentExporter, mapper func(*T) error) error {
	defer rows.Close()

	var dummy T
	typ := reflect.TypeOf(dummy)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	var headers []any
	var structFieldIndexes []int
	dbColToStructIdx := make(map[string]int)

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		xlsxTag := f.Tag.Get("xlsx")
		dbTag := f.Tag.Get("db")
		if dbTag == "" {
			dbTag = f.Name
		}

		if xlsxTag == "" || xlsxTag == "-" {
			if f.Tag.Get("db") != "" || f.Tag.Get("xlsx") != "-" {
				dbColToStructIdx[dbTag] = i
			}
			continue
		}

		headers = append(headers, xlsxTag)
		structFieldIndexes = append(structFieldIndexes, i)
		dbColToStructIdx[dbTag] = i
	}

	if err := exp.WriteHeaders(headers); err != nil {
		return err
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		var item T
		valElement := reflect.ValueOf(&item).Elem()

		scanArgs := make([]any, len(cols))
		for i, colName := range cols {
			if sIdx, ok := dbColToStructIdx[colName]; ok {
				scanArgs[i] = valElement.Field(sIdx).Addr().Interface()
			} else {
				var discard any
				scanArgs[i] = &discard
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		if mapper != nil {
			if err := mapper(&item); err != nil {
				return err
			}
		}

		rowValues := make([]any, len(structFieldIndexes))
		for i, sIdx := range structFieldIndexes {
			fieldVal := valElement.Field(sIdx).Interface()
			rowValues[i] = fieldVal
		}

		if err := exp.WriteRow(rowValues); err != nil {
			return err
		}
	}

	return rows.Err()
}
