package exporter

import (
	"database/sql"
	"fmt"
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

	// Reuse scan buffers across rows: rows.Scan overwrites them every
	// iteration, so per-row allocation is unnecessary.
	scanValues := make([]any, len(cols))
	scanPointers := make([]any, len(cols))
	for i := range scanValues {
		scanPointers[i] = &scanValues[i]
	}

	for rows.Next() {
		var item T
		valElement := reflect.ValueOf(&item).Elem()

		// Clear previous row's values (Scan overwrites all slots on
		// success; clearing guards drivers that leave slots untouched).
		for i := range scanValues {
			scanValues[i] = nil
		}

		if err := rows.Scan(scanPointers...); err != nil {
			return err
		}

		for i, colName := range cols {
			rawVal := scanValues[i]
			if rawVal == nil {
				continue
			}

			sIdx, ok := dbColToStructIdx[colName]
			if !ok {
				continue
			}
			field := valElement.Field(sIdx)
			if !field.CanSet() {
				continue
			}

			// Fast paths with output identical to the generic path below:
			// string->string assignment and []byte->string conversion.
			if field.Kind() == reflect.String {
				if s, ok := rawVal.(string); ok {
					field.SetString(s)
					continue
				}
				if b, ok := rawVal.([]byte); ok {
					field.SetString(string(b))
					continue
				}
			}

			v := reflect.ValueOf(rawVal)

			if field.Kind() == reflect.String && v.Kind() == reflect.Slice {
				if b, ok := rawVal.([]byte); ok {
					field.SetString(string(b))
				} else {
					field.SetString(fmt.Sprintf("%s", rawVal))
				}
			} else {
				if v.Type().ConvertibleTo(field.Type()) {
					field.Set(v.Convert(field.Type()))
				} else {
					if field.Kind() == reflect.String {
						field.SetString(fmt.Sprintf("%v", rawVal))
					}
				}
			}
		}
		if mapper != nil {
			if err := mapper(&item); err != nil {
				return err
			}
		}

		rowValues := make([]any, len(structFieldIndexes))
		for i, sIdx := range structFieldIndexes {
			rowValues[i] = valElement.Field(sIdx).Interface()
		}

		if err := exp.WriteRow(rowValues); err != nil {
			return err
		}
	}

	return rows.Err()
}
