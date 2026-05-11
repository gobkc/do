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

	for rows.Next() {
		var item T
		valElement := reflect.ValueOf(&item).Elem()

		scanValues := make([]any, len(cols))
		scanPointers := make([]any, len(cols))
		for i := range scanValues {
			scanPointers[i] = &scanValues[i]
		}

		if err := rows.Scan(scanPointers...); err != nil {
			return err
		}

		for i, colName := range cols {
			rawVal := scanValues[i]
			if rawVal == nil {
				continue
			}

			if sIdx, ok := dbColToStructIdx[colName]; ok {
				field := valElement.Field(sIdx)
				if !field.CanSet() {
					continue
				}

				v := reflect.ValueOf(rawVal)

				if field.Kind() == reflect.String && v.Kind() == reflect.Slice {
					field.SetString(fmt.Sprintf("%s", rawVal))
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
