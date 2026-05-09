package exporter

import (
	"context"
	"database/sql"
	"reflect"

	"gorm.io/gorm"
)

type Engine struct {
	db *gorm.DB
}

func NewEngine(db *gorm.DB) *Engine {
	return &Engine{db: db}
}

func Export[T any](ctx context.Context, e *Engine, query *gorm.DB, writer TableWriter) error {
	var model T
	headers, fieldNames := parseXlsxTags(model)

	return e.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		rows, err := query.Rows()
		if err != nil {
			return err
		}
		defer rows.Close()

		if err := writer.SetupSheet("Sheet1", headers); err != nil {
			return err
		}

		for rows.Next() {
			var data T
			if err := tx.ScanRows(rows, &data); err != nil {
				return err
			}

			v := reflect.ValueOf(data)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			rowValues := make([]interface{}, len(fieldNames))
			for i, name := range fieldNames {
				rowValues[i] = v.FieldByName(name).Interface()
			}

			if err := writer.WriteRow(rowValues); err != nil {
				return err
			}
		}

		return writer.Flush()
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
}

func parseXlsxTags(obj interface{}) (headers []string, fieldNames []string) {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("xlsx")
		if tag != "" {
			headers = append(headers, tag)
			fieldNames = append(fieldNames, field.Name)
		}
	}
	return
}
