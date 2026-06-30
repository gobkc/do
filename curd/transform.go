package curd

import (
	"encoding/json"
	"encoding/xml"
)

// FieldTransformer transforms a field value before it is sent to the database.
// It receives the column name and the current value, and returns the transformed value.
// Multiple transformers can be composed with ComposeTransformers.
type FieldTransformer func(fieldName string, value any) any

// ComposeTransformers composes multiple FieldTransformers into one.
// Transformers are applied in order, each receiving the output of the previous.
//
// Usage:
//
//	t := ComposeTransformers(
//	    JSONBMarshaler("metadata", "config"),
//	    XMLMarshaler("document"),
//	)
func ComposeTransformers(transformers ...FieldTransformer) FieldTransformer {
	return func(fieldName string, value any) any {
		for _, t := range transformers {
			value = t(fieldName, value)
		}
		return value
	}
}

// JSONBMarshaler returns a FieldTransformer that JSON-marshals the specified fields.
// Fields not in the list are passed through unchanged. Nil values are passed through.
// The marshaled result is returned as a string, which PostgreSQL will auto-cast
// to jsonb for jsonb-typed columns.
//
// Usage:
//
//	c.WithTransformer(JSONBMarshaler("metadata", "tags"))
func JSONBMarshaler(fields ...string) FieldTransformer {
	fieldSet := make(map[string]bool, len(fields))
	for _, f := range fields {
		fieldSet[f] = true
	}
	return func(fieldName string, value any) any {
		if !fieldSet[fieldName] || value == nil {
			return value
		}
		data, err := json.Marshal(value)
		if err != nil {
			return value
		}
		return string(data)
	}
}

// XMLMarshaler returns a FieldTransformer that XML-marshals the specified fields.
// Fields not in the list are passed through unchanged. Nil values are passed through.
// The marshaled result is returned as a string, which PostgreSQL will auto-cast
// to xml for xml-typed columns.
//
// Usage:
//
//	c.WithTransformer(XMLMarshaler("document"))
func XMLMarshaler(fields ...string) FieldTransformer {
	fieldSet := make(map[string]bool, len(fields))
	for _, f := range fields {
		fieldSet[f] = true
	}
	return func(fieldName string, value any) any {
		if !fieldSet[fieldName] || value == nil {
			return value
		}
		data, err := xml.Marshal(value)
		if err != nil {
			return value
		}
		return string(data)
	}
}
