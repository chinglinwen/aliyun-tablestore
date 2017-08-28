package tablestore

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// Default tag name.
var TagName = "tablestore"

// Simple table from struct model.
func NewSimpleTable(s interface{}, options ...tableOption) (*Table, error) {
	name := nameConvert(structName(s))
	row, err := structToRow(s)
	if err != nil {
		return nil, err
	}
	return New(name, []Row{row}, options...)
}

func NewSimpleTableBatch(ss []interface{}, options ...tableOption) (*Table, error) {
	name := nameConvert(structName(s))
	rows := []Row{}
	for i, s := range ss {
		row, err := structToRow(s)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("item %v: %v", i, err))
		}
		rows = append(rows, row)
	}
	return New(name, rows, options...)
}

// CreateSimpleTable create the simple table directly.
func CreateSimpleTable(s interface{}) error {
	return NewSimpleTable(s).Create()
}

// For multiple rows as batch process.
func CreateSimpleTableBatch(ss []interface{}) error {
	return NewSimpleTableBatch(ss).Create()
}

func GetRow(s interface{}) (Row, error) {
	return NewSimpleTable(s).GetRow()
}

func GetRowHistory(s interface{}, max int) (RowHistory, error) {
	return NewSimpleTable(s).GetRowHistory(max)
}

func PutRow(s interface{}) error {
	return NewSimpleTable(s).PutRow()
}

func UpdateRow(s interface{}) error {
	return NewSimpleTable(s).UpdateRow()
}

func GetRows(ss []interface{}) ([]Row, error) {
	return NewSimpleTableBatch(ss).GetRows()
}

func PutRows(ss []interface{}) error {
	return NewSimpleTable(ss).PutRows()
}

func structName(s interface{}) string {
	return reflect.TypeOf(s).Name()
}

func structToRow(s interface{}) (Row, error) {
	t := reflect.TypeOf(s)
	if t.Kind() != "struct" {
		return nil, errors.New("type is not struct")
	}
	row := []Column{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := field.Interface()
		name := nameConvert(field.Name)
		tag := field.Tag.Get(TagName)
		if tag != nil {
			tags := strings.Split(tag, ",")
			if tags[0] == "-" {
				continue // ignore this column
			}
			if tags[0] != "" {
				name = tags[0]
			}
		}
		pkey := strings.Contains(tag, "pkey")
		row = append(row, Column{Name: name, Value: value, Pkey: pkey})
	}
	return row, nil
}

func nameConvert(name string) string {
	var words []string
	l := 0
	for s := name; s != ""; s = s[l:] {
		l = strings.IndexFunc(s[1:], unicode.IsUpper) + 1
		if l <= 0 {
			l = len(s)
		}
		words = append(words, s[:l])
	}
	return strings.ToLower(strings.Join(words, "_"))
}

// use slice as row

// use struct as row
