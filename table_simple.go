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
	name, err := structName(s)
	if err != nil {
		return nil, err
	}
	row, err := structToRow(s)
	if err != nil {
		return nil, err
	}
	return New(name, []Row{row}, options...), nil
}

func NewSimpleTableBatch(slice interface{}, options ...tableOption) (*Table, error) {
	ss, err := interfaceSlice(slice)
	if err != nil {
		return nil, err
	}
	name, err := structName(ss[0])
	if err != nil {
		return nil, err
	}
	rows := []Row{}
	for i, s := range ss {
		row, err := structToRow(s)
		if err != nil {
			return nil, fmt.Errorf("item %v: %v", i, err)
		}
		rows = append(rows, row)
	}
	return New(name, rows, options...), nil
}

func interfaceSlice(slice interface{}) (ret []interface{}, err error) {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		err = errors.New("not a slice type")
		return
	}
	ret = make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}
	if ret == nil || len(ret) == 0 {
		err = errors.New("slice is nil, or zero length")
		return
	}
	return
}

// CreateSimpleTable create the simple table directly.
func CreateSimpleTable(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.Create()
}

// For multiple rows as batch process.
func CreateSimpleTableBatch(ss interface{}) error {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return err
	}
	return t.Create()
}

func GetRow(s interface{}) (Row, error) {
	t, err := NewSimpleTable(s)
	if err != nil {
		return nil, err
	}
	return t.GetRow()
}

func GetRowHistory(s interface{}, max int) (RowHistory, error) {
	t, err := NewSimpleTable(s)
	if err != nil {
		return nil, err
	}
	return t.GetRowHistory(max)
}

func PutRow(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.PutRow()
}

func UpdateRow(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.UpdateRow()
}

func GetRows(ss interface{}) ([]Row, error) {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return nil, err
	}
	return t.GetRows()
}

func PutRows(ss interface{}) error {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return err
	}
	return t.PutRows()
}

func structName(s interface{}) (string, error) {
	v, err := strctVal(s)
	if err != nil {
		return "", err
	}
	return nameConvert(v.Type().Name()), nil
}

func structToRow(s interface{}) (row Row, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			return
		}
	}()

	v, err := strctVal(s)
	if err != nil {
		return nil, err
	}
	row = []Column{}
	for i := 0; i < v.NumField(); i++ {
		// skip unexported field
		typeField := v.Type().Field(i)
		if unicode.IsLower(rune(typeField.Name[0])) {
			continue
		}

		tag := typeField.Tag.Get(TagName)
		name := nameConvert(typeField.Name)
		if tag != "" {
			tags := strings.Split(tag, ",")
			if tags[0] == "-" {
				continue // ignore this column
			}
			if tags[0] != "" {
				name = tags[0]
			}
		}
		pkey := strings.Contains(tag, "pkey")
		value := v.Field(i).Interface()
		row = append(row, Column{Name: name, Value: value, Pkey: pkey})
	}
	return row, nil
}

func strctVal(s interface{}) (v reflect.Value, err error) {
	v = reflect.ValueOf(s)
	// if pointer get the underlying element≤
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		err = errors.New("not a struct")
		return
	}
	return
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
