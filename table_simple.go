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

// SimpleTable a simplify table concept based on struct.
type SimpleTable struct {
	model interface{}
	table *Table
}

// Get the underlying table.
func (s *SimpleTable) GetTable() *Table {
	return s.table
}

// Simple table from struct model.
//
// example struct:
//
//		type User struct {
//			Id   int    `tablestore:",pkey"`
//			User string `tablestore:"usera"`
//			Pass string
//
//			extra string // `tablestore:"-"`
//		}
//
//		s  = User{Id: 1, User: "user1", Pass: "pass1"}
//
func NewSimpleTable(s interface{}, options ...tableOption) (t *SimpleTable, err error) {
	name, err := structName(s)
	if err != nil {
		return
	}
	row, err := structToRow(s)
	if err != nil {
		return
	}
	t = &SimpleTable{
		model: s,
		table: New(name, []Row{row}, options...),
	}
	return
}

// Create batch process for slice of struct.
//
//		 slice := []User{
//				{Id: 2, User: "user2", Pass: "pass2"},
//				{Id: 3, User: "user3", Pass: "pass3"},
//		 }
//
func NewSimpleTableBatch(slice interface{}, options ...tableOption) (t *SimpleTable, err error) {
	ss, err := interfaceSlice(slice)
	if err != nil {
		return
	}
	return NewSimpleTableBatchRaw(ss)
}

// Create batch process for slice of interface.
func NewSimpleTableBatchRaw(ss []interface{}, options ...tableOption) (t *SimpleTable, err error) {
	name, err := structName(ss[0])
	if err != nil {
		return
	}
	rows := []Row{}
	for i, s := range ss {
		row, err := structToRow(s)
		if err != nil {
			return nil, fmt.Errorf("item %v: %v", i, err)
		}
		rows = append(rows, row)
	}
	t = &SimpleTable{
		model: ss,
		table: New(name, rows, options...),
	}
	return
}

// convert slice of struct to slice of interface.
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

// Create for both kind of table ( include batch ).
func (s *SimpleTable) Create() error {
	return s.table.Create()
}

// CreateSimpleTable create the simple table directly.
func CreateSimpleTable(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.Create()
}

// For multiple rows as a batch process.
func CreateSimpleTableBatch(ss interface{}) error {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return err
	}
	return t.Create()
}

// Get a single row.
func (s *SimpleTable) GetRow() (Row, error) {
	return s.table.GetRow()
}

// Get a single row. (table defined by struct s)
func GetRow(s interface{}) (Row, error) {
	t, err := NewSimpleTable(s)
	if err != nil {
		return nil, err
	}
	return t.GetRow()
}

// Get row history.
func (s *SimpleTable) GetRowHistory(max int) (RowHistory, error) {
	return s.table.GetRowHistory(max)
}

// Get row history. (table defined by struct s)
func GetRowHistory(s interface{}, max int) (RowHistory, error) {
	t, err := NewSimpleTable(s)
	if err != nil {
		return nil, err
	}
	return t.GetRowHistory(max)
}

// Put a single row.
func (s *SimpleTable) PutRow() error {
	return s.table.PutRow()
}

// Put a single row. (table defined by struct s)
func PutRow(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.PutRow()
}

// Update a single row.
func (s *SimpleTable) UpdateRow() error {
	return s.table.UpdateRow()
}

// Update a single row. (table defined by struct s)
func UpdateRow(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.UpdateRow()
}

// Get multiple rows.
func (s *SimpleTable) GetRows() ([]Row, error) {
	return s.table.GetRows()
}

// Get multiple rows. (table defined by struct s)
// Often provide primary key is enough.
func GetRows(ss interface{}) ([]Row, error) {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return nil, err
	}
	return t.GetRows()
}

// Put multiple rows.
func (s *SimpleTable) PutRows() error {
	return s.table.PutRows()
}

// Put multiple rows. (table defined by struct s)
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

// convert name default rule.
// default rule:
//		User -> user
//		userRule -> user_rule
// split by uppercase, and insert an underscore.
//
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
