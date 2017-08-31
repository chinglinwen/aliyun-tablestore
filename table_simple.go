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

var (
	ErrSliceCantSet   = errors.New("slice can't set, try use pointer for slice element")
	ErrLengthNotMatch = errors.New("length does not match")
)

// SimpleTable a simplify table concept based on struct.
type SimpleTable struct {
	model    interface{}
	tagnames map[string]string
	table    *Table
}

// Customize table name rather than default name.
// default: split by uppercase, and insert an underscore.
type TableName interface {
	TableName() string
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
	name, err := tablename(s)
	if err != nil {
		return
	}
	row, tagnames, err := structToRow(s)
	if err != nil {
		return
	}
	t = &SimpleTable{
		model:    s,
		tagnames: tagnames,
		table:    New(name, []Row{row}, options...),
	}
	return
}

func tablename(s interface{}) (name string, err error) {
	name, err = structName(s)
	if err != nil {
		return
	}
	if sn, ok := s.(TableName); ok {
		name = sn.TableName()
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
	name, err := tablename(ss[0])
	if err != nil {
		return
	}
	var tagnames map[string]string
	rows := []Row{}
	for i, s := range ss {
		row, tg, err := structToRow(s)
		if i == 0 {
			tagnames = tg
		}
		if err != nil {
			return nil, fmt.Errorf("item %v: %v", i, err)
		}
		rows = append(rows, row)
	}
	t = &SimpleTable{
		model:    ss,
		tagnames: tagnames,
		table:    New(name, rows, options...),
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
func (s *SimpleTable) GetRowRaw() (Row, error) {
	return s.table.GetRow()
}

// Get a single row. result in struct itself.
// For the GetRow, primary key is enough.
func (s *SimpleTable) GetRow() (err error) {
	row, err := s.GetRowRaw()
	if err != nil {
		return
	}
	s.fillStruct(row)
	return
}

func (s *SimpleTable) fillStruct(row Row) {
	t := reflect.ValueOf(s.model)
	// if pointer get the underlying element
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for _, v := range row {
		x := v.Value
		if i, ok := x.(int64); ok {
			x = int(i)
		}
		val := t.FieldByName(s.tagnames[v.Name])
		val.Set(reflect.ValueOf(x))
	}
	return
}

// Get a single row. (table defined by struct s)
func GetRow(s interface{}) (err error) {
	t, err := NewSimpleTable(s)
	if err != nil {
		return
	}
	return t.GetRow()
}

// Get row history.
func (s *SimpleTable) GetRowHistoryRaw(max int) (RowHistory, error) {
	return s.table.GetRowHistory(max)
}

func (s *SimpleTable) GetRowHistory(max int) (d interface{}, err error) {
	rows, err := s.table.GetRowHistory(max)
	if err != nil {
		return
	}

	typ := reflect.TypeOf(s.model)
	n := len(rows)
	slice := reflect.MakeSlice(reflect.SliceOf(typ), n, n).Interface()
	s.model = slice // s.model remains the same at user side

	err = s.fillStructs(rows)
	if err != nil {
		return
	}
	d = slice
	return
}

// Get row history. (table defined by struct s)
// returned result need to cast back to user type, eg: dd, ok := d.([]*User).
func GetRowHistory(s interface{}, max int) (interface{}, error) {
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
func (s *SimpleTable) GetRowsRaw() ([]Row, error) {
	return s.table.GetRows()
}

// Get multiple rows, result in slice of struct itself.
func (s *SimpleTable) GetRows() error {
	rows, err := s.table.GetRows()
	if err != nil {
		return err
	}
	return s.fillStructs(rows)
}

func (s *SimpleTable) fillStructs(rows []Row) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v, try use pointer, eg: []*struct", r)
			return
		}
	}()

	t := reflect.ValueOf(s.model)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Len() != len(rows) && len(rows) > 0 {
		return ErrLengthNotMatch
	}

	for i := 0; i < t.Len(); i++ {
		isnil := t.Index(i).IsNil()
		if isnil {
			typ := t.Index(i).Type()
			x := reflect.New(typ.Elem())
			t.Index(i).Set(x)
		}
		tt := reflect.ValueOf(t.Index(i).Interface()).Elem()

		if !tt.CanSet() {
			return ErrSliceCantSet
		}

		for _, v := range rows[i] {
			if v.Pkey && !isnil {
				continue
			}
			x := v.Value
			if i, ok := x.(int64); ok {
				x = int(i)
			}
			val := tt.FieldByName(s.tagnames[v.Name])
			val.Set(reflect.ValueOf(x))
		}
	}
	return
}

// Get multiple rows. (table defined by struct s)
// Often provide primary key is enough.
func GetRows(ss interface{}) error {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return err
	}
	return t.GetRows()
}

// Put multiple rows.
func (s *SimpleTable) PutRows() error {
	return s.table.PutRows()
}

// Put multiple rows. (table defined by struct ss)
func PutRows(ss interface{}) error {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return err
	}
	return t.PutRows()
}

// Delete one or multiple rows. (table defined by struct s)
func DelRow(s interface{}) error {
	t, err := NewSimpleTable(s)
	if err != nil {
		return err
	}
	return t.DelRows()
}

// Delete one or multiple rows.
func (s *SimpleTable) DelRows() error {
	return s.table.DelRows()
}

// Delete one or multiple rows. (table defined by slice of struct ss)
func DelRows(ss interface{}) error {
	t, err := NewSimpleTableBatch(ss)
	if err != nil {
		return err
	}
	return t.DelRows()
}

func structName(s interface{}) (string, error) {
	v, err := strctVal(s)
	if err != nil {
		return "", err
	}
	return nameConvert(v.Type().Name()), nil
}

func structToRow(s interface{}) (row Row, tagnames map[string]string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			return
		}
	}()

	v, err := strctVal(s)
	if err != nil {
		return
	}
	var pkeyexist bool
	row = []Column{}
	tagnames = make(map[string]string, 0)
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
		noauto := strings.Contains(tag, "noauto")
		if !pkey && name == "id" && !noauto {
			pkey = true // automatic primary key on id
		}
		if pkey {
			pkeyexist = true
		}
		value := v.Field(i).Interface()
		row = append(row, Column{Name: name, Value: value, Pkey: pkey})
		tagnames[name] = typeField.Name
	}
	if !pkeyexist {
		err = errors.New("no primary key specified")
		return
	}
	return
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
