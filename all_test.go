package tablestore

import (
	"fmt"
	"testing"
)

//func init() {
//	endpoint := "http://xxx.cn-beijing.ots.aliyuncs.com"
//	instance := "xxx"
//	accessKeyId := "xxx"
//	accessKeySecret := "xxx"
//	SetKey(endpoint, instance, accessKeyId, accessKeySecret)
//}

// For create table only
// it can use zero value, and only one row.

func init() {
	rows := []Row{
		[]Column{
			Column{Name: "id", Value: 1, Pkey: true},
			Column{Name: "name", Value: "nameA", Pkey: true},
			Column{Name: "age", Value: 1},
			Column{Name: "phone", Value: []byte("1111")},
		},
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			Column{Name: "age", Value: 2},
			Column{Name: "phone", Value: []byte("1112")},
		},
	}
	tb = New("test", rows)

	rows = []Row{
		[]Column{
			Column{Name: "id", Value: 1, Pkey: true},
			Column{Name: "name", Value: "nameA", Pkey: true},
			//Column{Name: "age", Value: 1},
			//Column{Name: "phone", Value: 1111},
		},
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			//Column{Name: "age", Value: 2},
			//Column{Name: "phone", Value: 1112},
		},
	}
	tbget = New("test", rows)

	rows = []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			Column{Name: "age", Value: 10},
			Column{Name: "phone", Value: []byte("1113")},
		},
	}
	tbputrow = New("test", rows)

	rows = []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			Column{Name: "age", Value: 10},
			Column{Name: "phone", Value: []byte("1113-update")},
		},
	}
	tbupdaterow = New("test", rows)

	rows = []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			//Column{Name: "age", Value: 10},
			//Column{Name: "phone", Value: []byte("1113")},
		},
	}
	tbgetrow = New("test", rows)

	tbempty = New("test", nil)
}

var (
	tb          *Table
	tbget       *Table
	tbputrow    *Table
	tbupdaterow *Table
	tbgetrow    *Table
	tbempty     *Table
)

func TestCreate(t *testing.T) {
	_ = tb.Create()
	//if _ := tb.Create(); err != nil {
	//	t.Errorf("err: %v", err)
	//}
}

func TestPutRow(t *testing.T) {
	err := tbputrow.PutRow()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestUpdateRow(t *testing.T) {
	err := tbupdaterow.UpdateRow()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestGetRow(t *testing.T) {
	row, err := tbgetrow.GetRow()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	printRow(row)
}

func printRow(row Row) {
	for _, v := range row {
		var val interface{}
		val = v.Value
		if v.Name == "phone" {
			val = string(v.Bytes())
		}
		fmt.Printf("%v,%v, ts: %v\n", v.Name, val, v.Timestamp)
	}
}

func TestGetRowHistory(t *testing.T) {
	rh, err := tbgetrow.GetRowHistory(0)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	printRows(rh)
}

func TestPutRows(t *testing.T) {
	err := tb.PutRows()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
}

func TestGetRows(t *testing.T) {
	rows, err := tbget.GetRows()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	printRows(rows)
}

func printRows(rows []Row) {
	for _, row := range rows {
		printRow(row)
		fmt.Println()
	}
}

func TestPutColumn(t *testing.T) {
	m := map[string]interface{}{
		"col1": "val1",
		"col2": 11,
	}
	err := tbputrow.PutColumn(m)
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestDelColumn(t *testing.T) {
	err := tbputrow.DelColumn("col1")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = tbputrow.DelColumn("col2")
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestDelRows(t *testing.T) {
	err := tbputrow.DelRows()
	//cond := SetColCondition([]Cond{Cond{0, "age", 10}})
	//err := tbputrow.DelRows(cond) //with extra condition for first row
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = tbempty.DelRows()
	if err != ErrNoAnyRow {
		t.Errorf("empty del err: %v", err)
	}
}

func TestGetRange(t *testing.T) {
	c := RangeCond{
		Name: "test",
		Min:  []string{"id", "name"},
		Max:  []string{"id", "name"},
	}
	rows, err := GetRange(c)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("rows length expect 2, got: %v", len(rows))
	}
	var age int
	for _, v := range rows[1] {
		if v.Name == "age" {
			age = v.Int()
			break
		}
	}
	if age != 10 {
		t.Errorf("rows[1].Age expect 10, got: %v", age)
	}
	//printRows(rows)
}

func TestDelTable(t *testing.T) {
	err := DelTable("addressbook")
	if err != nil {
		t.Errorf("del table err: %v", err)
	}
}
