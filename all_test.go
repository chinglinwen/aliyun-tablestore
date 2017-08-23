package tablestore

import (
	"fmt"
	"testing"
)

func init() {
	// dev setting
	endpoint := "http://weisudai-dev.cn-beijing.ots.aliyuncs.com"
	instance := "weisudai-dev"
	accessKeyId := "LTAIwrVogRswISb3"
	accessKeySecret := "9rCJZ8XnGpcCewyRi3eOYKMewWIKte"
	SetKey(endpoint, instance, accessKeyId, accessKeySecret)
}

// For create table only
// it can use zero value, and only one row.

var tb = &Table{
	Name: "test",
	Rows: []Row{
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
	},
}

var tbget = &Table{
	Name: "test",
	Rows: []Row{
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
	},
}

var tbputrow = &Table{
	Name: "test",
	Rows: []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			Column{Name: "age", Value: 10},
			Column{Name: "phone", Value: []byte("1113")},
		},
	},
}

var tbupdaterow = &Table{
	Name: "test",
	Rows: []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			Column{Name: "age", Value: 10},
			Column{Name: "phone", Value: []byte("1113-update")},
		},
	},
}

// omit a key will be error for getrow
var tbgetrow = &Table{
	Name: "test",
	Rows: []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			//Column{Name: "age", Value: 10},
			//Column{Name: "phone", Value: []byte("1113")},
		},
	},
}

var tbempty = &Table{
	Name: "test",
}

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
		if v.Name == "id" {
			fmt.Printf("%v,%#v\n", v.Name, v.Int())
			continue
		}
		if v.Name == "name" {
			fmt.Printf("%v,%#v\n", v.Name, v.String())
			continue
		}
		if v.Name == "phone" {
			fmt.Printf("%v,%#v\n", v.Name, string(v.Bytes()))
			continue
		}
		fmt.Printf("%v,%#v,type: %t\n", v.Name, v.Value, v.Value)
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
