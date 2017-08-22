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
			Column{Name: "id", Value: 3, Pkey: true},
			Column{Name: "name", Value: "nameC", Pkey: true},
			Column{Name: "age", Value: 10},
			Column{Name: "phone", Value: []byte("1113")},
		},
	},
}

// omit a key will be error for getrow
var tbgetrow = &Table{
	Name: "test",
	Rows: []Row{
		[]Column{
			Column{Name: "id", Value: 3, Pkey: true},
			Column{Name: "name", Value: "nameC", Pkey: true},
			//Column{Name: "age", Value: 10},
			//Column{Name: "phone", Value: []byte("1113")},
		},
	},
}

func TestCreate(t *testing.T) {
	if err := tb.Create(); err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestPutARow(t *testing.T) {
	err := tbputrow.PutRow()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestGetARow(t *testing.T) {
	row, err := tbgetrow.GetRow()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
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
		fmt.Printf("%v,%#v,%t\n", v.Name, v.Value, v.Value)
	}
}

func TestPutRows(t *testing.T) {
	resp, err := tb.PutRows()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	t.Logf("%#v\n", resp)
}

func TestGetRows(t *testing.T) {
	rows, err := tbget.GetRows()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	for _, row := range rows {
		for _, v := range row {
			fmt.Printf("%v:%v key:%v, ", v.Name, v.Value, v.Pkey)
		}
		fmt.Println()
	}
}

func TestDelARow(t *testing.T) {
	err := tbputrow.DelRow()
	//err := tbputrow.DelRow(SetColCondition("age", 10)) //with extra condition
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestAddCol(t *testing.T) {
	m := map[string]interface{}{
		"col1": "val1",
		"col2": 11,
	}
	err := tbputrow.PutColumn(m)
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestDelCol(t *testing.T) {
	err := tbputrow.DelColumn("col1")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = tbputrow.DelColumn("col2")
	if err != nil {
		t.Errorf("err: %v", err)
	}
}
