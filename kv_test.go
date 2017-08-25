package tablestore

import (
	"fmt"
	"testing"
)

func init() {
	err := CreateKV("kv", "", "", SetMaxVersion(10))
	if err != nil {
		fmt.Println("create kv err ", err)
	}
}

var kvput = &Table{
	Name: "kv",
	Rows: []Row{
		[]Column{
			Column{Name: "k", Value: "hello", Pkey: true},
			Column{Name: "v1", Value: "v5"},
		},
	},
}

func TestPut(t *testing.T) {
	//err := Put("kv", "hello", "there")
	//if err != nil {
	//	t.Errorf("err: %v", err)
	//}
	err := Update("kv", "hello", "there1")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = Update("kv", "hello", "there2")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	//err = kvput.PutRow()
	//if err != nil {
	//	t.Errorf("err: %v", err)
	//}
}

func TestGet(t *testing.T) {
	v, err := Get("kv", "hello")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	fmt.Println("v:", v)
}

func TestKVHistory(t *testing.T) {
	vs, err := KVHistory("kv", "hello", 4)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	for _, v := range vs {
		fmt.Println("vs:", v.String())
	}
}

func TestDel(t *testing.T) {
	err := Del("kv", "hello", "there2")
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

//func TestDelTable(t *testing.T) {
//	err := DelTable("kv")
//	if err != nil {
//		t.Errorf("err: %v", err)
//	}
//}
