package tablestore

import (
	"fmt"
	"testing"
	"time"
)

var kvname = "kv"

func init() {
	err := CreateKV(kvname, "", "", SetMaxVersion(10))
	if err != nil {
		fmt.Println("create kv err ", err)
	}
}

var kvput = &Table{
	Name: kvname,
	Rows: []Row{
		[]Column{
			Column{Name: "k", Value: "hello", Pkey: true},
			Column{Name: "v1", Value: "v5"},
		},
	},
}

func TestPut(t *testing.T) {
	//err := Put(kvname, "hello", "there")
	//if err != nil {
	//	t.Errorf("err: %v", err)
	//}
	err := Update(kvname, "hello", "there1")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = Update(kvname, "hello", "there2")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	//err = kvput.PutRow()
	//if err != nil {
	//	t.Errorf("err: %v", err)
	//}
}

func TestGet(t *testing.T) {
	v, err := Get(kvname, "hello")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	fmt.Printf("v: %v, time: %v\n", v, v.Time())
}

func TestSetKVTimestamp(t *testing.T) {
	ts := Timestamp(time.Now().Add(1 * time.Hour))
	err := UpdateWithTimeStamp(kvname, "ts", "when", ts)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	v, err := Get(kvname, "ts")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	if v.Timestamp != ts {
		t.Errorf("ts incorrect")
	}
	fmt.Printf("v: %v,time: %v\n", v.Value, Timestamp2Time(ts))
}

func TestKVHistory(t *testing.T) {
	vs, err := KVHistory(kvname, "hello", 4)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	for _, v := range vs {
		fmt.Println("vs:", v.String())
	}
}

func TestDel(t *testing.T) {
	err := Del(kvname, "hello", "there2")
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

//func TestDelTable(t *testing.T) {
//	err := DelTable(kvname)
//	if err != nil {
//		t.Errorf("err: %v", err)
//	}
//}
