package tablestore

import (
	"fmt"
	"testing"
)

type User struct {
	Id   int    `tablestore:",pkey"`
	User string `tablestore:"usera"`
	Pass string

	extra string // `tablestore:"-"`
}

var (
	u  = User{Id: 1, User: "user1", Pass: "pass1"}
	us = []User{
		{Id: 2, User: "user2", Pass: "pass2"},
		{Id: 3, User: "user3", Pass: "pass3"},
	}
)

func init() {
	err := CreateSimpleTable(u)
	if err != nil {
		fmt.Println("create simple table err ", err)
	}
	err = CreateSimpleTableBatch(us)
	if err != nil {
		fmt.Println("create simple table err ", err)
	}
}

func TestSimpleTableName(t *testing.T) {
	name, err := structName(u)
	if err != nil {
		t.Errorf("err %v", err)
	}
	expect := "user"
	if name != expect {
		t.Errorf("table name got %q, expect %q", name, expect)
	}
}

func TestSimplePutRow(t *testing.T) {
	err := PutRow(u)
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestSimpleUpdateRow(t *testing.T) {
	err := UpdateRow(u)
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestSimpleGetRow(t *testing.T) {
	row, err := GetRow(u)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	printRow(row)
}

func TestSimplePutRows(t *testing.T) {
	err := PutRows(us)
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestSimpleGetRows(t *testing.T) {
	rows, err := GetRows(us)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	printRows(rows)
}

func TestDelSimple(t *testing.T) {
	err := DelTable("user")
	if err != nil {
		fmt.Println("err: %v", err)
	}
}
