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

func (u User) TableName() string {
	return "userxx"
}

var (
	u  = User{Id: 1, User: "user1", Pass: "pass1"}
	uq = &User{Id: 1}
	us = []User{
		{Id: 2, User: "user2", Pass: "pass2"},
		{Id: 3, User: "user3", Pass: "pass3"},
	}
	usq = []User{
		{Id: 2},
		{Id: 3},
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
	name, err := tablename(u)
	if err != nil {
		t.Errorf("err %v", err)
	}
	expect := "userxx"
	if name != expect {
		t.Errorf("table name got %q, expect %q", name, expect)
	}

	type U1 struct {
		a int
	}
	name, err = tablename(&U1{})
	if err != nil {
		t.Errorf("err %v", err)
	}
	expect = "u1"
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

func TestSimpleGetRowByFunc(t *testing.T) {
	uq, err := GetRow(uq)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	//if uq.User != "user1" {
	//	t.Errorf("expect %v, got %v", "user1", uq.User)
	//	return
	//}
	fmt.Println("uq", uq)
	//spew.Dump("uq user", uq.(User))
}

func TestSimpleGetRow(t *testing.T) {
	//spew.Dump("uq before", uq)
	//uq, err := GetRow(uq)

	s, err := NewSimpleTable(uq)
	if err != nil {
		return
	}
	err = s.GetRow()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	uq := s.model.(User)
	fmt.Println("s", s.model)
	fmt.Println("uq", uq)
}

func TestSimplePutRows(t *testing.T) {
	err := PutRows(us)
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func TestSimpleGetRows(t *testing.T) {
	err := GetRows(usq)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	fmt.Println("usq", usq)
}

func TestDelSimple(t *testing.T) {
	err := DelTable("user")
	if err != nil {
		fmt.Println("err: %v", err)
	}
}
