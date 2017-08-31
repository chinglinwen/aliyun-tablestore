package tablestore

import (
	"fmt"
	"testing"
)

type User struct {
	Id     int    // automatic pkey for id field, add ,noauto tag to disable it
	User   string `tablestore:"usera"` // optional pkey
	Pass   string
	Ignore string `tablestore:"-"`
	Age    int

	extra string // unexported field will be ignored
}

func (u User) TableName() string {
	return "userxx"
}

var (
	u  = User{Id: 1, User: "user1", Pass: "pass1", Ignore: "ignore", Age: 1}
	uq = &User{Id: 1}
	us = []User{
		{Id: 2, User: "user2", Pass: "pass2", Ignore: "ignore", Age: 2},
		{Id: 3, User: "user3", Pass: "pass3", Ignore: "ignore", Age: 3},
	}
	usq = []*User{
		&User{Id: 2},
		&User{Id: 3},
	}
	uh  = User{Id: 4, User: "user4", Pass: "pass4", Ignore: "ignore", Age: 4}
	uh1 = User{Id: 4, User: "user4a", Pass: "pass4a", Ignore: "ignore", Age: 40}

	uhq  = &User{Id: 4}
	uhqd = []User{
		User{Id: 1},
		User{Id: 2},
		User{Id: 3},
		User{Id: 4},
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
	err := GetRow(uq)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	if uq.User != "user1" {
		t.Errorf("expect %v, got %v", "user1", uq.User)
		return
	}
}

func TestSimpleGetRow(t *testing.T) {
	s, err := NewSimpleTable(uq)
	if err != nil {
		return
	}
	err = s.GetRow()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	if uq.User != "user1" {
		t.Errorf("expect %v, got %v", "user1", uq.User)
		return
	}
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
	if usq[0].Ignore != "" {
		t.Errorf("ignore is been stored")
		return
	}
	if usq[0].Age != 2 && usq[1].Age != 3 {
		t.Errorf("get rows failed")
		return
	}
}

func TestSimpleHistory(t *testing.T) {
	_ = DelRow(uhq) // just try make clean from start.

	err := UpdateRow(uh)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = UpdateRow(uh1)
	if err != nil {
		t.Errorf("err: %v", err)
	}

	d, err := GetRowHistory(uhq, 0)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	dd, ok := d.([]*User)
	if !ok {
		t.Errorf("cast back to user type err")
		return
	}
	if len(dd) != 2 {
		t.Errorf("length expect %v, got %v", 2, len(dd))
		return
	}
	if dd[0].Age != 40 || dd[1].Age != 4 {
		t.Errorf("for history 1, age expect %v, got %v", 40, dd[0].Age)
		t.Errorf("for history 2, age expect %v, got %v", 4, dd[1].Age)
		return
	}
}

func TestSimpleDelRows(t *testing.T) {
	err := DelRows(uhqd)
	if err != nil {
		t.Errorf("del rows err: %v", err)
	}
	err = GetRow(uq)
	if err != nil && uq.Age == 1 {
		t.Errorf("del rows check failed")
		return
	}
}

func TestDelSimple(t *testing.T) {
	err := DelTable("userxx")
	if err != nil {
		t.Errorf("del table err: %v", err)
	}
}
