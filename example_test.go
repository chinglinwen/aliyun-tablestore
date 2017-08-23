package tablestore

import (
	"fmt"
)

// Example is tests too
// try run as
//		go test -v

func ExampleCreate() {
	_ = tb.Create()
	/*
		if _ := tb.Create(); err != nil {
			fmt.Printf("err: %v", err)
		}
	*/
	// Output:
}

func ExamplePutRow() {
	err := tbputrow.PutRow()
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	// Output:
}

func ExampleGetRow() {
	row, err := tbgetrow.GetRow()
	if err != nil {
		fmt.Printf("err: %v", err)
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
		fmt.Printf("%v,%#v,type: %t\n", v.Name, v.Value, v.Value)
	}
	// Output:
	// age,10,type: %!t(int64=10)
	// phone,"1113"
}

func ExamplePutRows() {
	err := tb.PutRows()
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	// Output:
}

func ExampleGetRows() {
	rows, err := tbget.GetRows()
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	for _, row := range rows {
		for _, v := range row {
			if v.Name == "phone" {
				fmt.Printf("%v:%v key:%v, ", v.Name, string(v.Bytes()), v.Pkey)
				continue
			}
			fmt.Printf("%v:%v key:%v, ", v.Name, v.Value, v.Pkey)
		}
		break // break incase example will cause error for multiple line output.
	}
	// Output:
	// id:1 key:true, name:nameA key:true, age:1 key:false, phone:1111 key:false,
}

func ExamplePutColumn() {
	m := map[string]interface{}{
		"col1": "val1",
		"col2": 11,
	}
	err := tbputrow.PutColumn(m)
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	// Output:
}

func ExampleDelColumn() {
	err := tbputrow.DelColumn("col1")
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	err = tbputrow.DelColumn("col2")
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	// Output:
}

func ExampleDelRows() {
	cond := SetColCondition([]Cond{Cond{0, "age", 10}})
	//err := tbputrow.DelRows()
	err := tbputrow.DelRows(cond) //with extra condition for first row
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	err = tbempty.DelRows()
	if err != ErrNoAnyRow {
		fmt.Printf("empty del err: %v", err)
	}
	// Output:
}
