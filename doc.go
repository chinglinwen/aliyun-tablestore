/*
Package tablestore implement a abstract table concept for aliyun tablestore.

For create table only
it can use zero value, and only one row.

Create:

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

	err := tb.Create()
	err := tb.PutRows()

GetRow:

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

	row, err := tbgetrow.GetRow()

PutRow:

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

	err := tbputrow.PutRow()

GetRows:

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


Put/Del columns:

	m := map[string]interface{}{
		"col1": "val1",
		"col2": 11,
	}
	err := tbputrow.PutColumn(m)

	err := tbputrow.DelColumn("col1")
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = tbputrow.DelColumn("col2")
	if err != nil {
		t.Errorf("err: %v", err)
	}

DelRows:

	cond := SetColCondition([]Cond{Cond{0, "age", 10}})
	//err := tbputrow.DelRows()
	err := tbputrow.DelRows(cond) //with extra condition for first row
	if err != nil {
		t.Errorf("err: %v", err)
	}
*/
package tablestore
