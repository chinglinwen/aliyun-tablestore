/*
Package tablestore implement a abstract table concept for aliyun tablestore.

SimpleTable:

We abstract a simple table concept based on struct.
Use struct as the model.

	type User struct {
		Id   int    `tablestore:",pkey"`
		User string `tablestore:"usera"`
		Pass string `tablestore:"-"`

		extra string // unexported field is ignored
	}

	var (
		u  = User{Id: 1, User: "user1", Pass: "pass1"}
		us = []User{
			{Id: 2, User: "user2", Pass: "pass2"},
			{Id: 3, User: "user3", Pass: "pass3"},
		}
	)

Create:

	// zero value is enough for create table only.
	err := CreateSimpleTable(u)

	// same behavior for create table. (use the first struct as model.)
	err = CreateSimpleTableBatch(us)

Put:

	err := PutRow(u)  // put will overwrite the history.

Update:

	err := UpdateRow(u)

GetRow:

	row, err := GetRow(u)

PutRows:

	err := PutRows(us)

GetRows:

	rows, err := GetRows(us)

DelTable:

	err := DelTable("user")


Origin table usage example:

for create table it can use zero value, and only one row.

Create:

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
	tb := New("test", rows)

	err := tb.Create()
	err := tb.PutRows()

GetRow:

	// Omit primary key will cause error.
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
	tbget := New("test", rows)

	row, err := tbgetrow.GetRow()

PutRow:

	rows = []Row{
		[]Column{
			Column{Name: "id", Value: 2, Pkey: true},
			Column{Name: "name", Value: "nameB", Pkey: true},
			Column{Name: "age", Value: 10},
			Column{Name: "phone", Value: []byte("1113")},
		},
	}
	tbputrow := New("test", rows)
	err := tbputrow.PutRow()

GetRows:

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

Key/Value:

Use update to keep the history, Put will delete history.

First time need to create kv (table first), in case table not exist yet.
Often at init from during the process

for KV init(create), zero value is enough.

Create:

	err := CreateKV("kv", "", "", SetMaxVersion(10))
	if err != nil {
		log.Fatal("kv init err: ", err)
	}

Update:

	err := Update("kv", "hello", "there1")
	err = Update("kv", "hello", "there2")

Get:

	v, err := Get("kv", "hello")

KVHistory:

	vs, err := KVHistory("kv", "hello", 4)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	for _, v := range vs {
		fmt.Println("vs:", v.String())
	}

Del:

	err := Del("kv", "hello", "there2")

Example setkey:

	func init() {
		endpoint := "http://xxx.cn-beijing.ots.aliyuncs.com"
		instance := "xxx"
		accessKeyId := "xxx"
		accessKeySecret := "xxx"
		SetKey(endpoint, instance, accessKeyId, accessKeySecret)
	}

*/
package tablestore
