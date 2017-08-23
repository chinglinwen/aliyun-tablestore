package tablestore

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

var tbempty = &Table{
	Name: "test",
}

// See example as go test.
