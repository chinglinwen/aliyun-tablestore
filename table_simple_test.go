package tablestore

import "fmt"

type user struct {
	id   int
	user string
	pass string
}

var u = user{id: 1, user: "user"}

func init() {
	err := CreateKV(kvname, "", "", SetMaxVersion(10))
	if err != nil {
		fmt.Println("create kv err ", err)
	}
}
