package tablestore

import (
	"errors"
)

type KV struct {
	Name string
	K, V interface{}

	table      *Table
	kname      string
	vname      string
	maxVersion int
}

func NewKV(tableName string, key, value interface{}, options ...kvOption) (k *KV) {
	k = &KV{
		Name:  tableName,
		K:     key,
		V:     value,
		kname: "k",
		vname: "v",
	}
	for _, op := range options {
		op(k)
	}
	row := []Row{
		[]Column{
			Column{Name: k.kname, Value: k.K, Pkey: true},
			Column{Name: k.vname, Value: k.V},
		},
	}
	if k.maxVersion != 0 {
		k.table = New(k.Name, row, MaxVersion(k.maxVersion))
		return
	}
	k.table = New(k.Name, row)
	return
}

type kvOption func(*KV)

func SetKVName(kname, vname string) kvOption {
	return func(k *KV) {
		k.kname = kname
		k.vname = vname
	}
}

func SetMaxVersion(max int) kvOption {
	return func(k *KV) {
		k.maxVersion = max
	}
}

func (k *KV) Create() error {
	return k.table.Create()
}

// Create kv table, zero value is enough.
func CreateKV(name string, k, v interface{}, options ...kvOption) error {
	kv := NewKV(name, k, v, options...)
	return kv.Create()
}

func (k *KV) Put() error {
	return k.table.PutRow()
}

func Put(name string, k, v interface{}) error {
	return NewKV(name, k, v).Put()
}

func (k *KV) Update() error {
	return k.table.UpdateRow()
}

func Update(name string, k, v interface{}) error {
	return NewKV(name, k, v).Update()
}

func (k *KV) Get() (t T, err error) {
	row, err := k.table.GetRow()
	if err != nil {
		return
	}
	for _, v := range row {
		t.value = v.Value
		return
	}
	err = errors.New("no any value")
	return
}

func Get(name string, k interface{}) (T, error) {
	return NewKV(name, k, nil).Get()
}

// Newest at lower index, zero index is the newest.
func (k *KV) KVHistory(max int) (vs []T, err error) {
	rows, err := k.table.GetRowHistory(max)
	if err != nil {
		return
	}
	for _, v := range rows {
		if len(v) != 1 {
			err = errors.New("no history or too many columns")
			return
		}
		vs = append(vs, T{value: v[0].Value})
	}
	return
}

// A helper function for kv's KVHistory.
func KVHistory(name string, k interface{}, max int) ([]T, error) {
	return NewKV(name, k, nil).KVHistory(max)
}

func (k *KV) Del() error {
	return k.table.DelRows()
}

func Del(name string, k, v interface{}) error {
	return NewKV(name, k, v).Del()
}

type T struct {
	value interface{}
}

// Convert value to int.
func (t *T) Int() (v int) {
	v, _ = t.value.(int)
	return
}

// Convert value to int64.
func (t *T) Int64() (v int64) {
	v, _ = t.value.(int64)
	return
}

// Convert value to string.
func (t *T) String() (v string) {
	v, _ = t.value.(string)
	return
}

// Convert value to []byte.
func (t *T) Bytes() (v []byte) {
	v, _ = t.value.([]byte)
	return
}

func (t *T) Any() interface{} {
	return t.value
}
