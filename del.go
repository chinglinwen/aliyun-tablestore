package tablestore

import (
	"errors"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

var ErrNoAnyRow = errors.New("no any row")

// DelRows delete provided rows ( one or many ).
func (t *Table) DelRows(options ...delOption) (err error) {
	if len(t.Rows) == 0 {
		return ErrNoAnyRow
	}
	for i, v := range t.Rows {
		req := new(tablestore.DeleteRowRequest)
		req.DeleteRowChange = v.setdelchange(t.Name, i, options...)
		_, err = t.GetClient().DeleteRow(req)
	}
	return
}

func (r Row) setdelchange(tableName string, i int, options ...delOption) *tablestore.DeleteRowChange {
	chg := new(tablestore.DeleteRowChange)
	chg.TableName = tableName
	chg.PrimaryKey = r.setpk()
	chg.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	for _, op := range options {
		op(i, chg)
	}
	return chg
}

type delOption func(int, *tablestore.DeleteRowChange)

// Condition for deleteing.
type Cond struct {
	Index int // start from zero.
	Key   string
	Value interface{}
}

// Extra condition for del rows,
// default is based on primary key, here can specify other normal columns.
func SetColCondition(conds []Cond) delOption {
	return func(i int, chg *tablestore.DeleteRowChange) {
		var key string
		var value interface{}
		for _, v := range conds {
			if i == v.Index {
				key, value = v.Key, v.Value
			}
		}
		if key == "" || value == nil {
			return
		}
		chg.SetColumnCondition(
			tablestore.NewSingleColumnCondition(key, tablestore.CT_EQUAL, wraptype(value)),
		)
	}
}
