package tablestore

import (
	"errors"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func (t *Table) DelRow(options ...delOption) (err error) {
	req := new(tablestore.DeleteRowRequest)
	req.DeleteRowChange, err = t.setdelchange(options...)
	if err != nil {
		return
	}
	_, err = t.GetClient().DeleteRow(req)
	return
}

func (t *Table) setdelchange(options ...delOption) (*tablestore.DeleteRowChange, error) {
	if len(t.Rows) == 0 {
		return nil, errors.New("no any row")
	}
	chg := new(tablestore.DeleteRowChange)
	chg.TableName = t.Name
	chg.PrimaryKey = t.Rows[0].setpk()
	chg.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	for _, op := range options {
		op(chg)
	}
	return chg, nil
}

type delOption func(*tablestore.DeleteRowChange)

// Extra condition for del row
// default to based on primary key, here can specify other normal columns
func SetColCondition(key string, value interface{}) delOption {
	return func(chg *tablestore.DeleteRowChange) {
		if key == "" || value == nil {
			return
		}
		chg.SetColumnCondition(
			tablestore.NewSingleColumnCondition(key, tablestore.CT_EQUAL, wraptype(value)),
		)
	}
}
