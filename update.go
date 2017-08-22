package tablestore

import (
	"errors"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// Provided primary key as condition, it's need exist and matched.
func (t *Table) DelColumn(name string) (err error) {
	if len(t.Rows) == 0 {
		return errors.New("no any row")
	}
	req := new(tablestore.UpdateRowRequest)
	chg := t.Rows[0].setupdatechange(t.Name)
	chg.DeleteColumn(name)
	req.UpdateRowChange = chg
	_, err = t.GetClient().UpdateRow(req)
	return
}

// PutColumn add a column and value to a row.
// It's not change the table structure.
func (t *Table) PutColumn(m map[string]interface{}) (err error) {
	if len(t.Rows) == 0 {
		return errors.New("no any row")
	}
	req := new(tablestore.UpdateRowRequest)
	chg := t.Rows[0].setupdatechange(t.Name)
	for name, v := range m {
		chg.PutColumn(name, wraptype(v))
	}
	req.UpdateRowChange = chg
	_, err = t.GetClient().UpdateRow(req)
	return
}

func (r Row) setupdatechange(tableName string) *tablestore.UpdateRowChange {
	chg := new(tablestore.UpdateRowChange)
	chg.TableName = tableName
	chg.PrimaryKey = r.setpk()
	chg.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	return chg
}
