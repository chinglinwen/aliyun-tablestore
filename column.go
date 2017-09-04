package tablestore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// Provided primary key as condition, it's need exist and matched.
func (t *Table) DelColumn(name string) (err error) {
	if len(t.Rows) == 0 {
		return ErrNoAnyRow
	}
	req := new(tablestore.UpdateRowRequest)
	chg := t.Rows[0].setudelchange(t.Name)
	chg.DeleteColumn(name)
	req.UpdateRowChange = chg

	c, err := t.GetClient()
	if err != nil {
		return
	}
	_, err = c.UpdateRow(req)
	return
}

// PutColumn add a column and value to a row.
// It's not change the table structure.
func (t *Table) PutColumn(m map[string]interface{}) (err error) {
	if len(t.Rows) == 0 {
		return ErrNoAnyRow
	}
	req := new(tablestore.UpdateRowRequest)
	chg := t.Rows[0].setudelchange(t.Name)
	for name, v := range m {
		chg.PutColumn(name, wraptype(v))
	}
	req.UpdateRowChange = chg

	c, err := t.GetClient()
	if err != nil {
		return
	}
	_, err = c.UpdateRow(req)
	return
}

func (r Row) setudelchange(tableName string) *tablestore.UpdateRowChange {
	chg := new(tablestore.UpdateRowChange)
	chg.TableName = tableName
	chg.PrimaryKey = r.setpk()
	chg.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	return chg
}
