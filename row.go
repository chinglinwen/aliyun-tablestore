package tablestore

import (
	"errors"
	"reflect"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// PutRow put only one row.
func (t *Table) PutRow() (err error) {
	if len(t.Rows) != 1 {
		return errors.New("no any row or two many rows")
	}
	req := new(tablestore.PutRowRequest)
	req.PutRowChange = t.Rows[0].setputchange(t.Name)
	_, err = t.GetClient().PutRow(req)
	return
}

// Default to first row as condition.
// Primary key length must be matched.
func (t *Table) GetRowRaw() (colmap *tablestore.ColumnMap, err error) {
	if len(t.Rows) == 0 {
		return nil, errors.New("no any row")
	}
	req := new(tablestore.GetRowRequest)
	criteria := new(tablestore.SingleRowQueryCriteria)

	criteria.PrimaryKey = t.Rows[0].setpk()
	req.SingleRowQueryCriteria = criteria
	req.SingleRowQueryCriteria.TableName = t.Name
	req.SingleRowQueryCriteria.MaxVersion = 1

	resp, err := t.GetClient().GetRow(req)
	if err != nil {
		return
	}
	colmap = resp.GetColumnMap()
	return
}

// GetRow get only one row, and it is the newest row(latest changed).
func (t *Table) GetRow() (Row, error) {
	colmap, err := t.GetRowRaw()
	if err != nil {
		return nil, err
	}
	columns := []Column{}
	for name, col := range colmap.Columns {
		v := col[len(col)-1] // take newest value
		columns = append(columns, Column{Name: name, Value: v.Value})
	}
	return Row(columns), nil
}

func (r Row) setpk() *tablestore.PrimaryKey {
	pk := new(tablestore.PrimaryKey)
	for _, v := range r {
		if !v.Pkey {
			continue
		}
		pk.AddPrimaryKeyColumn(v.Name, wraptype(v.Value))
	}
	return pk
}

// in case int tyep not suppported
func wraptype(v interface{}) interface{} {
	if v == nil {
		return v
	}
	switch reflect.TypeOf(v).Kind() {
	case reflect.Int:
		return int64(v.(int))
	default:
		return v
	}
}

func (r Row) setputchange(tableName string) *tablestore.PutRowChange {
	chg := new(tablestore.PutRowChange)
	chg.TableName = tableName
	chg.PrimaryKey = r.setpk()
	for _, v := range r {
		if v.Pkey {
			continue
		}
		chg.AddColumn(v.Name, wraptype(v.Value))
	}
	chg.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	return chg
}

// We don't introduce extra type for value,
// Because it make create table type more complex.
// So we use column as base type to convert.

// Convert column's value to int.
func (c *Column) Int() (v int) {
	if c.Value == nil {
		return
	}
	v, _ = c.Value.(int)
	return
}

// Convert column's value to int64.
func (c *Column) Int64() (v int64) {
	if c.Value == nil {
		return
	}
	v, _ = c.Value.(int64)
	return
}

// Convert column's value to string.
func (c *Column) String() (v string) {
	if c.Value == nil {
		return
	}
	v, _ = c.Value.(string)
	return
}

// Convert column's value to []byte.
func (c *Column) Bytes() (v []byte) {
	if c.Value == nil {
		return
	}
	v, _ = c.Value.([]byte)
	return
}
