package tablestore

import (
	"errors"
	"reflect"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

var (
	ErrNoRowOrTooMany  = errors.New("no any row or too many rows for table")
	ErrNoAnyHistory    = errors.New("no any history")
	ErrHistoryNotFound = errors.New("history not found")
)

// PutRow put only one row, no history will be kept.
func (t *Table) PutRow() (err error) {
	if len(t.Rows) != 1 {
		return ErrNoRowOrTooMany
	}
	req := new(tablestore.PutRowRequest)
	req.PutRowChange = t.Rows[0].setputchange(t.Name)

	c, err := t.GetClient()
	if err != nil {
		return
	}
	_, err = c.PutRow(req)
	return
}

// UpdateRow will keep the histories
func (t *Table) UpdateRow() (err error) {
	if len(t.Rows) != 1 {
		return ErrNoRowOrTooMany
	}
	req := new(tablestore.UpdateRowRequest)
	req.UpdateRowChange = t.Rows[0].setupdatechange(t.Name)
	c, err := t.GetClient()
	if err != nil {
		return
	}
	_, err = c.UpdateRow(req)
	return
}

// Default to first row as condition.
// Primary key length must be matched.
func (t *Table) GetRowRaw(options ...rowOption) (colmap *tablestore.ColumnMap, err error) {
	if len(t.Rows) == 0 {
		return nil, ErrNoAnyRow
	}

	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.PrimaryKey = t.Rows[0].setpk()
	criteria.TableName = t.Name
	criteria.MaxVersion = 1 // default value

	for _, op := range options {
		op(criteria)
	}

	req := new(tablestore.GetRowRequest)
	req.SingleRowQueryCriteria = criteria

	c, err := t.GetClient()
	if err != nil {
		return
	}
	resp, err := c.GetRow(req)
	if err != nil {
		return
	}
	colmap = resp.GetColumnMap()
	return
}

type rowOption func(*tablestore.SingleRowQueryCriteria)

func SetRowMaxVersion(max int) rowOption {
	if max == 0 {
		max = 10000 // big enough for all version.
	}
	return func(c *tablestore.SingleRowQueryCriteria) {
		c.MaxVersion = int32(max)
	}
}

type RowHistory []Row

// max big than real, will use real length,
// zero means means all.
func (t *Table) GetRowHistory(max int) (RowHistory, error) {
	colmap, err := t.GetRowRaw(SetRowMaxVersion(max))
	if err != nil {
		return nil, err
	}
	var n int
	// zero will take all history (based on first column)
	for _, col := range colmap.Columns {
		n = len(col)
		break
	}
	if n == 0 {
		return nil, ErrNoAnyHistory
	}
	if max == 0 || max > n {
		max = n

	}
	rh := RowHistory{}
	i := 1
	for ; i <= max; i++ {
		columns := []Column{}
		for name, col := range colmap.Columns {
			if len(col) < i {
				continue
				//return nil, errors.New("not enough version")
			}
			v := col[i-1]
			columns = append(columns, Column{Name: name, Value: v.Value})
		}
		rh = append(rh, Row(columns))
	}
	if i == 1 {
		return nil, ErrHistoryNotFound
	}
	return rh, nil
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

// in case int type not suppported
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

func (r Row) setupdatechange(tableName string) *tablestore.UpdateRowChange {
	chg := new(tablestore.UpdateRowChange)
	chg.TableName = tableName
	chg.PrimaryKey = r.setpk()
	for _, v := range r {
		if v.Pkey {
			continue
		}
		chg.PutColumn(v.Name, wraptype(v.Value))
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
