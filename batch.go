package tablestore

import (
	"errors"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func (t *Table) PutRowsRaw() ([]tablestore.RowResult, error) {
	req := &tablestore.BatchWriteRowRequest{}
	for _, row := range t.Rows {
		req.AddRowChange(row.setputchange(t.Name))
	}
	c, err := t.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := c.BatchWriteRow(req)
	if err != nil {
		return nil, err
	}
	return resp.TableToRowsResult[t.Name], nil
}

func (t *Table) PutRows() (err error) {
	_, err = t.PutRowsRaw()
	return
}

// we must know primary key before query

func (t *Table) GetRowsRaw() (result []tablestore.RowResult, err error) {
	req := &tablestore.BatchGetRowRequest{}
	criteria := &tablestore.MultiRowQueryCriteria{}

	for _, row := range t.Rows {
		criteria.AddRow(row.setpk())
	}

	// no need for now
	//criteria.AddColumnToGet("col1")
	//criteria.AddRow(pk2)

	criteria.MaxVersion = 1
	criteria.TableName = t.Name
	req.MultiRowQueryCriteria = append(req.MultiRowQueryCriteria, criteria)

	// no need for now
	//condition := tablestore.NewSingleColumnCondition("col1", tablestore.CT_GREATER_THAN, int64(0))
	//criteria.Filter = condition

	c, err := t.GetClient()
	if err != nil {
		return
	}
	resp, err := c.BatchGetRow(req)
	if err != nil {
		return nil, err
	}
	return resp.TableToRowsResult[criteria.TableName], nil
}

func (t *Table) GetRows() (rows []Row, err error) {
	resp, err := t.GetRowsRaw()
	if err != nil {
		return
	}
	return rowResultParse(resp)
}

func rowResultParse(resp []tablestore.RowResult) (rows []Row, err error) {
	for _, rowresult := range resp {
		columns := []Column{}
		for _, pkeyresult := range rowresult.PrimaryKey.PrimaryKeys {
			columns = append(columns, Column{
				Name:  pkeyresult.ColumnName,
				Value: pkeyresult.Value,
				Pkey:  true,
			})
		}
		for _, colresult := range rowresult.Columns {
			columns = append(columns, Column{
				Name:  colresult.ColumnName,
				Value: colresult.Value,
			})
		}
		rows = append(rows, columns)
	}
	return
}

// Table scan, may need to change in the future.
type RangeCond struct {
	Name   string // table name
	Limit  int
	Min    []string                     // min primary key
	Max    []string                     // max primary key
	Client *tablestore.TableStoreClient //empty will use default client
}

// to have two primary key for the range
// min -> max
func GetRange(rc RangeCond) (*tablestore.GetRangeResponse, error) {
	if len(rc.Min) != len(rc.Max) {
		return nil, errors.New("number of min and max does not match")
	}
	n := len(rc.Min)
	if n == 0 {
		return nil, errors.New("no column for the primary key")
	}

	req := &tablestore.GetRangeRequest{}
	criteria := &tablestore.RangeRowQueryCriteria{}
	criteria.TableName = rc.Name

	startPK := new(tablestore.PrimaryKey)
	endPK := new(tablestore.PrimaryKey)
	for i := 0; i < n; i++ {
		startPK.AddPrimaryKeyColumnWithMinValue(rc.Min[i])
		startPK.AddPrimaryKeyColumnWithMaxValue(rc.Max[i])
	}

	criteria.StartPrimaryKey = startPK
	criteria.EndPrimaryKey = endPK

	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	criteria.Limit = int32(rc.Limit)

	req.RangeRowQueryCriteria = criteria

	if rc.Client == nil {
		rc.Client = defaultClient
	}
	return rc.Client.GetRange(req)
}
