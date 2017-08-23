package tablestore

import (
	"errors"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

var (
	defaultClient *tablestore.TableStoreClient
)

type Table struct {
	Name string
	Rows []Row

	client *tablestore.TableStoreClient
}

// Value: Primary key:   int,int64,string,[]byte.
// Value: Normal column: int,int64,string,[]byte,bool,float64.
type Column struct {
	Name     string
	Pkey     bool // Primary key or not
	AutoIncr bool // if auto increment
	Value    interface{}
}

type Row []Column

// New Create a table. ( with default client),
// It can be create by literal construction too.
func New(name string, rows []Row) *Table {
	return NewWithClient(name, rows, nil)
}

// NewWithClient create a table, with specified client.
func NewWithClient(name string, rows []Row, client *tablestore.TableStoreClient) *Table {
	if client == nil {
		client = defaultClient
	}
	return &Table{
		Name:   name,
		Rows:   rows,
		client: client,
	}
}

// SetClient set different client.
func (t *Table) SetClient(c *tablestore.TableStoreClient) {
	t.client = c
}

func (t *Table) GetClient() *tablestore.TableStoreClient {
	if t.client == nil {
		return defaultClient
	}
	return t.client
}

func SetKey(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...tablestore.ClientOption) {
	defaultClient = tablestore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret, options...)
}

// Create create table one row with zero value is enough.
func (t *Table) Create() (err error) {
	req := new(tablestore.CreateTableRequest)
	meta, err := t.setmeta()
	if err != nil {
		return
	}

	option := new(tablestore.TableOption)
	option.TimeToAlive = -1
	option.MaxVersion = 3

	res := new(tablestore.ReservedThroughput)
	res.Readcap = 0
	res.Writecap = 0

	req.TableMeta = meta
	req.TableOption = option
	req.ReservedThroughput = res

	_, err = t.GetClient().CreateTable(req)
	return
}

func (t *Table) setmeta() (*tablestore.TableMeta, error) {
	meta := new(tablestore.TableMeta)
	meta.TableName = t.Name

	if len(t.Rows) == 0 {
		return nil, errors.New("no any row, so no primary key been defined")
	}

	// keep columns primary key in order by slice
	for _, v := range t.Rows[0] {
		if !v.Pkey {
			continue
		}
		switch v.Value.(type) {
		case int:
			meta.AddPrimaryKeyColumn(v.Name, tablestore.PrimaryKeyType_INTEGER)
		case string:
			meta.AddPrimaryKeyColumn(v.Name, tablestore.PrimaryKeyType_STRING)
		case []byte:
			meta.AddPrimaryKeyColumn(v.Name, tablestore.PrimaryKeyType_BINARY)
		default:
			return nil, errors.New("type not supported for column: " + v.Name)
		}
	}
	return meta, nil
}
