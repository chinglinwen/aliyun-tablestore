package tablestore

import (
	"errors"
	"strings"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

var (
	defaultClient *tablestore.TableStoreClient
)

type Table struct {
	Name string
	Rows []Row

	client     *tablestore.TableStoreClient
	maxVersion int
	//timeToAlive int  // not implement as option yet
	//readcap int  // not implement as option yet
	//writeCap int // not implement as option yet
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

var ErrClientNotSet = errors.New("client is not set,no init")

// New Create a table. ( with default client),
// It can be create by literal construction too.
func New(name string, rows []Row, options ...tableOption) (t *Table) {
	t = &Table{
		Name:       name,
		Rows:       rows,
		client:     defaultClient, // default
		maxVersion: 3,             // default
	}
	for _, op := range options {
		op(t)
	}
	return
}

// SetClient set different client.
func (t *Table) SetClient(c *tablestore.TableStoreClient) {
	t.client = c
}

func (t *Table) GetClient() (*tablestore.TableStoreClient, error) {
	if t.client == nil {
		t.client = defaultClient
	}
	if t.client == nil {
		return nil, ErrClientNotSet
	}
	return t.client, nil
}

func (t *Table) MaxVersion(max int) {
	t.maxVersion = max
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
	option.MaxVersion = t.maxVersion // default 3

	res := new(tablestore.ReservedThroughput)
	res.Readcap = 0
	res.Writecap = 0

	req.TableMeta = meta
	req.TableOption = option
	req.ReservedThroughput = res

	c, err := t.GetClient()
	if err != nil {
		return
	}
	_, err = c.CreateTable(req)
	if err != nil {
		if strings.Contains(err.Error(), "exist") {
			err = nil
		}
	}
	return
}

type tableOption func(*Table)

func MaxVersion(max int) tableOption {
	return func(t *Table) {
		t.maxVersion = max
	}
}

func SetClient(client *tablestore.TableStoreClient) tableOption {
	return func(t *Table) {
		t.client = client
	}
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
		case int, int64:
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

func (t *Table) Del() (err error) {
	req := new(tablestore.DeleteTableRequest)
	req.TableName = t.Name
	c, err := t.GetClient()
	if err != nil {
		return
	}
	_, err = c.DeleteTable(req)
	return
}

func DelTable(name string) error {
	t := &Table{Name: name}
	return t.Del()
}
