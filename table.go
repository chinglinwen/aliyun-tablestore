package tablestore

import (
	"errors"
	"strings"
	"time"

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

	timestamp int64
}

// Value: Primary key:   int,int64,string,[]byte.
// Value: Normal column: int,int64,string,[]byte,bool,float64.
type Column struct {
	Name      string
	Pkey      bool // Primary key or not
	AutoIncr  bool // if auto increment
	Value     interface{}
	Timestamp int64 // default is put time
}

type Row []Column

var (
	ErrClientNotSet    = errors.New("client is not set,no init")
	ErrSomeSetKeyEmpty = errors.New("some of setkey value is empty")

	ErrMinMaxNotMatch = errors.New("number of min and max does not match")
	ErrNoPrimaryKey   = errors.New("no column for the primary key")
	ErrNoAnyRow       = errors.New("no any row")
	ErrNoAnyValue     = errors.New("no any value")
	ErrNoHistory      = errors.New("no history or too many columns")

	ErrNoPrimaryKeyDefined = errors.New("no any row, so no primary key been defined")
)

type TypeNotSupportError struct {
	Name string
}

func (t *TypeNotSupportError) Error() string {
	return "type not supported for column: " + t.Name
}

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

func SetKey(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...tablestore.ClientOption) error {
	if endPoint == "" || instanceName == "" ||
		accessKeyId == "" || accessKeySecret == "" {
		return ErrSomeSetKeyEmpty
	}
	defaultClient = tablestore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret, options...)
	return nil
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

// Default will be insert time
// SetTimestamp to set different timestamp.
// Must be in a day range. (day1+ts)<ts<(day3+ts)
//
// Example min timestamp and max timestamp range:
//	 MinTimestamp:1504417246415914, MaxTimestamp:1504590046415914
//	 Sunday, September 3, 2017 1:40:46.416 PM GMT+08:00
//	 Tuesday, September 5, 2017 1:40:46.416 PM GMT+08:00
//
func SetTimestamp(ts int64) tableOption {
	return func(t *Table) {
		t.timestamp = ts
	}
}

func Timestamp(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func Timestamp2Time(ts int64) time.Time {
	return time.Unix(0, ts*int64(time.Millisecond))
}

func (t *Table) setmeta() (*tablestore.TableMeta, error) {
	meta := new(tablestore.TableMeta)
	meta.TableName = t.Name

	if len(t.Rows) == 0 {
		return nil, ErrNoPrimaryKeyDefined
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
			return nil, &TypeNotSupportError{Name: v.Name}
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
