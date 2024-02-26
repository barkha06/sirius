package db

import (
	"fmt"
	"github.com/barkha06/sirius/internal/sdk_columnar"
	"log"
	"time"
)

type columnarOperationResult struct {
	key    string
	result perDocResult
}

func newColumnarOperationResult(key string, value interface{}, err error, status bool, offset int64) *columnarOperationResult {
	return &columnarOperationResult{
		key: key,
		result: perDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (c *columnarOperationResult) Key() string {
	return c.key
}

func (c *columnarOperationResult) Value() interface{} {
	return c.result.value
}

func (c *columnarOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *columnarOperationResult) GetError() error {
	return c.result.error
}

func (c *columnarOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *columnarOperationResult) GetOffset() int64 {
	return c.result.offset
}

type Columnar struct {
	connectionManager *sdk_columnar.ConnectionManager
}

func NewColumnarConnectionManager() *Columnar {
	return &Columnar{
		connectionManager: sdk_columnar.ConfigConnectionManager(),
	}
}

func (c *Columnar) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &sdk_columnar.ClusterConfig{}

	if _, err := c.connectionManager.GetCluster(connStr, username, password, clusterConfig); err != nil {
		return err
	}

	return nil
}

func (c *Columnar) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	return nil
}

func (c *Columnar) Close(connStr string) error {
	return c.connectionManager.Disconnect(connStr)
}

func (c *Columnar) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// Reading using Analytics Query

	cbCluster := c.connectionManager.Clusters[connStr].Cluster
	log.Println("Cluster:", cbCluster)

	b := cbCluster.Bucket("Default")
	err := b.WaitUntilReady(time.Second, nil)
	if err != nil {
		log.Println("In Columnar Read(), unable to connect to Bucket")
		log.Println(err)
	}

	results, err1 := cbCluster.AnalyticsQuery("SELECT 1;", nil)
	if err1 != nil {
		log.Println("In Columnar Read(), unable to execute query")
		log.Println(err1)
	}

	var greeting interface{}

	fmt.Println("Read Query Result")
	for results.Next() {
		err := results.Row(&greeting)
		if err != nil {
			panic(err)
		}
		fmt.Println(greeting)
	}

	// always check for errors after iterating.
	err = results.Err()
	if err != nil {
		panic(err)
	}
	return newColumnarOperationResult(key, nil, nil, true, offset)
}

func (c *Columnar) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}
