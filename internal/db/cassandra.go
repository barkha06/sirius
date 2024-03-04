package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/barkha06/sirius/internal/sdk_cassandra"
	"log"
)

// cassandraOperationResult stores the result information for Insert, Upsert, Delete and Read.
type cassandraOperationResult struct {
	key    string
	result perDocResult
}

func newCassandraOperationResult(key string, value interface{}, err error, status bool, offset int64) *cassandraOperationResult {
	return &cassandraOperationResult{
		key: key,
		result: perDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (c *cassandraOperationResult) Key() string {
	return c.key
}

func (c *cassandraOperationResult) Value() interface{} {
	return c.result.value
}

func (c *cassandraOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *cassandraOperationResult) GetError() error {
	return c.result.error
}

func (c *cassandraOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *cassandraOperationResult) GetOffset() int64 {
	return c.result.offset
}

type Cassandra struct {
	CassandraConnectionManager *sdk_cassandra.CassandraConnectionManager
}

func NewCassandraConnectionManager() *Cassandra {
	return &Cassandra{
		CassandraConnectionManager: sdk_cassandra.ConfigCassandraConnectionManager(),
	}
}

func (c *Cassandra) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &sdk_cassandra.CassandraClusterConfig{
		ClusterConfigOptions: sdk_cassandra.ClusterConfigOptions{
			KeyspaceName: extra.Keyspace,
			NumConns:     extra.NumOfConns,
		},
	}

	if _, err := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, clusterConfig); err != nil {
		log.Println("In Cassandra Connect(), error in GetCluster()")
		return err
	}

	return nil
}

func (c *Cassandra) Warmup(connStr, username, password string, extra Extras) error {
	// TODO
	log.Println("In Cassandra Warmup()")
	if err := validateStrings(connStr, username, password); err != nil {
		log.Println("In Cassandra Warmup(), error:", err)
		return err
	}

	return nil
}

func (c *Cassandra) Close(connStr string) error {
	return c.CassandraConnectionManager.Disconnect(connStr)
}

func (c *Cassandra) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	cassClusterConfig := c.CassandraConnectionManager.Clusters[connStr].CassandraClusterConfig

	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}

	cassClusterConfig.Keyspace = extra.Keyspace
	cassandraSession, errSessionCreate := cassClusterConfig.CreateSession()
	if errSessionCreate != nil {
		log.Println("Unable to connect to Cassandra!")
		log.Println(errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Unable to connect to Cassandra!"), false,
			keyValue.Offset)
	}

	insertQuery := "INSERT INTO " + extra.Keyspace + "." + extra.Table + " JSON ?"

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("Error marshaling JSON:", errDocToJSON)
	}

	errInsert := cassandraSession.Query(insertQuery, jsonData).Exec()
	if errInsert != nil {
		log.Println("Error inserting data:", errInsert)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errInsert, false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}
	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	var result map[string]interface{}
	iter := cassandraSessionObj.Query("SELECT * FROM ?.? WHERE ID=?", keyspaceName, tableName, key).Iter()
	result = make(map[string]interface{})
	if iter.Scan(&result) {
		return newCassandraOperationResult(key, result, nil, true, offset)
	} else if err := iter.Close(); err != nil {
		return newCassandraOperationResult(key, nil,
			fmt.Errorf("result is nil even after unsuccessful READ operation %s ", connStr), false,
			offset)
	} else {
		return newCassandraOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false,
			offset)
	}
}

func (c *Cassandra) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}
	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	query := "DELETE FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	if err2 := cassandraSessionObj.Query(query, key).Exec(); err2 != nil {
		return newCassandraOperationResult(key, nil,
			fmt.Errorf("unsuccessful Delete %s ", connStr), false, offset)
	}
	return newCassandraOperationResult(key, nil, nil, true, offset)
}

func (c *Cassandra) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}
