package db

import (
	"encoding/json"
	"errors"
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
	defer cassandraSession.Close()

	// ========= Testing some things
	// ====================================

	//hotelType := reflect.TypeOf(keyValue.Doc).Elem()
	//fmt.Println("hotelType =", hotelType)
	//// Iterate over the fields of the struct
	//hotel := template.Hotel{}
	//for i := 0; i < hotelType.NumField(); i++ {
	//	field := hotelType.Field(i)
	//	fmt.Printf("Field Name: %s, Field Type: %s\n", field.Name, field.Type)
	//
	//	// Get the value of the field
	//	fieldValue := reflect.ValueOf(keyValue.Doc).Elem().FieldByName(field.Name).Interface()
	//	fmt.Printf("Field Value: %v\n", fieldValue)
	//
	//	switch field.Name {
	//	case "ID":
	//		hotel.ID = fieldValue.(string)
	//	case "Country":
	//		hotel.Country = fieldValue.(string)
	//	case "Address":
	//		hotel.Address = fieldValue.(string)
	//	case "FreeParking":
	//		hotel.FreeParking = fieldValue.(bool)
	//	case "City":
	//		hotel.City = fieldValue.(string)
	//	case "Type":
	//		hotel.TemplateType = "Hotel"
	//	case "URL":
	//		hotel.URL = fieldValue.(string)
	//	case "Reviews":
	//		hotel.Reviews = fieldValue.([]template.Review)
	//	case "Phone":
	//		hotel.Phone = fieldValue.(string)
	//	case "Price":
	//		hotel.Price = fieldValue.(float64)
	//	case "AvgRating":
	//		hotel.AvgRating = fieldValue.(float64)
	//	case "FreeBreakfast":
	//		hotel.FreeBreakfast = fieldValue.(bool)
	//	case "Name":
	//		hotel.Name = fieldValue.(string)
	//	case "PublicLikes":
	//		hotel.PublicLikes = fieldValue.([]string)
	//	case "Email":
	//		hotel.Email = fieldValue.(string)
	//	case "Mutated":
	//		hotel.Mutated = fieldValue.(float64)
	//	case "Padding":
	//		hotel.Padding = ""
	//
	//	}

	//if field.Name == "ID" {
	//	hotel.ID = fieldValue.(string)
	//} else if field.Name == "Country" {
	//	hotel.Country = fieldValue.(string)
	//} else if field.Name == "Address" {
	//	hotel.Address = fieldValue.(string)
	//} else if field.Name == "FreeParking" {
	//	hotel.FreeParking = fieldValue.(bool)
	//} else if field.Name == "City" {
	//	hotel.City = fieldValue.(string)
	//} else if field.Name == "Type" {
	//	hotel.Type = "Hotel"
	//} else if field.Name == "URL" {
	//	hotel.URL = fieldValue.(string)
	//}
	//}

	//fieldValue1 := reflect.ValueOf(keyValue.Doc).Elem().Interface()

	//fmt.Println("fieldValue1:", fieldValue1)
	//fmt.Println(hotel)

	//docValue := reflect.ValueOf(keyValue.Doc).Elem().Interface()
	//jsonData, errDocToJSON := json.Marshal(docValue)
	//if errDocToJSON != nil {
	//	fmt.Println(errDocToJSON)
	//}

	// =========

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("Error marshaling JSON:", errDocToJSON)
	}

	insertQuery := "INSERT INTO " + extra.Table + " JSON '" + string(jsonData) + "'"
	//errInsert := cassandraSession.Query(insertQuery, jsonData).Exec()
	errInsert := cassandraSession.Query(insertQuery).Exec()
	if errInsert != nil {
		log.Println("In Cassandra Create(), Error inserting data:", errInsert)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errInsert, false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
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
