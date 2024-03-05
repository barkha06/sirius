package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gocql/gocql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/barkha06/sirius/internal/tasks"
)

const (
	CouchbaseDb       = "couchbase"
	MongoDb           = "mongodb"
	CouchbaseColumnar = "columnar"
	CassandraDb       = "cassandra"
)

func createDBOp(task *tasks.GenericLoadingTask) (string, bool) {
	status := false
	resultString := ""
	switch task.DBType {
	case MongoDb:
		mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else if task.Extra.Database == "" {
			resultString = "Empty Database name"
		} else {
			database := mongoClient.Database(task.Extra.Database)
			if database == nil {
				resultString = "Database Creation Unsuccessful   : " + task.Extra.Database
			} else if task.Extra.Collection == "" {
				status = true
				resultString = "Database Creation Successful: " + task.Extra.Database
			} else {
				err := database.CreateCollection(context.TODO(), task.Extra.Collection, nil)
				if err != nil {
					resultString = err.Error()
				} else {
					status = true
					resultString = "Collection Creation Successful : " + task.Extra.Database + "  /  " + task.Extra.Collection
				}
			}
		}
		mongoClient.Disconnect(context.TODO())
	case CouchbaseDb:
		return "", false
	case CouchbaseColumnar:
		return "", false
	}
	return resultString, status
}
func deleteDBOp(task *tasks.GenericLoadingTask) (string, bool) {
	status := false
	resultString := ""

	switch task.DBType {
	case MongoDb:
		mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else if task.Extra.Database == "" {
			resultString = "Empty Database name"
		} else {
			database := mongoClient.Database(task.Extra.Database)
			if database == nil {
				resultString = "Database does not exist  : " + task.Extra.Database

			} else if task.Extra.Collection == "" {
				err = database.Drop(context.TODO())
				if err != nil {
					resultString = err.Error()
				} else {
					status = true
					resultString = "Database Deletion Successful  : " + task.Extra.Database
				}
			} else {
				err := database.Collection(task.Extra.Collection).Drop(context.TODO())
				if err != nil {
					resultString = err.Error()
				} else {
					status = true
					resultString = "Collection Deletion Successful  : " + task.Extra.Collection
				}
			}
		}
		mongoClient.Disconnect(context.TODO())
	case CouchbaseDb:
		return "", false
	case CouchbaseColumnar:
		return "", false
	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}

		if task.Extra.Keyspace == "" {
			resultString = "Keyspace name not provided"
			break
		} else if task.Extra.Keyspace != "" && task.Extra.Table == "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}

			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra!")
				log.Println(errCreateSession)
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			dropKeyspaceQuery := fmt.Sprintf("DROP KEYSPACE %s", task.Extra.Keyspace)
			errDropKeyspace := cassandraSession.Query(dropKeyspaceQuery).Exec()
			if errDropKeyspace != nil {
				log.Println("unable to delete keyspace", errDropKeyspace)
				resultString += errDropKeyspace.Error()
				break
			}

			resultString += fmt.Sprintf("Keyspace '%s' deleted successfully.", task.Extra.Keyspace)
			status = true
			break
		} else if task.Extra.Keyspace != "" && task.Extra.Table != "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassClusterConfig.Keyspace = task.Extra.Keyspace
			cassandraSession, err := cassClusterConfig.CreateSession()
			if err != nil {
				log.Println("Unable to connect to Cassandra!")
				log.Println(err)
				resultString += "Unable to connect to Cassandra!"
				break
			}
			defer cassandraSession.Close()

			dropTableQuery := fmt.Sprintf("DROP TABLE %s", task.Extra.Table)
			errDropTable := cassandraSession.Query(dropTableQuery).Exec()
			if errDropTable != nil {
				log.Println("unable to delete table", errDropTable)
				resultString += errDropTable.Error()
				break
			}

			resultString += fmt.Sprintf("Table ' %s ' deleted successfully from Keyspace ' %s '.", task.Extra.Table, task.Extra.Keyspace)
			status = true
		}
	}
	return resultString, status

}
func ListDBOp(task *tasks.GenericLoadingTask) (any, bool) {
	status := false
	resultString := ""
	dblist := make(map[string][]string)
	switch task.DBType {
	case MongoDb:
		mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else {
			databases, err := mongoClient.ListDatabaseNames(context.TODO(), bson.D{})
			if err != nil {
				resultString = err.Error()
			} else {
				for _, db := range databases {
					collections, err := mongoClient.Database(db).ListCollectionNames(context.TODO(), bson.D{})
					if err == nil {
						status = true
						dblist[db] = collections
					} else {

						dblist[db] = append(dblist[db], err.Error())

					}
				}
			}

		}
		mongoClient.Disconnect(context.TODO())
	case CouchbaseDb:
		return "", false
	case CouchbaseColumnar:
		return "", false
	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}
		if task.Extra.Keyspace == "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra!")
				log.Println(errCreateSession)
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			var keyspaceName string
			keyspaces := make([]string, 0)

			//listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name != 'system' AND keyspace_name != 'system_traces' AND keyspace_name != 'system_auth' AND keyspace_name != 'system_distributed'"
			listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces"
			iterKeyspaces := cassandraSession.Query(listKeyspaceQuery).Iter()

			for iterKeyspaces.Scan(&keyspaceName) {
				keyspaces = append(keyspaces, keyspaceName)
			}
			if err := iterKeyspaces.Close(); err != nil {
				log.Println("error while iterating keyspaces names. err:", err)
				resultString += err.Error()
				break
			}
			resultString += "Given Cassandra cluster contains the following keyspaces"
			for _, keyspaceN := range keyspaces {
				dblist[task.Extra.Keyspace] = append(dblist[task.Extra.Keyspace], keyspaceN)
			}
			status = true
			break
		}
		if task.Extra.Table == "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassClusterConfig.Keyspace = task.Extra.Keyspace
			cassandraSession, err := cassClusterConfig.CreateSession()
			if err != nil {
				log.Println("Unable to connect to Cassandra!")
				log.Println(err)
				resultString += "Unable to connect to Cassandra!"
				break
			}
			defer cassandraSession.Close()

			var tableName string
			tables := make([]string, 0)

			listTableQuery := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
			iterTables := cassandraSession.Query(listTableQuery, task.Extra.Keyspace).Iter()

			for iterTables.Scan(&tableName) {
				tables = append(tables, tableName)
			}
			if err := iterTables.Close(); err != nil {
				log.Println("error while iterating table names. err:", err)
				resultString += err.Error()
				break
			}
			resultString += task.Extra.Keyspace + " keyspace contains the following tables"
			for _, tableN := range tables {
				dblist[task.Extra.Keyspace] = append(dblist[task.Extra.Keyspace], tableN)
			}
			status = true
		}
	}
	if !status && dblist == nil {
		if resultString == "" {
			return "no databases found", status
		}
		return resultString, status
	} else {
		return dblist, status
	}

}
func CountOp(task *tasks.GenericLoadingTask) (string, int64, bool) {
	status := false
	resultString := ""
	var count int64 = -1
	switch task.DBType {
	case MongoDb:
		mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else if task.Extra.Database == "" {
			resultString = "Empty Database name"
		} else {
			database := mongoClient.Database(task.Extra.Database)
			if database == nil {
				resultString = "Database Not Found   : " + task.Extra.Database
			} else if task.Extra.Collection == "" {
				resultString = "Empty Collection name " + task.Extra.Collection
			} else {
				col := database.Collection(task.Extra.Collection)
				if col == nil {
					resultString = "Collection Not Found   : " + task.Extra.Collection
				} else {
					status = true
					resultString = "Collection Creation Successful : " + task.Extra.Database + "  /  " + task.Extra.Collection
					count, err = col.CountDocuments(context.TODO(), bson.D{})
					if err != nil {
						resultString = err.Error()
					} else if count == 0 {
						resultString = "Empty Collection"
						status = true
					} else {
						resultString = "Successfully Counted Documents"
						status = true
					}
				}
			}
		}
		mongoClient.Disconnect(context.TODO())
	case CouchbaseDb:
		return "", count, false
	case CouchbaseColumnar:
		return "", count, false
	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}
		if task.Extra.Keyspace == "" {
			resultString = "Keyspace name not provided"
			break
		}
		if task.Extra.Table == "" {
			resultString = "Table name not provided"
			break
		}
		cassClusterConfig := gocql.NewCluster(task.ConnStr)
		cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
		cassClusterConfig.Keyspace = task.Extra.Keyspace
		cassandraSession, err := cassClusterConfig.CreateSession()
		if err != nil {
			log.Println("Unable to connect to Cassandra!")
			log.Println(err)
			resultString += "Unable to connect to Cassandra!"
		}
		defer cassandraSession.Close()

		countQuery := "SELECT COUNT(*) FROM " + task.Extra.Table
		var rowCount int64
		if errCount := cassandraSession.Query(countQuery).Scan(&rowCount); err != nil {
			log.Println("Error while getting COUNT", errCount)
			resultString += "Error while getting COUNT"
			status = false
			break
		}
		resultString += "Count Operation Successful. "
		resultString += "Count = " + string(rowCount)
		status = true
		count = rowCount
	}
	return resultString, count, status
}
