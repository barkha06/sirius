package main

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/barkha06/sirius/internal/tasks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CouchbaseDb       = "couchbase"
	MongoDb           = "mongodb"
	CouchbaseColumnar = "columnar"
	DynamoDb          = "dynamodb"
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
	case DynamoDb:
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(task.Username, task.Password, "")),
			config.WithRegion(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else if task.Extra.Table == "" {
			resultString = "Empty Table name"
		} else {
			dynamoDbClient := dynamodb.NewFromConfig(cfg)
			var dynamoInput dynamodb.CreateTableInput
			dynamoInput.AttributeDefinitions = []types.AttributeDefinition{{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeS,
			}}
			dynamoInput.KeySchema = []types.KeySchemaElement{{
				AttributeName: aws.String("ID"),
				KeyType:       types.KeyTypeHash,
			}}
			dynamoInput.TableName = aws.String(task.Extra.Table)
			if task.Extra.Provisioned {
				dynamoInput.BillingMode = types.BillingModeProvisioned
				dynamoInput.ProvisionedThroughput = &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(int64(task.Extra.ReadCapacity)),
					WriteCapacityUnits: aws.Int64(int64(task.Extra.WriteCapacity)),
				}
			} else {
				dynamoInput.BillingMode = types.BillingModePayPerRequest
			}
			table, err := dynamoDbClient.CreateTable(context.TODO(), &dynamoInput)
			if err != nil {
				resultString = err.Error()
			} else {
				waiter := dynamodb.NewTableExistsWaiter(dynamoDbClient)
				err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
					TableName: aws.String(task.Extra.Table)}, 5*time.Minute)
				if err != nil {
					resultString = err.Error()
				} else {
					status = true
					resultString = "Table successfully created at " + table.TableDescription.CreationDateTime.GoString()
				}
			}
		}

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
	case DynamoDb:
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(task.Username, task.Password, "")),
			config.WithRegion(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else if task.Extra.Table == "" {
			resultString = "Empty Table name"
		} else {
			dynamoDbClient := dynamodb.NewFromConfig(cfg)
			del, err := dynamoDbClient.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
				TableName: aws.String(task.Extra.Table)})
			if err != nil {
				resultString = err.Error()
			} else {
				resultString = "Successful deletion : " + *del.TableDescription.TableName
			}
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
	case DynamoDb:
		dblist_new := []string{}
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(task.Username, task.Password, "")),
			config.WithRegion(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else {
			dynamoDbClient := dynamodb.NewFromConfig(cfg)
			tablePaginator := dynamodb.NewListTablesPaginator(dynamoDbClient, &dynamodb.ListTablesInput{})
			for tablePaginator.HasMorePages() {
				output, err := tablePaginator.NextPage(context.TODO())
				if err != nil {
					resultString = err.Error()
				} else {
					status = true
					dblist_new = append(dblist_new, output.TableNames...)
				}
			}
			dblist[task.ConnStr] = dblist_new
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
	case DynamoDb:
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(task.Username, task.Password, "")),
			config.WithRegion(task.ConnStr))
		if err != nil {
			resultString = err.Error()
		} else if task.Extra.Table == "" {
			resultString = "Empty Table name"
		} else {
			dynamoDbClient := dynamodb.NewFromConfig(cfg)
			input := &dynamodb.ScanInput{
				TableName: aws.String(task.Extra.Table),
				Select:    types.SelectCount,
			}
			result, err := dynamoDbClient.Scan(context.TODO(), input)
			if err != nil {
				return err.Error(), -1, false
			}
			count += int64(result.Count)
			for result.LastEvaluatedKey != nil && len(result.LastEvaluatedKey) != 0 {
				input.ExclusiveStartKey = result.LastEvaluatedKey
				result, err = dynamoDbClient.Scan(context.TODO(), input)
				if err != nil {
					return err.Error(), -1, false
				}
				count += int64(result.Count)
			}

			if count <= 0 {
				resultString = "Empty Collection"
				status = true
			} else {
				resultString = "Successfully Counted Documents"
				status = true
			}
		}
	}
	return resultString, count, status

}
