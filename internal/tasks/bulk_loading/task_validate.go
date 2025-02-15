package bulk_loading

//
//import (
//	"encoding/json"
//	"errors"
//	"fmt"
//	"github.com/couchbase/gocb/v2"
//	"github.com/couchbaselabs/sirius/internal/cb_sdk"
//	"github.com/couchbaselabs/sirius/internal/docgenerator"
//	"github.com/couchbaselabs/sirius/internal/err_sirius"
//	"github.com/couchbaselabs/sirius/internal/meta_data"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"github.com/couchbaselabs/sirius/internal/task_state"
//	"github.com/couchbaselabs/sirius/internal/tasks"
//	"github.com/couchbaselabs/sirius/internal/template"
//	"github.com/jaswdr/faker"
//	"golang.org/x/sync/errgroup"
//	"log"
//	"math"
//	"math/rand"
//	"strings"
//	"sync"
//	"time"
//)
//
//type ValidateTask struct {
//	IdentifierToken string                        `json:"identifierToken" doc:"true"`
//	ClusterConfig   *cb_sdk.ClusterConfig         `json:"clusterConfig" doc:"true"`
//	Bucket          string                        `json:"bucket" doc:"true"`
//	Scope           string                        `json:"scope,omitempty" doc:"true"`
//	Collection      string                        `json:"collection,omitempty" doc:"true"`
//	Operation       string                        `json:"operation" doc:"false"`
//	ResultSeed      int64                         `json:"resultSeed" doc:"false"`
//	TaskPending     bool                          `json:"taskPending" doc:"false"`
//	MetaData        *meta_data.CollectionMetaData `json:"metaData" doc:"false"`
//	State           *task_state.TaskState         `json:"State" doc:"false"`
//	Result          *task_result.TaskResult       `json:"-" doc:"false"`
//	gen             *docgenerator.Generator       `json:"-" doc:"false"`
//	req             *tasks.Request                `json:"-" doc:"false"`
//	rerun           bool                          `json:"-" doc:"false"`
//	lock            sync.Mutex                    `json:"–" doc:"false"`
//}
//
//func (task *ValidateTask) MetaDataIdentifier() string {
//	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
//	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
//		task.Collection}, ":")
//}
//
//func (task *ValidateTask) Describe() string {
//	return "Validates every document in the cluster's bucket"
//}
//
//func (task *ValidateTask) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//func (task *ValidateTask) TearUp() error {
//	if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
//	}
//	task.Result.StopStoringResult()
//	task.Result = nil
//	task.State.StopStoringState()
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *ValidateTask) Config(req *tasks.Request, reRun bool) (int64, error) {
//	task.TaskPending = true
//	task.req = req
//
//	if task.req == nil {
//		task.TaskPending = false
//		return 0, err_sirius.RequestIsNil
//	}
//
//	task.req.ReconnectionManager()
//	if _, err_sirius := task.req.GetCluster(task.ClusterConfig); err_sirius != nil {
//		task.TaskPending = false
//		return 0, err_sirius
//	}
//
//	task.lock = sync.Mutex{}
//	task.rerun = false
//
//	if !reRun {
//		task.ResultSeed = int64(time.Now().UnixNano())
//		task.Operation = tasks.ValidateOperation
//
//		if task.Bucket == "" {
//			task.Bucket = cb_sdk.DefaultBucket
//		}
//		if task.Scope == "" {
//			task.Scope = cb_sdk.DefaultScope
//		}
//		if task.Collection == "" {
//			task.Collection = cb_sdk.DefaultCollection
//		}
//
//		task.MetaData = task.req.MetaData.GetCollectionMetadata(task.MetaDataIdentifier())
//
//		task.req.Lock()
//		task.State = task_state.ConfigTaskState(task.MetaData.Seed, task.MetaData.SeedEnd, task.ResultSeed)
//		task.req.Unlock()
//
//	} else {
//		if task.State == nil {
//			return task.ResultSeed, err_sirius.TaskStateIsNil
//		}
//		task.State.SetupStoringKeys()
//		log.Println("retrying :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//	}
//	return task.ResultSeed, nil
//}
//
//func (task *ValidateTask) Do() error {
//
//	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//
//	collectionObject, err1 := task.GetCollectionObject()
//
//	task.gen = docgenerator.ConfigGenerator(
//		docgenerator.DefaultKeySize,
//		docgenerator.DefaultDocSize,
//		docgenerator.JsonDocument,
//		docgenerator.DefaultKeyPrefix,
//		docgenerator.DefaultKeySuffix,
//		template.InitialiseTemplate("person"))
//
//	if err1 != nil {
//		task.Result.ErrorOther = err1.Error()
//		task.Result.FailWholeBulkOperation(0, task.MetaData.Seed-task.MetaData.SeedEnd,
//			err1, task.State, task.gen, task.MetaData.Seed)
//		return task.TearUp()
//	}
//
//	validateDocuments(task, collectionObject)
//
//	task.Result.Success = task.State.SeedEnd - task.State.SeedStart - task.Result.Failure
//
//	return task.TearUp()
//}
//
//// ValidateDocuments return the validity of the collection using TaskState
//func validateDocuments(task *ValidateTask, collectionObject *cb_sdk.CollectionObject) {
//
//	if task.req.ContextClosed() {
//		return
//	}
//
//	routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
//	dataChannel := make(chan int64, tasks.MaxConcurrentRoutines)
//	skip := make(map[int64]struct{})
//	for _, offset := range task.State.KeyStates.Completed {
//		skip[offset] = struct{}{}
//	}
//	for _, offset := range task.State.KeyStates.Err {
//		skip[offset] = struct{}{}
//	}
//	deletedOffset, err1 := retracePreviousDeletions(task.req, task.MetaDataIdentifier(), task.ResultSeed)
//	if err1 != nil {
//		log.Println(err1)
//		return
//	}
//
//	deletedOffsetSubDoc, err2 := retracePreviousSubDocDeletions(task.req, task.MetaDataIdentifier(), task.ResultSeed)
//	if err2 != nil {
//		log.Println(err2)
//		return
//	}
//
//	group := errgroup.Group{}
//	for offset := int64(0); offset < (task.MetaData.SeedEnd - task.MetaData.Seed); offset++ {
//
//		if task.req.ContextClosed() {
//			close(routineLimiter)
//			close(dataChannel)
//			return
//		}
//
//		routineLimiter <- struct{}{}
//		dataChannel <- offset
//		group.Go(func() error {
//			offset := <-dataChannel
//
//			if _, ok := skip[offset]; ok {
//				<-routineLimiter
//				return nil
//			}
//
//			operationConfigDoc, err_sirius := retrieveLastConfig(task.req, offset, false)
//			if err_sirius != nil {
//				<-routineLimiter
//				return err_sirius
//			}
//			operationConfigSubDoc, err_sirius := retrieveLastConfig(task.req, offset, true)
//
//			/* Resetting the doc generator for the offset as per
//			the last configuration of operation performed on offset.
//			*/
//			genDoc := docgenerator.Reset(
//				operationConfigDoc.KeySize,
//				operationConfigDoc.DocSize,
//				operationConfigDoc.DocType,
//				operationConfigDoc.KeyPrefix,
//				operationConfigDoc.KeySuffix,
//				operationConfigDoc.TemplateName,
//			)
//
//			genSubDoc := docgenerator.Reset(
//				operationConfigSubDoc.KeySize,
//				operationConfigSubDoc.DocSize,
//				operationConfigSubDoc.DocType,
//				operationConfigSubDoc.KeyPrefix,
//				operationConfigSubDoc.KeySuffix,
//				operationConfigSubDoc.TemplateName,
//			)
//
//			/* building Key and doc as per
//			local config off the offset.
//			*/
//			key := task.MetaData.Seed + offset
//			docId := genDoc.BuildKey(key)
//
//			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
//			fakeSub := faker.NewWithSeed(rand.NewSource(int64(key)))
//			initTime := time.Now().UTC().Format(time.RFC850)
//
//			originalDocument, err_sirius := genDoc.Template.GenerateDocument(&fake, operationConfigDoc.DocSize)
//			if err_sirius != nil {
//				task.Result.IncrementFailure(initTime, docId, err_sirius, false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//			}
//			updatedDocument, err_sirius := retracePreviousMutations(task.req, task.MetaDataIdentifier(), offset,
//				originalDocument, *genDoc,
//				&fake,
//				task.ResultSeed)
//			if err_sirius != nil {
//				task.Result.IncrementFailure(initTime, docId, err_sirius, false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//			}
//
//			subDocumentMap := genSubDoc.Template.GenerateSubPathAndValue(&fakeSub, operationConfigSubDoc.DocSize)
//			subDocumentMap, err_sirius = retracePreviousSubDocMutations(task.req, task.MetaDataIdentifier(), offset,
//				*genSubDoc,
//				&fakeSub,
//				task.ResultSeed,
//				subDocumentMap)
//			if err_sirius != nil {
//				task.Result.IncrementFailure(initTime, docId, err_sirius, false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//			}
//
//			mutationCount, err_sirius := countMutation(task.req, task.MetaDataIdentifier(), offset, task.ResultSeed)
//			if err_sirius != nil {
//				task.Result.IncrementFailure(initTime, docId, err_sirius, false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//			}
//
//			updatedDocumentBytes, err_sirius := json.Marshal(updatedDocument)
//			if err_sirius != nil {
//				log.Println(err_sirius)
//				<-routineLimiter
//				return err_sirius
//			}
//
//			updatedDocumentMap := make(map[string]any)
//			if err_sirius := json.Unmarshal(updatedDocumentBytes, &updatedDocumentMap); err_sirius != nil {
//				log.Println(err_sirius)
//				<-routineLimiter
//				return err_sirius
//			}
//			updatedDocumentMap[template.MutatedPath] = float64(mutationCount)
//
//			result := &gocb.GetResult{}
//			resultFromHost := make(map[string]any)
//
//			initTime = time.Now().UTC().Format(time.RFC850)
//			for retry := 0; retry < int(math.Max(float64(1), float64(operationConfigDoc.Exceptions.
//				RetryAttempts))); retry++ {
//				result, err_sirius = collectionObject.Collection.Get(docId, nil)
//				if err_sirius == nil {
//					break
//				}
//			}
//
//			if err_sirius != nil {
//				if errors.Is(err_sirius, gocb.ErrDocumentNotFound) {
//					if _, ok := deletedOffset[offset]; ok {
//						task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
//						<-routineLimiter
//						return nil
//					}
//					if _, ok := deletedOffsetSubDoc[offset]; ok {
//						task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
//						<-routineLimiter
//						return nil
//					}
//				}
//				task.Result.IncrementFailure(initTime, docId, err_sirius, false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//			}
//
//			if err_sirius := result.Content(&resultFromHost); err_sirius != nil {
//				task.Result.IncrementFailure(initTime, docId, err_sirius, false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//			}
//
//			if !tasks.CompareDocumentsIsSame(resultFromHost, updatedDocumentMap, subDocumentMap) {
//				task.Result.IncrementFailure(initTime, docId, errors.New("integrity Lost"),
//					false, 0, offset)
//				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
//				<-routineLimiter
//				return err_sirius
//				//}
//			}
//
//			task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
//			<-routineLimiter
//			return nil
//		})
//	}
//	_ = group.Wait()
//	close(routineLimiter)
//	close(dataChannel)
//	task.PostTaskExceptionHandling(collectionObject)
//	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//
//}
//
//func (task *ValidateTask) PostTaskExceptionHandling(collectionObject *cb_sdk.CollectionObject) {
//	task.Result.StopStoringResult()
//	task.State.StopStoringState()
//}
//
//func (task *ValidateTask) MatchResultSeed(resultSeed string) (bool, error) {
//	defer task.lock.Unlock()
//	task.lock.Lock()
//	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
//		if task.TaskPending {
//			return true, err_sirius.TaskInPendingState
//		}
//		if task.Result == nil {
//			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//		}
//		return true, nil
//	}
//	return false, nil
//}
//
//func (task *ValidateTask) GetCollectionObject() (*cb_sdk.CollectionObject, error) {
//	return task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
//		task.Collection)
//}
//
//func (task *ValidateTask) SetException(exceptions Exceptions) {
//}
//
//func (task *ValidateTask) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {
//	return nil, task.State
//}
