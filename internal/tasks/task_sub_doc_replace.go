package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math"
	"math/rand"
	"time"
)

type SubDocReplace struct {
	IdentifierToken       string                             `json:"identifierToken" doc:"true"`
	ClusterConfig         *sdk.ClusterConfig                 `json:"clusterConfig" doc:"true"`
	Bucket                string                             `json:"bucket" doc:"true"`
	Scope                 string                             `json:"scope,omitempty" doc:"true"`
	Collection            string                             `json:"collection,omitempty" doc:"true"`
	SubDocOperationConfig *SubDocOperationConfig             `json:"subDocOperationConfig" doc:"true"`
	ReplaceSpecOptions    *ReplaceSpecOptions                `json:"replaceSpecOptions" doc:"true"`
	MutateInOptions       *MutateInOptions                   `json:"mutateInOptions" doc:"true"`
	Operation             string                             `json:"operation" doc:"false"`
	ResultSeed            int64                              `json:"resultSeed" doc:"false"`
	TaskPending           bool                               `json:"taskPending" doc:"false"`
	State                 *task_state.TaskState              `json:"State" doc:"false"`
	MetaData              *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	Result                *task_result.TaskResult            `json:"-" doc:"false"`
	gen                   *docgenerator.Generator            `json:"-" doc:"false"`
	req                   *Request                           `json:"-" doc:"false"`
	rerun                 bool                               `json:"-" doc:"false"`
}

func (task *SubDocReplace) Describe() string {
	return " SubDocReplace upserts a Sub-Document"
}

func (task *SubDocReplace) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SubDocReplace) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SubDocReplace) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SubDocReplace) Config(req *Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	task.rerun = reRun

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SubDocReplaceOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configSubDocOperationConfig(task.SubDocOperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configReplaceSpecOptions(task.ReplaceSpecOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configMutateInOptions(task.MutateInOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		task.MetaData = task.req.MetaData.GetCollectionMetadata(task.CollectionIdentifier(), 0, 0, "",
			"", "", "")

		task.req.lock.Lock()
		task.State = task_state.ConfigTaskState(task.MetaData.Seed, task.MetaData.SeedEnd, task.ResultSeed)
		task.req.lock.Unlock()

	} else {
		if task.State == nil {
			return task.ResultSeed, task_errors.ErrTaskStateIsNil
		}
		task.State.SetupStoringKeys()
		_ = DeleteResultFile(task.ResultSeed)
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SubDocReplace) tearUp() error {
	//Use this case to store task's state on disk when required
	//if err := task.State.SaveTaskSateOnDisk(); err != nil {
	//	log.Println("Error in storing TASK State on DISK")
	//}
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SubDocReplace) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	task.gen = docgenerator.ConfigGenerator(task.MetaData.DocType, task.MetaData.KeyPrefix,
		task.MetaData.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.MetaData.TemplateName))

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeBulkOperation(task.SubDocOperationConfig.Start, task.SubDocOperationConfig.End,
			task.MetaData.DocSize, task.gen, err1, task.State)
		return task.tearUp()
	}

	replaceSubDocuments(task, collectionObject)
	task.Result.Success = (task.SubDocOperationConfig.End - task.SubDocOperationConfig.Start) - task.Result.Failure

	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func replaceSubDocuments(task *SubDocReplace, collectionObject *sdk.CollectionObject) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)

	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	group := errgroup.Group{}
	for iteration := task.SubDocOperationConfig.Start; iteration < task.SubDocOperationConfig.End; iteration++ {

		routineLimiter <- struct{}{}
		dataChannel <- iteration

		group.Go(func() error {
			offset := <-dataChannel
			key := offset + task.MetaData.Seed
			docId := task.gen.BuildKey(key)

			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))

			task.gen.Template.GenerateSubPathAndValue(&fake)
			task.req.retracePreviousSubDocMutations(task.CollectionIdentifier(), offset, *task.gen, &fake,
				task.ResultSeed)

			var err error
			initTime := time.Now().UTC().Format(time.RFC850)
			for retry := 0; retry < int(math.Max(float64(1), float64(task.SubDocOperationConfig.Exceptions.
				RetryAttempts))); retry++ {

				var iOps []gocb.MutateInSpec
				for path, value := range task.gen.Template.GenerateSubPathAndValue(&fake) {
					iOps = append(iOps, gocb.ReplaceSpec(path, value, &gocb.ReplaceSpecOptions{
						IsXattr: task.ReplaceSpecOptions.IsXattr,
					}))
				}

				if !task.ReplaceSpecOptions.IsXattr {
					iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
						int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
							CreatePath: false,
							IsXattr:    false,
						}))
				}

				initTime = time.Now().UTC().Format(time.RFC850)
				_, err = collectionObject.Collection.MutateIn(docId, iOps, &gocb.MutateInOptions{
					Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
					PersistTo:       task.MutateInOptions.PersistTo,
					ReplicateTo:     task.MutateInOptions.ReplicateTo,
					DurabilityLevel: getDurability(task.MutateInOptions.Durability),
					StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
					Timeout:         time.Duration(task.MutateInOptions.Expiry) * time.Second,
					PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
				})

				if err == nil {
					break
				}
			}
			if err != nil {
				task.Result.IncrementFailure(initTime, docId, nil, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err

			}

			task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SubDocReplace) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
	task.State.StopStoringState()

	// Get all the errorOffset
	errorOffsetMaps := task.State.ReturnErrOffset()
	// Get all the completed offset
	completedOffsetMaps := task.State.ReturnCompletedOffset()

	// For the offset in ignore exceptions :-> move them from error to completed
	shiftErrToCompletedOnIgnore(task.SubDocOperationConfig.Exceptions.IgnoreExceptions, task.Result, errorOffsetMaps, completedOffsetMaps)

	if task.SubDocOperationConfig.Exceptions.RetryAttempts > 0 {

		exceptionList := getExceptions(task.Result, task.SubDocOperationConfig.Exceptions.RetryExceptions)

		// For the retry exceptions :-> move them on success after retrying from err to completed
		for _, exception := range exceptionList {

			errorOffsetListMap := make([]map[int64]RetriedResult, 0)
			for _, failedDocs := range task.Result.BulkError[exception] {
				m := make(map[int64]RetriedResult)
				m[failedDocs.Offset] = RetriedResult{}
				errorOffsetListMap = append(errorOffsetListMap, m)
			}

			routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
			dataChannel := make(chan map[int64]RetriedResult, MaxConcurrentRoutines)
			wg := errgroup.Group{}
			for _, x := range errorOffsetListMap {
				dataChannel <- x
				routineLimiter <- struct{}{}
				wg.Go(func() error {
					m := <-dataChannel
					var offset = int64(-1)
					for k, _ := range m {
						offset = k
					}
					docId, key := task.gen.GetDocIdAndKey(offset)
					fake := faker.NewWithSeed(rand.NewSource(int64(key)))

					task.gen.Template.GenerateSubPathAndValue(&fake)
					task.req.retracePreviousSubDocMutations(task.CollectionIdentifier(), offset, *task.gen, &fake, task.ResultSeed)

					retry := 0
					var err error
					result := &gocb.MutateInResult{}

					for retry = 0; retry <= task.SubDocOperationConfig.Exceptions.RetryAttempts; retry++ {

						var iOps []gocb.MutateInSpec
						for path, value := range task.gen.Template.GenerateSubPathAndValue(&fake) {
							iOps = append(iOps, gocb.ReplaceSpec(path, value, &gocb.ReplaceSpecOptions{
								IsXattr: task.ReplaceSpecOptions.IsXattr,
							}))
						}

						if !task.ReplaceSpecOptions.IsXattr {
							iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
								int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
									CreatePath: false,
									IsXattr:    false,
								}))
						}

						result, err = collectionObject.Collection.MutateIn(docId, iOps, &gocb.MutateInOptions{
							Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
							PersistTo:       task.MutateInOptions.PersistTo,
							ReplicateTo:     task.MutateInOptions.ReplicateTo,
							DurabilityLevel: getDurability(task.MutateInOptions.Durability),
							StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
							Timeout:         time.Duration(task.MutateInOptions.Expiry) * time.Second,
							PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
						})

						if err == nil {
							break
						}
					}

					if err != nil {
						m[offset] = RetriedResult{
							Status: true,
							CAS:    0,
						}
					} else {
						m[offset] = RetriedResult{
							Status: true,
							CAS:    uint64(result.Cas()),
						}
					}

					<-routineLimiter
					return nil
				})
			}
			_ = wg.Wait()

			shiftErrToCompletedOnRetrying(exception, task.Result, errorOffsetListMap, errorOffsetMaps, completedOffsetMaps)
		}
	}

	task.State.MakeCompleteKeyFromMap(completedOffsetMaps)
	task.State.MakeErrorKeyFromMap(errorOffsetMaps)
	task.Result.Failure = int64(len(task.State.KeyStates.Err))

}

func (task *SubDocReplace) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *SubDocReplace) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SubDocReplace) SetException(exceptions Exceptions) {
	task.SubDocOperationConfig.Exceptions = exceptions
}
