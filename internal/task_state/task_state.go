package task_state

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	COMPLETED         = 1
	ERR               = 0
	StateChannelLimit = 10000
	TASKSTATELOGS     = "./internal/task_state/task_state_logs"
)

type StateHelper struct {
	Status int64 `json:"-"`
	Offset int64 `json:"-"`
}

type KeyStates struct {
	Completed []int64 `json:"completed"`
	Err       []int64 `json:"err_sirius"`
}

type TaskState struct {
	ResultSeed   int64              `json:"resultSeed"`
	KeyStates    map[int64]int      `json:"keyStates"`
	StateChannel chan StateHelper   `json:"-"`
	ctx          context.Context    `json:"-"`
	cancel       context.CancelFunc `json:"-"`
	lock         sync.Mutex         `json:"-"`
}

// ConfigTaskState returns an instance of TaskState
func ConfigTaskState(resultSeed int64) *TaskState {
	ctx, cancel := context.WithCancel(context.Background())
	ts := &TaskState{}

	if state, err := ReadStateFromFile(fmt.Sprintf("%d", resultSeed)); err == nil {
		ts = state
		ts.ctx = ctx
		ts.cancel = cancel
		ts.StateChannel = make(chan StateHelper, StateChannelLimit)
		ts.lock = sync.Mutex{}
	} else {
		ts = &TaskState{
			ResultSeed:   resultSeed,
			KeyStates:    make(map[int64]int),
			StateChannel: make(chan StateHelper, StateChannelLimit),
			ctx:          ctx,
			cancel:       cancel,
			lock:         sync.Mutex{},
		}
	}

	defer func() {
		ts.StoreState()
	}()

	defer func() {
		go func() {
			time.Sleep(24 * time.Hour)
			ts = nil
		}()
	}()

	return ts
}

// SetupStoringKeys will initialize contextWithCancel and calls
// the StoreState to start key states.
func (t *TaskState) SetupStoringKeys() {
	ctx, cancel := context.WithCancel(context.Background())
	t.StateChannel = make(chan StateHelper, StateChannelLimit)
	t.ctx = ctx
	t.cancel = cancel
	t.StoreState()
}

// StoreState will receive the offsets on dataChannel after every " d " durations.
// It will append those keys types to Completed or Error Key state .
func (t *TaskState) StoreState() {

	go func() {
		var completed []int64
		var err []int64
		d := time.NewTicker(10 * time.Second)
		defer d.Stop()
		if t.ctx.Err() != nil {
			log.Print("Ctx closed for StoreState()")
			return
		}
		for {
			select {
			case <-t.ctx.Done():
				{
					t.StoreCompleted(completed)
					t.StoreError(err)
					err = err[:0]
					completed = completed[:0]
					close(t.StateChannel)
					t.SaveTaskSateOnDisk()
					return
				}
			case s := <-t.StateChannel:
				{
					if s.Status == COMPLETED {
						completed = append(completed, s.Offset)
					}
					if s.Status == ERR {
						err = append(err, s.Offset)
					}
				}
			case <-d.C:
				{
					t.StoreCompleted(completed)
					t.StoreError(err)
					err = err[:0]
					completed = completed[:0]
					t.SaveTaskSateOnDisk()
				}
			}
		}
	}()

}

// StoreCompleted appends a list of completed offset to Completed Key state
func (t *TaskState) StoreCompleted(completed []int64) {
	t.lock.Lock()
	for _, offset := range completed {
		t.KeyStates[offset] = COMPLETED
	}
	t.lock.Unlock()
}

// StoreError appends a list of error offset to Error Key state
func (t *TaskState) StoreError(err []int64) {
	t.lock.Lock()
	for _, offset := range err {
		t.KeyStates[offset] = ERR
	}
	t.lock.Unlock()
}

// StopStoringState will terminate the thread which is receiving offset
// on dataChannel.
func (t *TaskState) StopStoringState() {
	if t.ctx.Err() != nil {
		return
	}
	t.cancel()
	time.Sleep(1 * time.Second)
}

func (t *TaskState) SaveTaskSateOnDisk() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, TASKSTATELOGS, fmt.Sprintf("%d", t.ResultSeed))
	content, err := json.MarshalIndent(t, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

// ReadStateFromFile reads the task state stored on a file.
func ReadStateFromFile(seed string) (*TaskState, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fileName := filepath.Join(cwd, TASKSTATELOGS, seed)
	state := &TaskState{}
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	if err := json.Unmarshal(content, state); err != nil {
		return nil, err
	}
	return state, nil
}
