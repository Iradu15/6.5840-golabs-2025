package mr

import "time"

type KeyValue struct {
	Key   string
	Value string
}

type TaskStatus int

const (
	// define the available TaskStatus
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskType int

const (
	// define the available TaskTypes
	MapTask TaskType = iota
	ReduceTask
	Exit
)

type Task struct {
	TaskType   TaskType
	FileName   string   // only for mappers
	FileNames  []string // only for reducers
	TaskNumber int
	StartTime  time.Time
	Status     TaskStatus
}

// for sorting KV values by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
