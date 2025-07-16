package mr

/*
	RPC definitions, remember to capitalize all names.
*/

import (
	"os"
	"strconv"
)

type EmptyTaskArgs struct {
}

type AssignTaskReply struct {
	Task     Task
	WorkerId int
}

type AskNReduceReply struct {
	NReduce int
}

type MarkTaskAsCompletedArgs struct {
	Task      Task
	WorkerId  int
	FileNames []string // NReduce files for mappers, only one for reducers
}

type MarkTaskAsCompletedReply struct {
	OK  bool
	Err string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
