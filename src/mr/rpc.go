package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


type AssignTaskArgs struct{
	WorkerId int
}

type AssignTaskReply struct{
	Task Task
	WorkerId int
}


type AskDefaultParametersArgs struct{
}

type AskDefaultParametersReply struct{
	NReduce int
	NMap int
}


type MarkTaskAsCompletedArgs struct{
	Task Task
	WorkerId int
	FileNames []string // NReduce for a mapper, one for a reducer
}

type MarkTaskAsCompletedReply struct{
	OK bool
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
