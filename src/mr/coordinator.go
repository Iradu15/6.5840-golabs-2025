package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mutex sync.Mutex

	inputFiles []string
	NReduce    int
	NMap       int

	MappersDone     int
	ReducersDone    int
	AllMappersDone  bool
	AllReducersDone bool

	CompletedMapTasks    []Task
	CompletedReduceTasks []Task
	IdleTasks            []Task
	InProgressTasks      []Task

	ReducerToInputFilesMap map[int][]string // maps intermediate files for each reducer task

	WorkerCounter int
	
	DoneLogPrint bool
}

func (c *Coordinator) AssignTask(args *EmptyTaskArgs, reply *AssignTaskReply) error {
	/*
		Assign task and a worker id.
		Start with map tasks. After all mapper tasks completed, start assigning reduce tasks
	*/
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.IdleTasks) == 0 {
		return errors.New("no idle tasks available for assignment")
	}

	log.Println("Coordinator was asked to assign task, STATUS:")
	log.Printf("completed maps: %v, completed reduces %v, in progress: %v, idle: %v\n", len(c.CompletedMapTasks), len(c.CompletedReduceTasks), len(c.InProgressTasks), len(c.IdleTasks))
	remainingMapperTasks := len(c.inputFiles) - c.MappersDone - len(c.InProgressTasks)
	if !c.AllMappersDone && remainingMapperTasks <= 0 {
		return errors.New("reducer needs to wait, mappers still doing work")
	}

	reply.WorkerId = c.WorkerCounter
	c.WorkerCounter += 1

	// complete task information before moving it to in progress
	reply.Task = c.IdleTasks[0]
	reply.Task.Status = InProgress
	if !c.AllMappersDone {
		reply.Task.TaskType = MapTask
		reply.Task.FileName = c.inputFiles[reply.Task.TaskNumber]
	} else {
		reply.Task.TaskType = ReduceTask
		reply.Task.FileNames = c.ReducerToInputFilesMap[reply.Task.TaskNumber]
	}
	reply.Task.StartTime = time.Now()

	/*
		Move task from idle to in progress

		IMPORTANT: Set task.StartTime = time.Now when adding to c.InProgressTasks.
		The rescheduler uses this timestamp to detect stale tasks. If StartTime is not accurate
		(e.g., set to a future time), the task may not be rescheduled properly if a worker fails to complete it.
	*/
	c.InProgressTasks = append(c.InProgressTasks, reply.Task)
	c.IdleTasks = c.IdleTasks[1:]
	log.Printf("Coordinator assigned task %v to %v\n", reply.Task, reply.WorkerId)

	return nil
}

func (c *Coordinator) MarkTaskAsCompleted(args *MarkTaskAsCompletedArgs, reply *MarkTaskAsCompletedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.checkTaskIsAlreadyCompleted(args.Task.TaskNumber, args.Task.TaskType) {
		return nil
	}

	if args.Task.TaskType == MapTask {
		err := c.completeMapTask(args.Task, args.FileNames)
		if err != nil {
			return err
		}
	} else {
		c.completeReduceTask(args.Task)
	}
	args.Task.Status = Completed

	updatedInProgressTasks, err := c.removeTaskFromCollection(c.InProgressTasks, args.Task.TaskNumber)
	if err != nil {
		return err
	}
	c.InProgressTasks = updatedInProgressTasks
	log.Printf("Coordinator marks task %v as completed\n", args.Task.TaskNumber)

	return nil
}

func (c *Coordinator) AskNReduce(args *EmptyTaskArgs, reply *AskNReduceReply) error {
	reply.NReduce = c.NReduce
	return nil
}

func (c *Coordinator) server() {
	/*
		Start a thread that listens for RPCs from worker.go
	*/
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	/*
		main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
	*/
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := c.AllMappersDone && c.AllReducersDone
	if res && !c.DoneLogPrint {
		log.Print("Whole Job finished, coordinator exits....")
		c.DoneLogPrint = true
	}
	return res
}

func (c *Coordinator) reschedule() {
	/*
		Check periodically for tasks stuck inProgress > 10s.
		Add those to idle for reassigning.
	*/
	for !c.Done() {
		c.mutex.Lock()

		newInProgress := []Task{}
		for _, task := range c.InProgressTasks {
			if time.Since(task.StartTime) > 10*time.Second {
				// add time in future such that it is not rescheduled in a loop immediately
				task.StartTime = time.Now().Add(1 * time.Hour)

				log.Printf("Coordinator is rescheduling %v\n", task.TaskNumber)
				c.IdleTasks = append([]Task{task}, c.IdleTasks...)

				/*
					Remove current task from inProgress:
					- because coordinator assigns another map task only if |inProgress| + |completed| <= |files|
					and unless removed from inProgress, the map task would not be reassigned
				*/
			} else {
				newInProgress = append(newInProgress, task)
			}
		}
		c.InProgressTasks = newInProgress
		c.mutex.Unlock()

		time.Sleep(1 * time.Second)
	}

}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	/*
		Create a Coordinator.
		Params:
			- nReduce is the number of reduce tasks to use.
			- files is the collection of input files that will be assigned to mappers
	*/
	os.Remove(coordinatorSock())
	c := Coordinator{}
	c.NReduce = nReduce
	c.WorkerCounter = 0
	c.DoneLogPrint = false

	// create one idle task per input file
	c.inputFiles = files
	for i := 0; i < len(files); i++ {
		idleTask := Task{MapTask, files[i], []string{}, i, time.Now(), Idle}
		c.IdleTasks = append(c.IdleTasks, idleTask)
	}

	// create NReduce idle tasks for the reducers
	for i := 0; i < nReduce; i++ {
		idleTask := Task{ReduceTask, "", []string{}, i, time.Now(), Idle}
		c.IdleTasks = append(c.IdleTasks, idleTask)
	}

	// assign empty list for each bucket
	c.ReducerToInputFilesMap = make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		c.ReducerToInputFilesMap[i] = []string{}
	}

	c.server()
	go c.reschedule()

	return &c
}
