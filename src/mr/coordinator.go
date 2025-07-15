package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	mutex sync.Mutex

	files []string

	CompletedMapTasks    []Task
	CompletedReduceTasks []Task

	IdleTasks       []Task
	InProgressTasks []Task

	ReducerToInputFilesMap map[int][]string // maps intermediate files for each reducer task

	NReduce int
	NMap    int

	MappersDone     int
	ReducersDone    int
	AllMappersDone  bool
	AllReducersDone bool
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// if mappers still working and there there is already one worker assigned for each input file, need to wait
	if len(c.IdleTasks) == 0 {
		return errors.New("no idle tasks remained")
	}

	// log.Println("Coordinator was asked to assign task, STATUS:")
	// log.Printf("completed maps: %v, completed reduces %v, in progress: %v, idle: %v\n", len(c.CompletedMapTasks), len(c.CompletedReduceTasks), len(c.InProgressTasks), len(c.IdleTasks))
	if !c.AllMappersDone && c.MappersDone+len(c.InProgressTasks) >= len(c.files) {
		return errors.New("reducer needs to wait, mappers still doing work")
	}

	// assign workerId = number of in progress tasks + 1
	reply.WorkerId = len(c.InProgressTasks)

	reply.Task = c.IdleTasks[0]
	reply.Task.Status = InProgress
	if !c.AllMappersDone {
		reply.Task.TaskType = MapTask
		reply.Task.FileName = c.files[reply.Task.TaskNumber]
	} else {
		reply.Task.TaskType = ReduceTask
		reply.Task.FileNames = c.ReducerToInputFilesMap[reply.Task.TaskNumber]
	}
	reply.Task.StartTime = time.Now()

	// move task from "idle" to "in progress" !!! is important that latest task added to
	// c.InProgressTasks has the start time = time.Now becase the rescheduler might not reschdeule it
	// otherwise because it adds time in future
	c.InProgressTasks = append(c.InProgressTasks, reply.Task)
	updatedIdleTasks, err := c.removeTaskFromCollection(c.IdleTasks, reply.Task.TaskNumber)
	if err != nil {
		// log.Printf("Error while removing task %v from idle", reply.Task.TaskNumber)
	}
	c.IdleTasks = updatedIdleTasks

	// log.Printf("Coordinator assigned task %v to %v\n", reply.Task, reply.WorkerId)

	return nil
}

func (c *Coordinator) MarkTaskAsCompleted(args *MarkTaskAsCompletedArgs, reply *MarkTaskAsCompletedReply) error {
	// lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Task.TaskType == MapTask {
		// check task was already completed by another thread
		if c.checkTaskIsContained(c.CompletedMapTasks, args.Task.TaskNumber) {
			return nil
		}

		c.MappersDone += 1
		if c.MappersDone == len(c.files) {
			// log.Println("All mappers done...")
			c.AllMappersDone = true
		}
		c.CompletedMapTasks = append(c.CompletedMapTasks, args.Task)

		// map the mapper intermediary files to corresponding reducer
		for _, fileName := range args.FileNames {
			parts := strings.Split(fileName, "-")
			bucket, _ := strconv.Atoi(parts[2])
			c.ReducerToInputFilesMap[bucket] = append(c.ReducerToInputFilesMap[bucket], fileName)
		}

		// log.Println("Reducer to files mapping:", c.ReducerToInputFilesMap)

	} else {
		// check task was already completed by another thread
		if c.checkTaskIsContained(c.CompletedReduceTasks, args.Task.TaskNumber) {
			return nil
		}

		c.ReducersDone += 1
		if c.ReducersDone == c.NReduce {
			// log.Println("All reducers done...")
			c.AllReducersDone = true
		}
		c.CompletedReduceTasks = append(c.CompletedReduceTasks, args.Task)
	}

	args.Task.Status = Completed

	updatedInProgressTasks, err := c.removeTaskFromCollection(c.InProgressTasks, args.Task.TaskNumber)
	if err != nil {
		return err
	}
	c.InProgressTasks = updatedInProgressTasks

	// log.Printf("Coordinator marks task %v as completed\n", args.Task.TaskNumber)

	return nil
}

func (c *Coordinator) AskDefaultParameters(args *AskDefaultParametersArgs, reply *AskDefaultParametersReply) error {
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	// log.Printf("Coordinator responded with default params: %v - %v\n", c.NMap, c.NReduce)
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

	//lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := c.AllMappersDone && c.AllReducersDone
	if res {
		// log.Print("Whole Job finished, coordinator exits....")
	}
	return res
}

func (c *Coordinator) reschedule() {
	/*
		Go through all inProgress Tasks and readd the ones that did not complete in 10s in idle
	*/

	for !c.Done() {

		c.mutex.Lock()

		newInProgress := []Task{}
		for _, task := range c.InProgressTasks {
			if time.Since(task.StartTime) > 10*time.Second {
				// add time in future such that it is not rescheduled immediately
				task.StartTime = time.Now().Add(1 * time.Hour)
				// log.Printf("Coordinator is rescheduling %v\n", task.TaskNumber)
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
		main/mrcoordinator.go calls this function.
		nReduce is the number of reduce tasks to use.
	*/
	c := Coordinator{}

	c.NMap = len(files)
	c.NReduce = nReduce

	// create one idle task per input file
	c.files = files
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

	/*
		1. Track state:
			One map task per file inputs
			nReduce reducers (0, nReduce - 1)

			Each task:

		2. Assign task
			Start with map tasks. After all mapper tasks completed, start assigning reduce tasks

		3. Check periodically for tasks stuck inProgress > 10s
			Reset those tasks to idle for reassigning

		4. Accept Task completion
			If a worker reports it completed a task and the task is already completed, ignore it,
			otherwise mark it as completed.
			Save the nReduce intermediate file names for each mapper task (those will be forwarded to reducers)
	*/

	c.server()
	go c.reschedule()

	return &c
}
