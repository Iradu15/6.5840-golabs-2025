package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

func ihash(key string) int {
	/*
		Use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
	*/
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	/*
		main/mrworker.go calls this function
	*/

	NReduce, err := CallNReduce()
	if err != nil {
		log.Fatal(err)
	}

	for {
		task, workerId, err := CallAssignTask()
		/*
			If the worker fails to contact the coordinator, it can assume that the coordinator
			has exited because the job is done, so the worker can terminate too
		*/
		if err != nil {
			if err.Error() == "reducer needs to wait, mappers still doing work" ||
				err.Error() == "no idle tasks available for assignment" {
				time.Sleep(5 * time.Second)
				continue
			} else {
				log.Fatal(err)
			}
		}

		switch task.TaskType {
		case 0:
			HandleMapTask(mapf, task, NReduce, workerId)
		case 1:
			HandleReduceTask(reducef, task, NReduce, workerId)
		default:
			log.Printf("Worker %v received instruction to exit from coordinator", workerId)
			os.Exit(0)
		}

		time.Sleep(5 * time.Second)
	}
}

func HandleMapTask(mapf func(string, string) []KeyValue, task Task, NReduce int, workerId int) {
	/*
		For documentation details see comments from bottom of page [1]
	*/
	content, err := GetFileContent(task.FileName)
	if err != nil {
		log.Fatal(err)
	}

	kva := mapf(task.FileName, content)
	for i := 0; i < len(kva); i++ {
		bucket := ihash(kva[i].Key) % NReduce
		tmpFileName := fmt.Sprintf("mr-%v-%v-tmp", task.TaskNumber, bucket)
		err = CreateFile(tmpFileName)
		if err != nil {
			log.Fatalf("Error while creating tmp file %v for mapper task: %v", tmpFileName, err)
		}

		AppendKvToFile(tmpFileName, kva[i])
	}

	// rename (& create unless existent) all [0, NRange) files atomically and store them in a list
	intermediateFileLocations := []string{}
	for i := 0; i < NReduce; i++ {
		tmpFileName := fmt.Sprintf("mr-%v-%v-tmp", task.TaskNumber, i)
		err = CreateFile(tmpFileName)
		if err != nil {
			log.Fatalf("Error while creating tmp file %v for mapper task: %v", tmpFileName, err)
		}

		fileName := fmt.Sprintf("mr-%v-%v", task.TaskNumber, i)
		os.Rename(tmpFileName, fileName)
		intermediateFileLocations = append(intermediateFileLocations, fileName)
	}

	task.Status = Completed
	log.Printf("Worker %v finished map task %v\n", workerId, task.TaskNumber)
	CallMarkTaskAsCompleted(intermediateFileLocations, workerId, task)
}

func HandleReduceTask(reducef func(string, []string) string, task Task, NReduce int, workerId int) {
	/*
		For documentation details see comments from bottom of page [2]
	*/
	kva := []KeyValue{}
	for i := 0; i < len(task.FileNames); i++ {
		fileName := task.FileNames[i]
		fileKva, err := GetKvFromFile(fileName)
		if err != nil {
			log.Fatal(err)
		}
		kva = append(kva, fileKva...)
	}

	sort.Sort(ByKey(kva))
	tmpOutputName := fmt.Sprintf("mr-out-%v-tmp", task.TaskNumber)
	CreateFile(tmpOutputName)

	ofile, err := os.OpenFile(tmpOutputName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer ofile.Close()

	// call Reduce on each distinct key in intermediate[],
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	oname := fmt.Sprintf("mr-out-%v", task.TaskNumber)
	os.Rename(tmpOutputName, oname)

	task.Status = Completed
	fileNameAsList := []string{oname}
	log.Printf("Worker %v finished reduce task %v\n", workerId, task.TaskNumber)
	CallMarkTaskAsCompleted(fileNameAsList, workerId, task)
}

func CallAssignTask() (Task, int, error) {
	/*
		Ask coordinator for task
	*/
	args := EmptyTaskArgs{}
	reply := AssignTaskReply{}
	ok, err := call("Coordinator.AssignTask", &args, &reply)

	if !ok {
		log.Printf("CallAssignTask failed: %v\n", err)
		return Task{}, 0, err
	}

	return reply.Task, reply.WorkerId, nil
}

func CallNReduce() (int, error) {
	/*
		Ask coordinator for NReduce
	*/
	args := EmptyTaskArgs{}
	reply := AskNReduceReply{}
	ok, err := call("Coordinator.AskNReduce", &args, &reply)

	if !ok {
		return -1, err
	}

	return reply.NReduce, nil
}

func CallMarkTaskAsCompleted(intermediateFileLocations []string, workerId int, task Task) {
	/*
		Ask coordinator to mark task as completed
	*/
	args := MarkTaskAsCompletedArgs{task, workerId, intermediateFileLocations}
	reply := MarkTaskAsCompletedReply{}
	ok, err := call("Coordinator.MarkTaskAsCompleted", &args, &reply)
	if ok {
		if reply.Err != "" {
			log.Printf("Coordinator could not mark task %v as completed\n", task.TaskNumber)
		}
		return
	}

	log.Printf("Call (MarkTaskAsCompleted) failed: %v\n", err)
}

func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	/*
		Send RPC request to the coordinator, wait for the response.
		Returns false if something goes wrong, otherwise true
	*/
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}

	fmt.Println(err)
	return false, err
}

/*
	[1]
	Map Task Flow:

	- Read the contents of the input file provided by the coordinator.
	- Call the map function (mapf) with the file name and its contents to generate a list of key-value pairs.
	- Partition the key-value pairs into nReduce buckets using a hash of the key.
	For example, with 1 mapper and 2 reduce tasks, keys like (apple, 1), (banana, 1), (pear, 1)
	will be distributed across 2 buckets. A bucket can contain multiple unique keys.
	- Write each bucket’s contents to a file named "mr-X-Y", where:
		X = map task number
		Y = reduce task number (bucket index)
	- After completing the map task, the worker informs the coordinator and includes the names
	of all intermediate files it produced.
	- Intermediate files should be written to the current working directory, to be read later
	by reduce workers.
*/

/*
	[2]
	Reduce Task Flow:

	- A reduce task can only begin after all map tasks have completed.
	- Read intermediate files listed in task.FileNames and extract key-value pairs.
	- Sort the key-value pairs by key.
	- For each unique key, group its values and invoke reducef(key, values).
	- The output should be written as one line per key to a file named "mr-out-X",
	  where X is the reduce task number. (See main/mrsequential.go for reference format.)
	- Each reduce task writes to a temporary file first to avoid partial results.
	- Once complete, the temporary file is atomically renamed to its final output file.
*/
