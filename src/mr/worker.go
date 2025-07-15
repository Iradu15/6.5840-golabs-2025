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

	NReduce, _, err := CallDefaultParameters()
	if err != nil {
		log.Fatal(err)
	}
	// log.Printf("Default params received from coordinator: %v \n", NReduce)

	for {
		task, workerId, err := CallAssignTask()
		/*
			If the worker fails to contact the coordinator, it can assume that the coordinator
			has exited because the job is done, so the worker can terminate too
		*/
		if err != nil {
			if err.Error() == "reducer needs to wait, mappers still doing work" ||
				err.Error() == "no idle tasks remained" {
				time.Sleep(5 * time.Second)
				continue
			} else if err.Error() == "CallAssignTask failed, could not contact coordinator" {
				// log.Printf("Worker %v is finishing the job...", task.TaskNumber)
				os.Exit(0)
			} else {
				log.Fatal(err)
			}
		}

		// log.Printf("Worker %v was assigned %v\n", workerId, task)

		switch task.TaskType {
		case 0:
			HandleMapTask(mapf, task, NReduce, workerId)
		case 1:
			HandleReduceTask(reducef, task, NReduce, workerId)
		default:
			fmt.Printf("Worker %v received instruction to exit from coordinator", workerId)
			os.Exit(0)
		}

		time.Sleep(5 * time.Second)
	}
}

func HandleMapTask(mapf func(string, string) []KeyValue, task Task, NReduce int, workerId int) {
	/*
		Flow:
		- get file content from the received input
		- call mapf() using the file_name and the extracted_content
		- Mapper should group in nReduce intermediate files for consumption by each of the reduce tasks. A
			file might have different keys.
			Example: 1 mapper and 2 workers and mapper outputs (apple, 1), (banana, 1), (pear, 1) but there are 2 buckets,
			so 1 bucket will have at least 2 different words;
		- Write the kv(key-value) to a file called mr-X-Y (X is map task number, Y is the bucket number / reduce task number)
		- When a map task completes, the worker sends a message to the master and includes the names of the R temporary
		files in the message.
		- The worker should put intermediate Map output in files in the current directory, where your worker can later
		read them as input to Reduce tasks.
	*/
	// log.Printf("Worker %v is handling map task\n", workerId)
	content, err := GetFileContent(task.FileName)
	if err != nil {
		log.Fatal(err)
	}

	kva := mapf(task.FileName, content)
	for i := 0; i < len(kva); i++ {
		bucket := ihash(kva[i].Key) % NReduce

		// attention if the file already exists
		tmpFileName := fmt.Sprintf("mr-%v-%v-tmp", task.TaskNumber, bucket)
		if !FileExists(tmpFileName) {
			// log.Printf("PULA PULA tmp file Name %v\n", tmpFileName)
			_, err := os.Create(tmpFileName)
			if err != nil {
				log.Fatalf("Error while creating tmp file %v for mapper task: %v", tmpFileName, err)
			}
		}

		AppendKvToFile(tmpFileName, kva[i])
	}

	// rename all [0, NRange) files atomically and store them in a list
	intermediateFileLocations := []string{}
	for i := 0; i < NReduce; i++ {
		tmpFileName := fmt.Sprintf("mr-%v-%v-tmp", task.TaskNumber, i)

		// create a file corresponding to each reducer(in case they don't already exist)
		if !FileExists(tmpFileName) {
			_, err := os.Create(tmpFileName)
			// log.Printf("PULA PULA Created %v because it didnt already exist \n", tmpFileName)
			if err != nil {
				log.Fatalf("Error while creating tmp file %v for mapper task: %v", tmpFileName, err)
			}
		}

		fileName := fmt.Sprintf("mr-%v-%v", task.TaskNumber, i)
		os.Rename(tmpFileName, fileName)
		intermediateFileLocations = append(intermediateFileLocations, fileName)
	}

	task.Status = Completed
	// log.Printf("Worker %v finished his task\n", workerId)
	CallMarkTaskAsCompleted(intermediateFileLocations, workerId, task)
}

func HandleReduceTask(reducef func(string, []string) string, task Task, NReduce int, workerId int) {
	/*
		Flow:
		- A reducer can't start unless all mapper tasks finished
		- Retrieve file content from task.FileNames
		- In case there are multiple keys they need to sort by them and for each one of them call the
		reducef() with the grouped key and its corresponding values
		- A mr-out-X file should contain one line per Reduce function output. (main/mrsequential.go :
		"this is the correct format")
		- Each in-progress task writes its output to private temporary files.
		- When a reduce task completes, the reduce worker atomically renames its temporary output file to the final
		output file.
	*/
	// log.Printf("Worker %v starts reducer task %v with files: %v\n", workerId, task.TaskNumber, task.FileNames)

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

	if !FileExists(tmpOutputName) {
		os.Create(tmpOutputName)
	}

	ofile, err := os.OpenFile(tmpOutputName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}

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
	ofile.Close()

	task.Status = Completed
	// log.Printf("Worker %v finishes reducer task %v\n", workerId, task.TaskNumber)
	fileNameAsList := []string{oname}
	CallMarkTaskAsCompleted(fileNameAsList, workerId, task)
}

func CallAssignTask() (Task, int, error) {
	/*
		Ask coordinator for task
	*/
	// log.Printf("11111111111111111111111111111111111111111111111111\n")
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}
	ok, err := call("Coordinator.AssignTask", &args, &reply)

	if !ok {
		// log.Printf("CallAssignTask failed: %v\n", err)
		return Task{}, 0, err
	}

	// fmt.Printf("Task received by %v: %v\n", reply.WorkerId, reply.Task)

	// log.Printf("1111111111111111111    FINISH  1111111111111111111111111111111\n")

	return reply.Task, reply.WorkerId, nil
}

func CallDefaultParameters() (int, int, error) {
	/*
		Ask coordinator for default parameters: NReduce, NMap
	*/
	// log.Printf("222222222222222222222222222222222222222222222222\n")
	args := AskDefaultParametersArgs{}
	reply := AskDefaultParametersReply{}
	ok, _ := call("Coordinator.AskDefaultParameters", &args, &reply)

	if !ok {
		return -1, -1, fmt.Errorf("AskDefaultParameters failed, could not contact coordinator")
	}

	// log.Printf("2222222222222222222222    FINISH  2222222222222222222222\n")

	return reply.NReduce, reply.NMap, nil
}

func CallMarkTaskAsCompleted(intermediateFileLocations []string, workerId int, task Task) {
	/*
		Ask coordinator to mark task as completed
	*/
	// log.Printf("3333333333333333333333333333333333333333\n")
	args := MarkTaskAsCompletedArgs{task, workerId, intermediateFileLocations}
	reply := MarkTaskAsCompletedReply{}
	ok, _ := call("Coordinator.MarkTaskAsCompleted", &args, &reply)
	if ok {
		_, err := reply.OK, reply.Err
		if err != "" {
			// fmt.Printf("Coordinator could not mark task %v as completed\n", task.TaskNumber)
		} else {
			// fmt.Printf("Coordinator marked task %v as completed\n", task.TaskNumber)
		}
	} else {
		// fmt.Printf("Call (MarkTaskAsCompleted) failed")
	}

	// log.Printf("33333333333333333    FINISH  33333333333333333\n")

}

func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	/*
		Send an RPC request to the coordinator, wait for the response.
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
