package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

func (c *Coordinator) removeTaskFromCollection(tasks []Task, taskNumber int) ([]Task, error) {
	newTasks := []Task{}
	alreadyRemoved := false

	for _, task := range tasks {
		if task.TaskNumber != taskNumber || alreadyRemoved {
			newTasks = append(newTasks, task)
		} else {
			alreadyRemoved = true
		}
	}

	return newTasks, nil
}

func (c *Coordinator) checkTaskIsContained(tasks []Task, taskNumber int) bool {
	for _, task := range tasks {
		if task.TaskNumber == taskNumber {
			return true
		}
	}
	return false
}

func (c *Coordinator) checkTaskIsAlreadyCompleted(taskNumber int, taskType TaskType) bool {
	/*
		Check if task was already completed by another thread
	*/
	var collection []Task
	if taskType == MapTask {
		collection = c.CompletedMapTasks
	} else {
		collection = c.CompletedReduceTasks
	}
	return c.checkTaskIsContained(collection, taskNumber)
}

func (c *Coordinator) completeMapTask(task Task, fileNames []string) error {
	c.MappersDone += 1
	if c.MappersDone == len(c.inputFiles) {
		log.Println("All mappers done...")
		c.AllMappersDone = true
	}
	c.CompletedMapTasks = append(c.CompletedMapTasks, task)

	// map the mapper's intermediary files to corresponding bucket
	for _, fileName := range fileNames {
		parts := strings.Split(fileName, "-")
		bucket, err := strconv.Atoi(parts[2])
		if err != nil {
			return fmt.Errorf("invalid intermediate filename: %s", fileName)
		}
		c.ReducerToInputFilesMap[bucket] = append(c.ReducerToInputFilesMap[bucket], fileName)
	}

	return nil
}

func (c *Coordinator) completeReduceTask(task Task) {
	c.ReducersDone += 1
	if c.ReducersDone == c.NReduce {
		log.Println("All reducers done...")
		c.AllReducersDone = true
	}
	c.CompletedReduceTasks = append(c.CompletedReduceTasks, task)
}
