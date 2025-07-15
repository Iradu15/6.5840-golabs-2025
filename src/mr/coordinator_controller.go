package mr

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
