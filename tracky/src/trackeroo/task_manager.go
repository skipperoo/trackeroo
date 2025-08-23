package trackeroo

import "os"

type TaskManager struct {
	Task
	tasks []ITask
}

const (
	WATCHDOG int = 98
)

func NewTaskManager() *TaskManager {
	taskManager := new(TaskManager)
	zdmClient := NewTdmClient()
	taskManager.tasks = append(taskManager.tasks, zdmClient)
	taskManager.tasks = append(taskManager.tasks, NewAgent(zdmClient))
	taskManager.name = "Task Manager"
	return taskManager
}

func (tm *TaskManager) Shutdown(reason int) {
	tm.Reset(reason)
}

func (tm *TaskManager) Start() {
	tm.running = true
	for _, task := range tm.tasks {
		task.Start()
	}
	for tm.running {
		now := UnixTime()
		for _, task := range tm.tasks {
			if task.IsResetting() {
				tm.running = false
				tm.reason = task.Reason()
				break
			}
			if (now - uint64(task.LastKick())) > uint64(task.Wtd()) {
				Warning("Task %s triggered the watchdog, resetting...", task.Name())
				tm.running = false
				tm.reason = WATCHDOG
			}
		}
		Millisleep(1000)
	}
	Warning("Starting shutdown sequence")
	for _, task := range tm.tasks {
		task.Kill() // Maybe add wg done group
	}
	Millisleep(5000)
	os.Exit(1)
}
