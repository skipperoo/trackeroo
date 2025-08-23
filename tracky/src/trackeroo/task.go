package trackeroo

type ITask interface {
	Name() string
	Start()
	IsRunning() bool
	Kick()
	LastKick() int
	Kill()
	Reset(reason int)
	Reason() int
	IsResetting() bool
	Wtd() int
}

type Task struct {
	name      string
	running   bool
	resetting bool
	reason    int
	wtd       int
	kicked    uint64
}

func NewTask(name string) *Task {
	taskState := new(Task)
	taskState.name = name
	taskState.kicked = UnixTime()
	taskState.resetting = false
	taskState.running = false
	taskState.wtd = 120
	return taskState
}

func (t *Task) Kick() {
	t.kicked = UnixTime()
}

func (t *Task) Kill() {
	t.running = false
}

func (t *Task) Reset(reason int) {
	t.reason = reason
	t.resetting = true
}

func (t *Task) Reason() int {
	return t.reason
}

func (t *Task) IsResetting() bool {
	return t.resetting
}

func (t *Task) IsRunning() bool {
	return t.resetting
}

func (t *Task) LastKick() int {
	return int(t.kicked)
}

func (t *Task) Wtd() int {
	return t.wtd
}

func (t *Task) Name() string {
	return t.name
}
