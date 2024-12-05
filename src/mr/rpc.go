package mr

import "os"
import "strconv"
import "time"

type Task int
type Status int

const (
	Exit Task = iota
	Wait
	Map
	Reduce
)

const (
	Unassigned Status = iota
	Assigned
	Finished
)

type MapReduceTask struct {
	Task      Task
	Status    Status
	TimeStamp time.Time
	Index     int
	InputFiles  []string
	OutputFiles []string
}

type RequestTaskReply struct {
	TaskNumber  int
	Task MapReduceTask
	NReduce int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type StringReverseArgs struct {
    Str string
}

type StringReverseReply struct {
    // This can include fields if needed for acknowledgment or result status.
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
