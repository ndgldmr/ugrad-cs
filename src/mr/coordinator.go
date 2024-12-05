package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"

type Coordinator struct {
	inputFiles []string 
	nReduce int
	mapTasks []MapReduceTask
	reduceTasks []MapReduceTask
	mapComplete int
	reduceComplete int
	allMapComplete bool
	allReduceComplete bool
	mutex sync.Mutex
}

func (c *Coordinator) NotifyComplete(argument *RequestTaskReply, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch argument.Task.Task {
	case Map:
		c.mapTasks[argument.TaskNumber] = argument.Task
	case Reduce:
		c.reduceTasks[argument.TaskNumber] = argument.Task
	}
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskReply, reply *RequestTaskReply) error {
	c.mutex.Lock() 
	defer c.mutex.Unlock()
	if c.mapComplete < len(c.inputFiles) {
		reply.TaskNumber = c.mapComplete
		c.mapTasks[c.mapComplete].TimeStamp = time.Now()
		reply.Task = c.mapTasks[c.mapComplete]
		reply.Task.Status = Assigned
		reply.NReduce = c.nReduce
		c.mapComplete++
		return nil
	}
	if c.allMapComplete == false {
		for index, mapTask := range c.mapTasks {
			if mapTask.Status != Finished {
				if time.Since(mapTask.TimeStamp) > 10*time.Second {
					reply.TaskNumber = index
					c.mapTasks[index].TimeStamp = time.Now()
					reply.Task = c.mapTasks[index]
					reply.Task.Status = Assigned
					reply.NReduce = c.nReduce
					return nil
				} else {
					reply.Task.Task = Wait
					return nil
				}
			}
		}
		c.allMapComplete = true
	}
	if c.reduceComplete < c.nReduce {
		reply.TaskNumber = c.reduceComplete
		c.reduceTasks[c.reduceComplete].TimeStamp = time.Now()
		reply.Task = c.reduceTasks[c.reduceComplete]
		reply.Task.Status = Assigned
		reply.NReduce = c.nReduce
		c.reduceComplete++
		return nil
	}
	if c.allReduceComplete == false {
		for index, reduceTask := range c.reduceTasks {
			if reduceTask.Status != Finished {
				if time.Since(reduceTask.TimeStamp) > 10*time.Second {
					reply.TaskNumber = index
					c.reduceTasks[index].TimeStamp = time.Now()
					reply.Task = c.reduceTasks[index]
					reply.Task.Status = Assigned
					reply.NReduce = c.nReduce
					return nil
				} else {
					reply.Task.Task = Wait
					return nil
				}
			}
		}
		c.allReduceComplete = true
	}
	reply.Task.Status = Status(Exit)
	return nil
}

func (c *Coordinator) StringReverse(args *StringReverseArgs, reply *StringReverseReply) error {
    reversedStr := reverseString(args.Str)
    fmt.Printf("Coordinator: %s\n", reversedStr)
    return nil
}

func reverseString(s string) string {
    runes := []rune(s)
    for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
        runes[i], runes[j] = runes[j], runes[i]
    }
    return string(runes)
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) server() {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.allMapComplete && c.allReduceComplete
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:        files,
		nReduce:           nReduce,
		mapTasks:          make([]MapReduceTask, len(files)),
		reduceTasks:       make([]MapReduceTask, nReduce),
		mapComplete:       0,
		reduceComplete:    0,
		allMapComplete:    false,
		allReduceComplete: false,
		mutex:             sync.Mutex{},
	}
	initMapTasks := func() {
		for i := range c.mapTasks {
			c.mapTasks[i] = MapReduceTask{
				Task:        Map,
				Status:      Unassigned,
				TimeStamp:   time.Now(),
				Index:       i,
				InputFiles:  []string{files[i]},
				OutputFiles: nil,
			}
		}
	}
	generateInputFiles := func(index, totalFiles int) []string {
		var inputs []string
		for i := 0; i < totalFiles; i++ {
			inputs = append(inputs, fmt.Sprintf("mr-%d-%d", i, index))
		}
		return inputs
	}
	initReduceTasks := func() {
		for i := range c.reduceTasks {
			c.reduceTasks[i] = MapReduceTask{
				Task:        Reduce,
				Status:      Unassigned,
				TimeStamp:   time.Now(),
				Index:       i,
				InputFiles:  generateInputFiles(i, len(files)),
				OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
			}
		}
	}
	initMapTasks()
	initReduceTasks()
	c.server()
	return &c
}