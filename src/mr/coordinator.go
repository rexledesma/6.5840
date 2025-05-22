package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type BaseTask struct {
	Status    TaskStatus
	TaskId    int
	StartTime time.Time
}

type MapTask struct {
	BaseTask
	InputFile string
}

type ReduceTask struct {
	BaseTask
}

type Coordinator struct {
	mu          sync.Mutex
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	NReduce     int
}

const timeout = 10 * time.Second

func (t BaseTask) isTimeout() bool {
	return t.Status == InProgress && time.Since(t.StartTime) >= timeout
}

func (t BaseTask) canSchedule() bool {
	return t.Status == Idle || t.isTimeout()
}

// Your code here -- RPC handlers for the worker to call.

// RequestTask is an RPC handler for workers to request a task to work on
func (c *Coordinator) RequestTask(args *RequestTask, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First, check if all map tasks are completed
	mapTasksCompleted := true
	for i := range c.MapTasks {
		if c.MapTasks[i].Status != Completed {
			mapTasksCompleted = false
			break
		}
	}

	// If map tasks are not completed, assign an idle map task
	if !mapTasksCompleted {
		for i, mapTask := range c.MapTasks {
			if mapTask.canSchedule() {
				c.MapTasks[i].Status = InProgress
				c.MapTasks[i].StartTime = time.Now()

				reply.Type = "map"
				reply.TaskID = mapTask.TaskId
				reply.InputFile = mapTask.InputFile
				reply.NReduce = c.NReduce
				reply.NMap = len(c.MapTasks)
				return nil
			}
		}

		// If no tasks are available, ask the worker to wait
		reply.Type = "wait"
		return nil
	}

	// If map tasks are completed, check if all reduce tasks are completed
	reduceTasksCompleted := true
	for i := range c.ReduceTasks {
		if c.ReduceTasks[i].Status != Completed {
			reduceTasksCompleted = false
			break
		}
	}

	// If reduce tasks are not completed, assign an idle reduce task
	if !reduceTasksCompleted {
		for i, reduceTask := range c.ReduceTasks {
			if reduceTask.canSchedule() {
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].StartTime = time.Now()

				reply.Type = "reduce"
				reply.TaskID = reduceTask.TaskId
				reply.NReduce = c.NReduce
				reply.NMap = len(c.MapTasks)
				return nil
			}
		}

		// If no tasks are available, ask the worker to wait
		reply.Type = "wait"
		return nil
	}

	// If all tasks are completed, signal the worker to exit
	reply.Type = "done"
	return nil
}

// CompleteTask is an RPC handler for workers to mark tasks as completed
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case "map":
		mapTask := &c.MapTasks[args.TaskID]
		mapTask.BaseTask.Status = Completed
	case "reduce":
		reduceTask := &c.ReduceTasks[args.TaskID]
		reduceTask.BaseTask.Status = Completed
	}

	reply.OK = true
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Check if all map tasks are completed
	for _, mapTask := range c.MapTasks {
		if mapTask.Status != Completed {
			return false
		}
	}

	// Check if all reduce tasks are completed
	for _, reduceTask := range c.ReduceTasks {
		if reduceTask.Status != Completed {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:    make([]MapTask, len(files)),
		ReduceTasks: make([]ReduceTask, nReduce),
		NReduce:     nReduce,
	}

	// Initialize map tasks
	for mapTaskId, file := range files {
		c.MapTasks[mapTaskId] = MapTask{
			BaseTask: BaseTask{
				Status: Idle,
				TaskId: mapTaskId,
			},
			InputFile: file,
		}
	}

	// Initialize reduce tasks
	for reduceTaskId := range c.ReduceTasks {
		c.ReduceTasks[reduceTaskId] = ReduceTask{
			BaseTask: BaseTask{
				Status: Idle,
				TaskId: reduceTaskId,
			},
		}
	}

	c.server()
	return &c
}
