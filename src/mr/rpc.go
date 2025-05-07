package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type CompleteTaskArgs struct {
	Type   string
	TaskID int
}

type CompleteTaskReply struct {
	OK bool
}

// Add your RPC definitions here.

// Task request types
type RequestTask struct{}

type RequestTaskReply struct {
	Type      string // "map" or "reduce" or "wait" or "done"
	TaskID    int
	InputFile string
	NReduce   int
	NMap      int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
