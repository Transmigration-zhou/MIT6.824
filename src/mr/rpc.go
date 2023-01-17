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

// Add your RPC definitions here.
type TaskType int

const (
	FinishedTask TaskType = iota
	MapTask
	ReduceTask
)

type Task struct {
	TaskType    TaskType
	IsAvailable bool

	MapId       int
	MapFilename string
	NMap        int

	ReduceId int
	NReduce  int
}

type TaskArgs struct {
	TaskType TaskType
	MapId    int
	ReduceId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
