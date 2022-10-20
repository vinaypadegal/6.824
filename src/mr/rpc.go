package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

type TaskRequest struct {
	WorkerID int
}

type TaskResponse struct {
	TaskType string
	TaskNumber int

	// specific to map tasks
	Filename string
	NReduce int

	// specific to reduce tasks
	NMap int
}


type NotifyRequest struct {
	TaskType string
	TaskNumber int
	WorkerID int
}

type NotifyResponse struct {
	Success bool  // not really required tbh
}


// type MapRequest struct {}

// type MapReply struct {
// 	filename string
// 	mapper int
// 	nReduce int
// 	mapsDone bool
// }


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
