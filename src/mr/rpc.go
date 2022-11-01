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

// request object during RequestTask RPC
type TaskRequest struct {
	WorkerID int	// process id of worker
}

// response object during RequestTask RPC
type TaskResponse struct {
	TaskType string		// MAP or REDUCE
	TaskNumber int

	// specific to map tasks
	Filename string
	NReduce int		// number of reduce tasks

	// specific to reduce tasks
	NMap int  	// number of map tasks
}


// request object during NotifyTaskCompletion RPC
type NotifyRequest struct {
	TaskType string
	TaskNumber int
	WorkerID int
}

// response object during NotifyTaskCompletion RPC
type NotifyResponse struct {
	Success bool  // not really required tbh
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
