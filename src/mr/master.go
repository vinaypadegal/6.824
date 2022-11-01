package mr

import "log"
import "net"
import "os"
import "time"
import "sync"
import "net/rpc"
import "net/http"


type MapTask struct {
	taskStatus string    // MAP or REDUCE
	timeBegun time.Time    // time at which map task was assigned to worker 
	workerID int    // process id of worker to which task was assigned to
	filename string    // file which this task should read
	// taskNumber int
}

type ReduceTask struct {
	taskStatus string
	timeBegun time.Time
	workerID int
	// taskNumber int
}

type Master struct {
	// Your definitions here.
	mapTasks map[int]*MapTask    // list of map tasks -- each map task is a mapTask object
	reduceTasks map[int]*ReduceTask    // list of reduce tasks -- each reduce task is a reduceTask object

	// files []string
	nReduce int    // number of reducers
	nMap int    // number of mappers
	threshold time.Duration    // threshold after which task is reassigned to some other worker
	mapTasksCompleted bool
	reduceTasksCompleted bool

	mu sync.Mutex    // mutex lock to ensure mutual exclusion
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


// Assign reduce task if map tasks over, else assign map task
func (m *Master) RequestTask(request *TaskRequest, reply *TaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mapTasksCompleted == true {
		return m.AssignReduceTask(request, reply)
	} else {
		return m.AssignMapTask(request, reply)
	}
}


// Record task completion and check if all tasks complete
func (m *Master) NotifyTaskCompletion(req *NotifyRequest, res *NotifyResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.TaskType == "MAP" {
		completedTask := m.mapTasks[req.TaskNumber]
		if completedTask.workerID == req.WorkerID {
			completedTask.taskStatus = "Completed"    // mark task as completed
		}

		// check if all map tasks completed
		allMapsDone := true
		for _, v := range m.mapTasks {
			if v.taskStatus != "Completed" {
				allMapsDone = false
				break
			}
		}
		if allMapsDone {
			m.mapTasksCompleted = true
			log.Printf("Map tasks completed. Assigning reduce tasks now\n")
		}
	} else {
		completedTask := m.reduceTasks[req.TaskNumber]
		if completedTask.workerID == req.WorkerID {
			completedTask.taskStatus = "Completed"    // mark task as completed
		}

		// check if all reduce tasks completed
		allReducesDone := true
		for _, v := range m.reduceTasks {
			if v.taskStatus != "Completed" {
				allReducesDone = false
				break
			}
		}
		if allReducesDone {
			m.reduceTasksCompleted = true
			log.Printf("Reduce tasks completed. Master exiting...\n")
		}
	}
	res.Success = true
	return nil
}


// assign map task to worker
func (m *Master) AssignMapTask(request *TaskRequest, reply *TaskResponse) error {
	for k, v := range m.mapTasks {
		if v.taskStatus == "Not Started" {
			reply.TaskType = "MAP"
			reply.TaskNumber = k
			reply.Filename = v.filename
			reply.NReduce = m.nReduce
			v.taskStatus = "In Progress"
			v.timeBegun = time.Now()    // note down current time in mapTask object
			v.workerID = request.WorkerID    // note down the worker ID to which we assigning this task
			log.Printf("Assigning new map task %d to worker %d\n", k, request.WorkerID)
			break
		}
		if v.taskStatus == "In Progress" {
			// in this case, we check if 10 seconds threshold has been exceeded since 
			// the task was assigned to the previous worker
			if time.Now().Sub(v.timeBegun) > m.threshold { 
				oldWorker := v.workerID
				reply.TaskType = "MAP"
				reply.TaskNumber = k
				reply.Filename = v.filename
				reply.NReduce = m.nReduce
				v.timeBegun = time.Now()    // note down current time in mapTask object
				v.workerID = request.WorkerID    // note down the worker ID to which we assigning this task
				log.Printf("Re-assigning map task %d to worker %d, old worker was %d\n", k, request.WorkerID, oldWorker)
				break
			}
		} 
	}
	return nil
}


// assign reduce task
func (m *Master) AssignReduceTask(request *TaskRequest, reply *TaskResponse) error {
	for i := 0; i < m.nReduce; i++ {
		v := m.reduceTasks[i]
		if v.taskStatus == "Not Started" {
			reply.TaskType = "REDUCE"
			reply.TaskNumber = i
			reply.NMap = m.nMap
			v.taskStatus = "In Progress"
			v.timeBegun = time.Now()    // note down current time in reduceTask object
			v.workerID = request.WorkerID    // note down the worker ID to which we assigning this task
			log.Printf("Assigning new reduce task %d to worker %d\n", i, request.WorkerID)
			break
		}
		if v.taskStatus == "In Progress" {
			if time.Now().Sub(v.timeBegun) > m.threshold {
				oldWorker := v.workerID
				reply.TaskType = "REDUCE"
				reply.TaskNumber = i
				reply.NMap = m.nMap
				v.timeBegun = time.Now()    // note down current time in reduceTask object
				v.workerID = request.WorkerID    // note down the worker ID to which we assigning this task
				log.Printf("Re-assigning reduce task %d to worker %d, old worker was %d\n", i, request.WorkerID, oldWorker)
				break
			}
		} 
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	// if both map tasks and reduce tasks completed, return true. Else, return false.
	ret := m.mapTasksCompleted && m.reduceTasksCompleted

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks: make(map[int]*MapTask),
		reduceTasks: make(map[int]*ReduceTask),

		nMap: len(files),    // we have 8 files, so 8 mappers. One map task per file. 
		nReduce: nReduce,    // 10 reduce tasks
		threshold: time.Second * 10,    // default threshold of 10 seconds used

		mapTasksCompleted: false,
		reduceTasksCompleted: false,
	}

	// initialise each mapTask in the list mapTasks with default values
	for k, v := range files {
		mapTask := &MapTask{
			taskStatus: "Not Started",
			filename: v,
			// taskNumber: k
		}
		m.mapTasks[k] = mapTask
	}

	// initialise each reduceTask in the list reduceTasks with default values
	i := 0
	for i < nReduce {
		reduceTask := &ReduceTask{
			taskStatus: "Not Started",
			// taskNumber: i
		}
		m.reduceTasks[i] = reduceTask
		i = i + 1
	}

	log.Printf("Master: %d map tasks, %d reduce tasks\n", m.nMap, m.nReduce)

	m.server()
	return &m
}
