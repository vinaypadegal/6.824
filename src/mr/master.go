package mr

import "log"
import "net"
import "os"
import "time"
import "sync"
import "net/rpc"
import "net/http"


type MapTask struct {
	taskStatus string
	timeBegun time.Time
	workerID int
	filename string
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
	mapTasks map[int]*MapTask
	reduceTasks map[int]*ReduceTask

	// files []string
	nReduce int
	nMap int
	threshold time.Duration
	mapTasksCompleted bool
	reduceTasksCompleted bool

	mu sync.Mutex
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


func (m *Master) RequestTask(request *TaskRequest, reply *TaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mapTasksCompleted == true {
		log.Println("Map tasks completed. Assigning reduce tasks now")
		return m.AssignReduceTask(request, reply)
	} else {
		return m.AssignMapTask(request, reply)
	}
}


func (m *Master) NotifyTaskCompletion(req *NotifyRequest, res *NotifyResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.TaskType == "MAP" {
		completedTask := m.mapTasks[req.TaskNumber]
		if completedTask.workerID == req.WorkerID {
			completedTask.taskStatus = "Completed"
		}

		allMapsDone := true
		for _, v := range m.mapTasks {
			if v.taskStatus != "Completed" {
				allMapsDone = false
				break
			}
		}
		if allMapsDone {
			m.mapTasksCompleted = true
		}
	} else {
		completedTask := m.reduceTasks[req.TaskNumber]
		if completedTask.workerID == req.WorkerID {
			completedTask.taskStatus = "Completed"
		}

		allReducesDone := true
		for _, v := range m.reduceTasks {
			if v.taskStatus != "Completed" {
				allReducesDone = false
				break
			}
		}
		if allReducesDone {
			m.reduceTasksCompleted = true
		}
	}
	res.Success = true
	return nil
}


func (m *Master) AssignMapTask(request *TaskRequest, reply *TaskResponse) error {
	for k, v := range m.mapTasks {
		if v.taskStatus == "Not Started" {
			reply.TaskType = "MAP"
			reply.TaskNumber = k
			reply.Filename = v.filename
			reply.NReduce = m.nReduce
			v.taskStatus = "In Progress"
			v.timeBegun = time.Now()
			v.workerID = request.WorkerID
			log.Println("Assigning new map task %d to worker %d", k, request.WorkerID)
		}
		if v.taskStatus == "In Progress" {
			if time.Now().Sub(v.timeBegun) > m.threshold {
				oldWorker := v.workerID
				reply.TaskType = "MAP"
				reply.TaskNumber = k
				reply.Filename = v.filename
				reply.NReduce = m.nReduce
				v.timeBegun = time.Now()
				v.workerID = request.WorkerID
				log.Println("Re-assigning map task %d to worker %d, old worker was %d", k, request.WorkerID, oldWorker)
			}
		} 
	}
	return nil
}


func (m *Master) AssignReduceTask(request *TaskRequest, reply *TaskResponse) error {
	for i := 0; i < m.nReduce; i++ {
		v := m.reduceTasks[i]
		if v.taskStatus == "Not Started" {
			reply.TaskType = "REDUCE"
			reply.TaskNumber = i
			reply.NMap = m.nMap
			v.taskStatus = "In Progress"
			v.timeBegun = time.Now()
			v.workerID = request.WorkerID
			log.Println("Assigning new reduce task %d to worker %d", i, request.WorkerID)
		}
		if v.taskStatus == "In Progress" {
			if time.Now().Sub(v.timeBegun) > m.threshold {
				oldWorker := v.workerID
				reply.TaskType = "REDUCE"
				reply.TaskNumber = i
				reply.NMap = m.nMap
				v.timeBegun = time.Now()
				v.workerID = request.WorkerID
				log.Println("Re-assigning reduce task %d to worker %d, old worker was %d", i, request.WorkerID, oldWorker)
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

		nMap: len(files),
		nReduce: nReduce,
		threshold: time.Second * 10,

		mapTasksCompleted: false,
		reduceTasksCompleted: false,
	}

	for k, v := range files {
		mapTask := &MapTask{
			taskStatus: "Not Started",
			filename: v,
			// taskNumber: k
		}
		m.mapTasks[k] = mapTask
	}

	i := 0
	for i < nReduce {
		reduceTask := &ReduceTask{
			taskStatus: "Not Started",
			// taskNumber: i
		}
		m.reduceTasks[i] = reduceTask
		i = i + 1
	}

	// Your code here.
	// m.files = files
	// m.nReduce = nReduce
	// m.mapTasksDone = 0
	// m.reduceTasksDone = 0

	m.server()
	return &m
}
