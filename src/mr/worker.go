package mr

import "fmt"
import "log"
import "os"
import "io"
import "time"
import "encoding/json"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


// run map function and return list of intermediate key-value pairs outputted by map task
func runMap(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return mapf(filename, string(content))
}


// write this map task's output to the appropriate files, as specified as the documentation
func writeMapOutput(kva []KeyValue, mapTaskNumber int, nReduce int) {
	encoders := make(map[string]*json.Encoder)

	// hash each key and determine the reducer by which this key should be reduced
	for _, v := range kva {
		reducer_number := ihash(v.Key) % nReduce
		oname := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reducer_number)

		// write this key-value pair to the appropriate file using json.Encoder
		if _, exists := encoders[oname]; !exists {
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			encoders[oname] = enc
		}
		encoders[oname].Encode(&v)
	} 
	// return nil
}


// reducer retrieves all the map output key-values that must be reduced by it
func retrieveMapOutputs(reduceTaskNumber int, nMap int) (bool, []KeyValue) {
	intermediate := []KeyValue{}
	ok := true
	for i := 0; i < nMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, reduceTaskNumber)
		ifile, err := os.Open(iname)

		if err != nil {
			log.Printf("Unable to open file %s\n", iname)
			continue
		}

		// uses json.Decode to decode the intermediate files meant for it
		dec := json.NewDecoder(ifile)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
    		if err != nil {
      			log.Println(err)
				ok = false
    		} else {
				// consolidate all intermediate key-value pairs from all mappers into one list
				intermediate = append(intermediate, kv)
			}
		}
		
	}
	return ok, intermediate
}


// run reduce function and write to appropriate output file
func runReduce(reducef func(string, []string) string, intermediate []KeyValue, reduceTaskNumber int) {
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceTaskNumber)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	workerID := os.Getpid()   // process id 

	for true {
		ok, reply := RequestMaster(workerID)
		// if unable to connect the master
		if ok == false {
			log.Printf("Connection error, worker %d exiting.\n", workerID)
			break
		}

		// if received task is a map task
		if reply.TaskType == "MAP" {
			log.Printf("Worker %d: Assigned MAP task %d\n", workerID, reply.TaskNumber)
			// run map and get intermediate key value pairs
			kva := runMap(mapf, reply.Filename)
			// write intermediate key value pairs to appropriate intermediate files
			writeMapOutput(kva, reply.TaskNumber, reply.NReduce)
			// notify master via RPC
			res := NotifyMaster("MAP", reply.TaskNumber, workerID)
			log.Printf("Worker %d: Assigned MAP task %d, success status: %t\n", workerID, reply.TaskNumber, res.Success)
		// if received task is reduce task
		} else if reply.TaskType == "REDUCE" {
			log.Printf("Worker %d: Assigned REDUCE task %d\n", workerID, reply.TaskNumber)
			// retrieve all intermediate key value pairs
			ok, intermediate := retrieveMapOutputs(reply.TaskNumber, reply.NMap)
			if ok {
				// run reduce and write to appropriate out file
				runReduce(reducef, intermediate, reply.TaskNumber)
				// notify master of task completion via rpc
				res := NotifyMaster("REDUCE", reply.TaskNumber, workerID)
				log.Printf("Worker %d: Assigned REDUCE task %d, success status: %t\n", workerID, reply.TaskNumber, res.Success)
			}
		}
		time.Sleep(time.Second * 5)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//


// func CallExample() {
// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }


func RequestMaster(workerID int) (bool, TaskResponse) {
	request := TaskRequest{
		WorkerID: workerID,
	}
	reply := TaskResponse{}

	ok := call("Master.RequestTask", &request, &reply)

	return ok, reply
}


func NotifyMaster(taskType string, taskNumber int, workerID int) NotifyResponse {
	req := NotifyRequest{
		TaskType: taskType,
		TaskNumber: taskNumber,
		WorkerID: workerID,
	}
	res := NotifyResponse{}

	call("Master.NotifyTaskCompletion", &req, &res)

	return res
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
