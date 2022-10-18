package mr

import "fmt"
import "log"
import "os"
import "encoding/json"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func runMap(filename string) []KeyValue {
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


func writeMapOutput(kva []KeyValue, mapTaskNumber int, nReduce int) {
	encoders := make(map[string]*json.Encoder)
	for k, v := range kva {
		reducer_number := ihash(v.Key) % nReduce
		oname := "mr-" + mapTaskNumber + "-" + reducer_number
		if _, exists := encoders[oname]; !exists {
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			encoders[oname] = enc
		}
		encoders[oname].Encode(&kv)
	} 
	return nil
}


func retrieveMapOutputs(reduceTaskNumber int, nMap int) (bool, []KeyValue) {
	intermediate := []KeyValue{}
	ok = true
	for i := 0; i < nMap; i++ {
		iname := "mr-" + i + "-" + reduceTaskNumber
		ifile := json.NewDecoder(ifile)

		var kv KeyValue
    	if err := dec.Decode(&kv); err != nil {
      		log.Println(err)
			ok = false
    	} else {
			intermediate = append(intermediate, kv)
		}
	}
	return ok, intermediate
}


func runReduce(intermediate []KeyValue, reduceTaskNumber int) {
	sort.Sort([]KeyValue(intermediate))

	oname := "mr-out-" + reduceTaskNumber
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
	workerID := os.Getuid()

	for true {
		ok, reply := RequestMaster()
		if ok == false {
			log.Println("Master has exited, worker %d exiting.", workerID)
			break
		}
		if reply.taskType == "MAP" {
			log.Println("Worker %d: Assigned MAP task %d", workerID, reply.taskNumber)
			kva := runMap(reply.filename)
			writeMapOutput(kva, reply.taskNumber, reply.nReduce)
			res := NotifyMaster("MAP", reply.taskNumber, workerID)
			log.Println("Worker %d: Assigned MAP task, success status: %d", workerID, res.success)
		} else if reply.taskType == "REDUCE" {
			log.Println("Worker: %d: Assigned REDUCE task %d", workerID, reply.taskNumber)
			ok, intermediate := retrieveMapOutputs(reply.taskNumber, reply.nMap)
			if ok {
				runReduce(intermediate)
				res := NotifyMaster("REDUCE", reply.taskNumber, workerID)
				log.Println("Worker %d: Assigned REDUCE task, success status: %d", workerID, res.success)
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


func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}


func RequestMaster() (bool, TaskReply) {
	request := TaskRequest{
		workedID: workerID,
	}
	reply := TaskReply{}

	ok = call("Master.RequestTask", &request, &reply)

	return ok, reply
}


func NotifyMaster(taskType, taskNumber, workerID) NotifyResponse {
	req := NotifyRequest{
		taskType: taskType,
		taskNumber: taskNumber,
		workerID: workerID,
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
