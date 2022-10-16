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


func writeMapOutput(kva []KeyValue, mapper int, nReduce int) {
	encoders := make(map[string]*json.Encoder)
	for k, v := range kva {
		reducer_number := ihash(v.Key) % nReduce
		oname := "mr-" + mapper + "-" + reducer_number
		if _, exists := encoders[oname]; !exists {
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			encoders[oname] = enc
		}
		encoders[oname].Encode(&kv)
	} 
	return nil
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
			log.Println("Worker %d: Assigned %s task %d", workerID, reply.taskType, reply.taskNumber)
			kva := runMap(reply.filename)
			writeMapOutput(kva, reply.taskNumber, reply.nReduce)
		}
		else if reply.taskType == "REDUCE" {
			
		}
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


func RequestMaster() TaskReply{
	request = TaskRequest{}
	reply = TaskReply{}

	ok = call("Master.RequestTask", &request, &reply)

	return ok, reply
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
