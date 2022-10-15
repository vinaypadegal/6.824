package mr

import "fmt"
import "log"
import "os"
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
	// oname := "mr-out-0"
	// ofile, _ := os.Create(oname)

	// This needs to be optimised, figure out how to create array of kv objects

	// i := 0
	// for i < nReduce {
	// 	oname := fmt.Sprintf("mr-%v-%v", mapper, i)
	// 	ofile, _ := os.Create(oname)
	// 	enc := json.NewEncoder(ofile)
	// 	for _, kv := kva {
	// 		reducer_number = ihash(kv.Key)
	// 		if reducer_number == i {
	// 			err := enc.Encode(&kv)
	// 		}
	// 	}
	// }

	i := 0
	var ofiles [nReduce]*File  // is this right?
	for i < nReduce {
		oname := fmt.Sprintf("mr-%v-%v", mapper, i)
		ofile, _ := os.Create(oname)
		ofiles = append(ofiles, ofile)	
	}

	for _, kv := kva {
		reducer_number = ihash(kv.Key)
		enc := json.NewEncoder(ofiles[reducer_number])
		err := enc.Encode(&kv)
	}
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	askForReduce := false
	
	for true {
		if (askForReduce == false) {
			ok, reply := RequestMapTask()
			if ok == false {
				break
			}
			if reply.mapsDone == true {
				askForReduce = true
				continue
			}
			kva := runMap(reply.filename)
			writeMapOutput(kva)
		} else {
			// request and run reduce tasks
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

func RequestTask() TaskReply{
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
