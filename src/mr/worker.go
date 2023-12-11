package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	args := ExampleArgs{}
	reply := TaskResponse{}

	for {
		request := call("Coordinator.RequestTask", &args, &reply)

		if request {
			switch task := reply.Task; task {
			case Map:
				CreateMapFile(&reply, mapf)
			case Done:
				os.Exit(2)
			case Wait:
				time.Sleep(1)
			}
		} else {
			print("error")
		}
	}
}

func CreateMapFile(reply *TaskResponse, mapf func(string, string) []KeyValue) {

	file, err := os.Open(reply.File_Name)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File_Name)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File_Name)
	}

	kva := mapf(reply.File_Name, string(content))

	args := TaskComplete{Done: true, Process: Map, Job_ID: reply.Job_ID}

	coordinator_call := CompleteResponse{}
	//If the worker takes longer than 10 seconds to execute for whatever reason
	//We want to terminate the process
	rand.Seed(time.Now().UnixNano())

	// Sleep for a random duration between 0 and 19 seconds
	sleepDuration := rand.Int63n(20)
	time.Sleep(time.Duration(sleepDuration) * time.Second)

	if reply.TimeStamp+10 < time.Now().Unix() {
		println("timeout", reply.Job_ID)
		return
	}

	_ = call("Coordinator.Complete", &args, &coordinator_call)

	if coordinator_call.Accepted {

		intermediate := make([][]KeyValue, reply.Reduce_tasks)

		for _, kv := range kva {
			row_index := ihash(kv.Key) % reply.Reduce_tasks
			intermediate[row_index] = append(intermediate[row_index], kv)
		}

		for r, kva := range intermediate {
			oname := fmt.Sprintf("mr-%d-%d", reply.Job_ID, r)
			ofile, _ := os.CreateTemp("", oname)
			enc := json.NewEncoder(ofile)
			for _, kv := range kva {
				enc.Encode(&kv)
			}
			ofile.Close()
			os.Rename(ofile.Name(), oname)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func GetFileNames() ([]string, int) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	// declare a reply structure.
	reply := FileNames{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	ok := call("Coordinator.FileNames", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y grest success")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.File_arr, reply.Reduce
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
