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

type FileNames struct {
	File_arr []string
	Reduce   int
}

type ExampleReply struct {
	Y int
}
type TaskResponse struct {
	Task         string //Const either Map or Reduce
	File_Name    string //For Mapping step
	Reduce_tasks int    //For determining partition buckets
	Task_Done    bool
	Job_ID       int
	TimeStamp    int64
}

type CompleteResponse struct {
	Accepted bool
}

const Map = "Map"
const Reduce = "Reduce"
const Done = "Done"
const Wait = "Wait"

type TaskComplete struct {
	Done    bool
	Process string
	Job_ID  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
