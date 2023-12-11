package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files             []string
	Reduce_Tasks      int
	Reduce_Jobs_Queue []string
	Map_Queue         []Task
	Map_Tasks         map[int]string
	Job_ID            int
	mu                sync.Mutex
}

type Task struct {
	Filename string
	Job_id   int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Complete(args *TaskComplete, reply *CompleteResponse) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Done && args.Process == Map {
		_, value := c.Map_Tasks[args.Job_ID]
		//Here we check to see if a job has been completed that has already
		//Been ackonwledged as timeouted on the server side
		if value {
			println("Job ID:", args.Job_ID, " Finsihed:")
			delete(c.Map_Tasks, args.Job_ID)
			reply.Accepted = true
		} else {
			reply.Accepted = false
		}
		return nil
	} else if args.Done && args.Process == Map {
		//c.Reduce_Jobs_Done += 1
	}
	return nil
}

func (c *Coordinator) FileNames(args *ExampleArgs, reply *FileNames) error {
	reply.File_arr = c.Files
	reply.Reduce = c.Reduce_Tasks
	return nil
}

func (c *Coordinator) RequestTask(args *ExampleArgs, reply *TaskResponse) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Map_Queue) > 0 {
		//Instructing worker that they are assigned a worker task
		element := c.Map_Queue[0]

		go CheckTimeout(c, element.Job_id)

		reply.Task = Map
		//Access item at the start of the queue
		//Provide Filename to Worker
		reply.File_Name = element.Filename
		//Provide Unique JobID
		reply.Job_ID = element.Job_id
		println("Job ID:", element.Job_id, " Issued", "FileName: ", element.Filename)
		//Pop element of of queue
		c.Map_Queue = c.Map_Queue[1:]
		//Provide timestamp for worker so that it will know if it has timed out
		reply.Reduce_tasks = c.Reduce_Tasks
		current_time := time.Now().Unix()

		reply.TimeStamp = current_time
		c.Map_Tasks[element.Job_id] = element.Filename

	} else if len(c.Map_Tasks) > 0 {
		reply.Task = Wait
	} else {
		reply.Task = Done
	}

	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	return ret
}
func CheckTimeout(c *Coordinator, Job_ID int) {

	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	key, value := c.Map_Tasks[Job_ID]
	//We are checking to see if the value still exists within the task queue
	//If it does still exist we have to create a new task by adding an element
	//To the map queue and delete the old outgoing task
	if value {
		c.Map_Queue = append(c.Map_Queue, Task{key, c.Job_ID + 1})
		c.Job_ID += 1
		println("Job ID:", Job_ID, " Cancelled", key)
		delete(c.Map_Tasks, Job_ID)
	}

	return

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Files: files, Reduce_Tasks: nReduce}
	c.Map_Queue = make([]Task, 0)
	c.Map_Tasks = make(map[int]string, 0)

	for _, filename := range files {
		kva := Task{filename, c.Job_ID}
		c.Map_Queue = append(c.Map_Queue, kva)
		c.Job_ID += 1
	}
	// Your code here.

	c.server()
	return &c
}
