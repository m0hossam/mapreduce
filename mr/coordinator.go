package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Task timeout:
	taskTimeout = 10 * time.Second
	// Task type:
	mapTask = iota
	reduceTask
	waitTask
	exitTask
	// Task state:
	idleTask
	runningTask
	completedTask
)

type Coordinator struct {
	mu                    sync.Mutex
	done                  bool
	nMap                  int
	nReduce               int
	nMapDone              int
	nReduceDone           int
	intermediateFileNames map[string]bool
	tasks                 map[string]*Task // task key = "map-id" or "reduce-id"
}

type Task struct {
	id              int
	tType           int
	state           int
	mapFileName     string
	reduceFileNames []string
	workerID        int
	startTime       time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.TaskType = waitTask // default task if no idle task is found and we haven't finished yet
	if c.done {
		reply.TaskType = exitTask
		return nil
	}

	for _, t := range c.tasks {
		if t.state == idleTask {
			reply.TaskID = t.id
			reply.TaskType = t.tType
			reply.NReduce = c.nReduce
			if t.tType == mapTask {
				reply.MapFileName = t.mapFileName
			}
			if t.tType == reduceTask {
				reply.ReduceFileNames = t.reduceFileNames
			}
			t.state = runningTask
			t.workerID = args.WorkerID
			t.startTime = time.Now()
			break
		}
	}

	return nil
}

func (c *Coordinator) TaskDone(args *DoneArgs, reply *DoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == mapTask {
		t, ok := c.tasks["map-"+strconv.Itoa(args.TaskID)]
		if !ok {
			return errors.New("task not found")
		}
		t.state = completedTask
		c.nMapDone++
		for _, f := range args.IntermediateFileNames {
			c.intermediateFileNames[f] = true
		}
		if c.nMapDone == c.nMap {
			c.startReduce()
		}
	}
	if args.TaskType == reduceTask {
		t, ok := c.tasks["reduce-"+strconv.Itoa(args.TaskID)]
		if !ok {
			return errors.New("task not found")
		}
		t.state = completedTask
		c.nReduceDone++
	}
	if c.nMapDone == c.nMap && c.nReduceDone == c.nReduce {
		c.done = true
	}

	reply.Exit = true
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

// check if any running task has exceeded the max timeout
func (c *Coordinator) checkTimeout() {
	for {
		time.Sleep(500 * time.Millisecond)
		c.mu.Lock()
		for _, t := range c.tasks {
			if t.state == runningTask {
				if time.Since(t.startTime) > taskTimeout {
					t.state = idleTask
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) startReduce() {
	afilenames := make([][]string, c.nReduce)
	for filename := range c.intermediateFileNames {
		parts := strings.Split(filename, "-") // mr-X-Y
		idstr := parts[2]
		id, _ := strconv.Atoi(idstr)
		afilenames[id] = append(afilenames[id], filename)
	}
	for i := 0; i < c.nReduce; i++ {
		t := &Task{
			id:              i,
			tType:           reduceTask,
			state:           idleTask,
			reduceFileNames: afilenames[i],
		}
		c.tasks["reduce-"+strconv.Itoa(i)] = t
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.done = false
	c.nMap = len(files)
	c.nReduce = nReduce
	c.nMapDone = 0
	c.nReduceDone = 0
	c.intermediateFileNames = make(map[string]bool, nReduce)
	c.tasks = make(map[string]*Task, c.nMap+c.nReduce)

	for i := 0; i < len(files); i++ {
		tname := "map-" + strconv.Itoa(i)
		c.tasks[tname] = &Task{
			id:          i,
			tType:       mapTask,
			state:       idleTask,
			mapFileName: files[i],
		}
	}

	c.server()
	go c.checkTimeout()
	return &c
}
