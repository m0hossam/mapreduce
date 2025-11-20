package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"slices"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Try calling RequestTask periodically until you get a task or exit
	taskArgs := TaskArgs{}
	taskArgs.WorkerID = os.Getpid()
	taskReply := TaskReply{}
	intermediateFiles := make([]string, 0)
	taskDone := false
	for !taskDone {
		if !call("Coordinator.RequestTask", &taskArgs, &taskReply) {
			log.Fatal("Encountered error while requesting task from coordinator")
		}

		switch taskReply.TaskType {
		case mapTask:
			intermediateFiles = executeMap(mapf, &taskReply)
			taskDone = true
		case reduceTask:
			executeReduce(reducef, &taskReply)
			taskDone = true
		case waitTask:
			time.Sleep(500 * time.Second)
		case exitTask:
			return
		default:
			log.Fatal("Received unknown task type from coordinator")
		}
	}

	// Call TaskDone
	doneArgs := DoneArgs{}
	doneArgs.TaskID = taskReply.TaskID
	doneArgs.TaskType = taskReply.TaskType
	doneArgs.IntermediateFileNames = intermediateFiles
	doneReply := DoneReply{}
	call("Coordinator.TaskDone", &doneArgs, &doneReply) // exit without checking if this RPC called succeeded
}

// executes the map task and returns intermediate file names
func executeMap(mapf func(string, string) []KeyValue, taskReply *TaskReply) []string {
	// Open and read the input file:
	file, err := os.Open(taskReply.MapFileName)
	if err != nil {
		log.Fatalf("cannot open %v", taskReply.MapFileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskReply.MapFileName)
	}
	file.Close()

	// Map input file contents into KV pairs:
	kva := mapf(taskReply.MapFileName, string(content))

	// Partition KV pairs into nReduce arrays for nReduce reduce-tasks:
	buckets := make([][]KeyValue, taskReply.NReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % taskReply.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	// Keep track of intermediate file names to be sent to the Coordinator:
	intermediateFileNames := make([]string, 0)

	// Save each partition of KV pairs to an intermediate file on disk:
	for i := 0; i < taskReply.NReduce; i++ {
		// Create a temporary file to avoid dirty reads of partially written files:
		tempFile, err := os.CreateTemp("", "mr-tmp-*") // "*" will be replaced by a randomly generated string
		if err != nil {
			log.Fatalf("could not create temp file: %v", err)
		}

		// Encode the KV pairs into JSON:
		enc := json.NewEncoder(tempFile)
		for _, kv := range buckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("could not encode json: %v", err)
			}
		}
		tempFile.Close()

		// Rename the temp file to its intended name (os.Rename() is atomic on Unix in the same filesystem):
		finalName := fmt.Sprintf("mr-%d-%d", taskReply.TaskID, i)
		if err := os.Rename(tempFile.Name(), finalName); err != nil {
			log.Fatalf("could not rename file: %v", err)
		}

		intermediateFileNames = append(intermediateFileNames, finalName)
	}
	return intermediateFileNames
}

// executes the reduce task
func executeReduce(reducef func(string, []string) string, taskReply *TaskReply) {
	kva := make([]KeyValue, 0)
	// Open and read all input files:
	for _, filename := range taskReply.ReduceFileNames {
		// Open file:
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// Decode the KV pairs in the file from JSON and append them to the KV array:
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("could not decode json: %v", err)
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// Sorts all KV pairs by key (so that all occurrences of the same key are grouped together):
	slices.SortFunc(kva, func(a, b KeyValue) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})

	// Open temp output file:
	tempFile, err := os.CreateTemp("", "mr-tmp-*") // "*" will be replaced by a randomly generated string
	if err != nil {
		log.Fatalf("could not create temp file: %v", err)
	}

	// Write keys and their reduced values to the temp output file:
	i := 0
	for i < len(kva) {
		// Group all values associated with kva[i].Key:
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		// Reduce the values into one value:
		finalv := reducef(kva[i].Key, values)

		// Output the key ands it final value to the temp output file:
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, finalv)

		// Advance i to the next key:
		i = j
	}

	// Atomically rename the temp output file:
	finalName := fmt.Sprintf("mr-out-%d", taskReply.TaskID)
	if err := os.Rename(tempFile.Name(), finalName); err != nil {
		log.Fatalf("could not rename file: %v", err)
	}
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
