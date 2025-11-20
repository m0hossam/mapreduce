### Run Instructions

- In the 1st terminal:
    - `go run cmd/coordinator/mrcoordinator.go data/pg*.txt`
- In the 2nd terminal:
    - `go build -buildmode=plugin cmd/apps/wc/wc.go`
    - `go run cmd/worker/mrworker.go wc.so`
- To run the tests:
    - `cd scripts`
    - `bash test-mr.sh`

### Assignment

Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the coordinator via RPC. Each worker process will, in a loop, ask the coordinator for a task, read the task's input from one or more files, execute the task, write the task's output to one or more files, and again ask the coordinator for a new task. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

### Rules

- The map phase should divide the intermediate keys into buckets for `nReduce` reduce tasks, where nReduce is the number of reduce tasks -- the argument that `main/mrcoordinator.go` passes to `MakeCoordinator()`. Each mapper should create `nReduce` intermediate files for consumption by the reduce tasks.
- The worker implementation should put the output of the X'th reduce task in the file `mr-out-X`.
- A `mr-out-X` file should contain one line per Reduce function output. The line should be generated with the Go `"%v %v"` format, called with the key and value. Have a look in `main/mrsequential.go` for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.
- You can modify `mr/worker.go`, `mr/coordinator.go`, and `mr/rpc.go`. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- `main/mrcoordinator.go` expects `mr/coordinator.go` to implement a `Done()` method that returns true when the MapReduce job is completely finished; at that point, `mrcoordinator.go` will exit.
- When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from `call()`: if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.
- A reasonable naming convention for intermediate files is `mr-X-Y`, where X is the Map task number, and Y is the reduce task number.
- If you pass reply structures that have non-default fields, the RPC system may silently return incorrect values.
- When calling the RPC `call()` function, the reply struct should be empty (i.e. has the default values only). You **shouldn't set** any fields of reply before the call. Otherwise, the RPC system may silently return incorrect values. RPC calls should look like this:
    ```go
    reply := SomeType{} // do not set any reply field
    call(method, &args, &reply)
    ```
