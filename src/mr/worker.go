package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Request a task from the coordinator
		args := RequestTask{}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			fmt.Println("call failed!")
			time.Sleep(time.Second)
			continue
		}

		// Process the task based on its type
		switch reply.Type {
		case "map":
			// Perform map task
			performMapTask(mapf, &reply)
		case "reduce":
			// Perform reduce task
			performReduceTask(reducef, &reply)
		case "wait":
			// No tasks available, wait a bit
			time.Sleep(time.Second)
		case "done":
			// Job is complete
			return
		}
	}
}

func performMapTask(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	// Read input file
	file, err := os.Open(reply.InputFile)
	if err != nil {
		fmt.Printf("cannot open %v", reply.InputFile)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read %v", reply.InputFile)
		return
	}

	// Apply map function
	kvs := mapf(reply.InputFile, string(content))

	// Create buckets for intermediate files
	buckets := make([][]KeyValue, reply.NReduce)

	// Separate key-value pairs into buckets using ihash
	for _, kv := range kvs {
		bucketIndex := ihash(kv.Key) % reply.NReduce
		buckets[bucketIndex] = append(buckets[bucketIndex], kv)
	}

	// Write intermediate files
	for reduceTaskId, bucket := range buckets {
		if len(bucket) > 0 {
			filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceTaskId)
			file, err := os.Create(filename)
			if err != nil {
				fmt.Printf("cannot create file: %v\n", err)
				continue
			}
			defer file.Close()

			enc := json.NewEncoder(file)
			for _, kv := range bucket {
				err = enc.Encode(&kv)
				if err != nil {
					fmt.Printf("error encoding key-value pair: %v\n", err)
				}
			}
		}
	}

	// Notify coordinator that map task is complete
	taskDoneArgs := CompleteTaskArgs{
		Type:   "map",
		TaskID: reply.TaskID,
	}
	taskDoneReply := CompleteTaskReply{}
	call("Coordinator.CompleteTask", &taskDoneArgs, &taskDoneReply)
}

func performReduceTask(reducef func(string, []string) string, reply *RequestTaskReply) {
	// Read intermediateKvs files for this reduce task
	intermediateKvs := make(map[string][]string)
	for mapTaskId := range reply.NMap {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskId, reply.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("cannot open %v", filename)
			continue
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateKvs[kv.Key] = append(intermediateKvs[kv.Key], kv.Value)
		}
	}

	// Create output file
	outputFilename := fmt.Sprintf("mr-out-%d", reply.TaskID)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		fmt.Printf("cannot create %v", outputFilename)
		return
	}
	defer outputFile.Close()

	// Apply reduce function to each key group
	for key, values := range intermediateKvs {
		// Apply reduce function and write output
		output := reducef(key, values)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}

	// Notify coordinator that reduce task is complete
	taskDoneArgs := CompleteTaskArgs{
		Type:   "reduce",
		TaskID: reply.TaskID,
	}
	taskDoneReply := CompleteTaskReply{}
	call("Coordinator.CompleteTask", &taskDoneArgs, &taskDoneReply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
