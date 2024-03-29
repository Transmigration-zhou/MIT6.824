package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// GetTask 获取任务
func GetTask() Task {
	reply := Task{}
	ok := call("Coordinator.AssignTask", &TaskArgs{}, &reply)
	if !ok {
		log.Fatalf("GetTask failed!")
	}
	return reply
}

// CallDone 任务完成回调
func CallDone(task *Task) {
	args := TaskArgs{}
	args.TaskType = task.TaskType
	switch task.TaskType {
	case MapTask:
		args.MapId = task.MapId
	case ReduceTask:
		args.ReduceId = task.ReduceId
	}
	ok := call("Coordinator.FinishTask", &args, &Task{})
	if !ok {
		log.Fatalf("CallDone failed!")
	}
}

func handleMap(mapf func(string, string) []KeyValue, response *Task) {
	filename := response.MapFilename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	var IntermediateFiles []*os.File
	for i := 0; i < response.NReduce; i++ {
		IntermediateFilename := fmt.Sprintf("mr-%d-%d", response.MapId, i)
		IntermediateFile, err := os.Create(IntermediateFilename)
		if err != nil {
			log.Fatalf("cannot create %v", IntermediateFilename)
		}
		IntermediateFiles = append(IntermediateFiles, IntermediateFile)
	}

	kva := mapf(filename, string(content))

	for _, v := range kva {
		idx := ihash(v.Key) % response.NReduce
		fmt.Fprintf(IntermediateFiles[idx], "%v %v\n", v.Key, v.Value)
	}
}

func handleReduce(reduceFunc func(string, []string) string, response *Task) {
	var intermediate []KeyValue
	for i := 0; i < response.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, response.ReduceId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		br := bufio.NewReader(file)
		for {
			line, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			item := strings.Split(string(line), " ")
			intermediate = append(intermediate, KeyValue{item[0], item[1]})
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oName := fmt.Sprintf("mr-out-%d", response.ReduceId)
	oFile, _ := os.Create(oName)
	defer oFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

// main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {
	for {
		task := GetTask()
		if !task.IsAvailable && task.TaskType != FinishedTask {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch task.TaskType {
		case MapTask:
			handleMap(mapFunc, &task)
			CallDone(&task)
		case ReduceTask:
			handleReduce(reduceFunc, &task)
			CallDone(&task)
		case FinishedTask:
			return
		}
		time.Sleep(100 * time.Millisecond)
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
