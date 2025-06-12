package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
// Map返回一个kv的数组
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
func myOpenFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// var workerID int = rand.Intn(90)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := MyRequest{
			Result: REQUEST,
		}
		reply := MyResponse{}
		ok := call("Coordinator.MapHandler", &args, &reply)
		if reply.Task == nil {
			continue
		}
		if ok {
			if reply.Task.TaskType == DONE {
				break
			}
			if reply.Task.TaskType == WAIT {
				// fmt.Printf("好的，我知道没有MAP了，睡觉！")
				time.Sleep(40000)
				continue
			} else if reply.Task.TaskType == MAP {
				taskid := reply.Task.TaskID - reply.Task.Nreduce
				// fmt.Printf("我是技师%d号,我拿到MAP任务%d,%v了\n", workerID, taskid, reply.Task.Files)
				for _, filename := range reply.Task.Files {
					content := myOpenFile(filename)
					kva := mapf(filename, string(content))
					for _, kv := range kva {
						savename := fmt.Sprintf("mr-tmp-%d-%d", taskid, ihash(kv.Key)%reply.Task.Nreduce)
						ofile, _ := os.OpenFile(savename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
						enc := json.NewEncoder(ofile)
						enc.Encode(&kv)
						ofile.Close()
					}
				}
				args2 := MyRequest{
					TaskID: reply.Task.TaskID,
					Result: TaskResult(MAPSUCCESS),
				}
				reply2 := MyResponse{}
				call("Coordinator.MapHandler", &args2, &reply2)
			} else if reply.Task.TaskType == REDUCE {
				savename := fmt.Sprintf("mr-out-%d", reply.Task.TaskID)
				ofile, _ := os.OpenFile(savename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
				kva := []KeyValue{}
				for _, filename := range reply.Task.Files {
					file, _ := os.Open(filename)
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
				sort.Sort(ByKey(kva))
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}
				// fmt.Printf("完成reduce任务，编号%d", reply.Task.TaskID)
				args2 := MyRequest{
					TaskID: reply.Task.TaskID,
					Result: TaskResult(REDUCESUCCESS),
				}
				reply2 := MyResponse{}
				call("Coordinator.MapHandler", &args2, &reply2)
			}
		}
	}
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
