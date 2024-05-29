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
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		req := RequestTaskMsg{}
		res := Task{}

		ok := call("Coordinator.RequestTask", &req, &res)

		if ok {
			switch res.Stage {
			case Map:
				log.Printf("[WORKER] Started Job-%v [Map] for file %v", res.Id, res.Files[0])
				doMapProcedure(mapf, res.Id, res.NReduce, res.Files[0])
			case Reduce:
				log.Printf("[WORKER] Started Job-%v [Reduce] for files with length %v", res.Id, len(res.Files))
				doReduceProcedure(reducef, res.Id, res.Files)
			case Stop:
				log.Println("[WORKER] Worker stopped!")
				return
			default:
				log.Fatal("Unknown task status!")
				return
			}

			res.Status = Finished
			reportTaskStatus(&res)
		} else {
			log.Fatalf("Failed to request job from the coordinator!\n")
			break
		}
	}
}

func reportTaskStatus(task *Task) {
	res := EmptyMsg{}

	ok := call("Coordinator.ReportTaskStatus", &task, &res)

	if !ok {
		log.Fatalf("Failed to report task status!\n")
	}
}

func doMapProcedure(
	mapf func(string, string) []KeyValue,
	id int,
	nReduce int,
	fileName string) {
	
	file, err := os.Open(fileName)
	intermediate := []KeyValue{}

	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	file.Close()

	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	resultKv := make([][]KeyValue, nReduce)

	for _, v := range intermediate {
		index := ihash(v.Key) % nReduce
		resultKv[index] = append(resultKv[index], v)
	}

	for i := 0; i < nReduce; i++ {
		outFileName := "mr-tmp-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
		outFile, err := os.Create(outFileName)

		if err != nil {
			log.Fatal("[Worker] Failed to create tmp map file for ", outFileName, err)
		}

		encoder := json.NewEncoder(outFile)
		encodeErr := encoder.Encode(resultKv[i])

		if encodeErr != nil {
			log.Fatal("[Worker] Failed to encode JSON: ", err)
		}

		outFile.Close()
	}
}

func doReduceProcedure(
	reducef func(string, []string) string,
	id int,
	files []string) {
	intermediate := shuffle(files)
	finalName := fmt.Sprintf("mr-out-%d", id)
	ofile, err := os.Create(finalName)

	if err != nil {
		log.Fatal("[Worker] Failed to create file:", err)
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
}

func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, fi := range files {
		file, err := os.Open(fi)

		if err != nil {
			log.Fatalf("[Worker] Failed to open %v %v", fi, err)
		}

		tempKva := []KeyValue{}
		dec := json.NewDecoder(file)
		if err := dec.Decode(&tempKva); err != nil {
			log.Printf("[Worker] Failed to read temp results from JSON: %v\n", err)
			break
		}

		kva = append(kva, tempKva...)

		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Fatalf("Failed to perform RPC for name [%v], %v", rpcname, err)
	return false
}
