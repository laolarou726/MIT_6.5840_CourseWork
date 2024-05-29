package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Stage uint8
type Status uint8

const (
	Map Stage = 1 << iota
	Reduce
	Stop
)

const (
	Pending Status = 1 << iota
	Running
	Finished
	Failed
)

type Coordinator struct {
	taskChannel chan Task
	taskStatus map[int]*Task
	jobStage Stage
	files []string
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

func (c * Coordinator) RequestTask(req RequestTaskMsg, res *Task) error {
	task := <-c.taskChannel
	task.StartTime = time.Now()
	task.Status = Running

	t, ok := c.taskStatus[task.Id]

	if !ok {
		log.Fatalf("[Coor] Key [%v] not found!", task.Id)
	}

	t.StartTime = time.Now()
	t.Status = Running
	*res = task

	log.Printf("Task %v <Type %v> with length: %v has been dequeued from channel.\n", task.Id, task.Stage, len(task.Files))

	return nil
}

func (c *Coordinator) ReportTaskStatus(req *Task, res *EmptyMsg) error {
	t, ok := c.taskStatus[req.Id]

	if !ok {
		log.Fatalf("[Coor] Key [%v] not found!", req.Id)
	}

	t.Status = req.Status

	if (req.Status != Finished) {
		log.Fatalf("[WORKER] Failed to execute Job-%v <Length: %v>[%v]!", req.Id, len(req.Files), req.Status)

		t.Status = Pending
		t.StartTime = time.Now()
		c.taskChannel <- *t

		return nil
	}

	log.Printf("[WORKER] Job-%v <Length: %v>[%v] done!", req.Id, len(req.Files), req.Status)

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

func (c *Coordinator) buildMapTasks() {
	log.Printf("[Coor] Start to build Map tasks with %v files and reduce num %v.", len(c.files), c.nReduce)

	for i, v := range c.files {
		task := Task{
			i,
			time.Now(),
			[]string{v},
			Pending,
			Map,
			c.nReduce,
		}

		c.taskStatus[i] = &task
		c.taskChannel <- task
	}
}

func (c *Coordinator) buildReduceTasks() {
	c.taskStatus = make(map[int]*Task)

	for i := 0; i < c.nReduce; i++ {
		task := Task{
			i,
			time.Now(),
			selectReduceFiles(i),
			Pending,
			Reduce,
			c.nReduce,
		}

		c.taskStatus[task.Id] = &task
		c.taskChannel <- task
	}
}

func selectReduceFiles(reduceNum int) []string {
	s := []string{}
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-tmp-") && strings.HasSuffix(f.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, f.Name())
		}
	}
	return s
}

func (c *Coordinator) checkTasks() {
	for {
		time.Sleep(time.Second * 5) // sleep 5 secs

		isAllFinished := true

		for _, v := range c.taskStatus {
			if v.Status == Finished {
				continue
			}

			isAllFinished = false

			if v.Status == Pending {
				continue
			}
	
			if time.Since(v.StartTime) > time.Second * 10 {
				v.Status = Pending
				v.StartTime = time.Now()
				c.taskChannel <- *v

				log.Printf("Task %v[%v]'s execution time has exceeded the limits(10s)! Reassign the job now.\n", v.Id, v.Files)
			}
		}

		if isAllFinished && c.jobStage == Map {
			log.Printf("Map stage has finished, now entering reduce stage.\n")

			c.jobStage = Reduce
			c.buildReduceTasks()
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if (c.jobStage != Reduce) {
		return false
	}

	result := true

	for _, v := range c.taskStatus {
		result = result && Stage(v.Status) == Stage(Finished)
	}

	if result {
		log.Println("[Coor] All job done. Shutting down all the workers...")

		for i := 0; i < c.nReduce; i++ {
			task := Task{
				i,
				time.Time{},
				[]string{},
				Pending,
				Stop,
				-1,
			}
	
			c.taskStatus[task.Id] = &task
			c.taskChannel <- task
		}

		time.Sleep(time.Second * 3)
	}

	return result
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		make(chan Task, len(files)),
		make(map[int]*Task, len(files)),
		Map,
		files,
		nReduce,
	}

	// Your code here.

	c.server()
	c.buildMapTasks()

	go c.checkTasks()

	return &c
}
