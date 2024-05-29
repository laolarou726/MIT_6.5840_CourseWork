package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// Add your RPC definitions here.

const NEW_JOB string = "new"
const STOP string = "stop"

type Task struct {
	Id int
	StartTime time.Time
	Files []string
	Status Status
	Stage Stage
	NReduce int
}

type EmptyMsg struct {}

type RequestTaskMsg struct {}

type StatusReportMsg struct {
	Id int
	Status Status
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
