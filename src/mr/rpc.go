package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
//import "time"


type JobRequest struct {
}

type JobReply struct {
	Vaild bool
	Exit bool
	WId int64
	JobType int 
	File string 
	ReduceId int
	StartT int64
	Lease int64

	NReduce int
	ImmeFile []int64
}

type HeartBeat struct {
	Wid int64
	SendTime int64
}

type HeartBeatReply struct {
	State int
	LastHeartBeatT int64
	Lease int64
}

type JobCompleteSig struct {
	WId int64
	JobType int 
	MapFile string 
	ReduceId int

}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
