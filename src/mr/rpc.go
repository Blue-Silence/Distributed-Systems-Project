package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
//import "time"

//
// example to show how to declare the arguments
// and reply for an RPC.
//




type JobRequest struct {
}

type JobReply struct {
	vaild bool
	exit bool
	wId int64
	jobType int 
	file string 
	reduceId int
	startT int64
	lease int64
	nReduce int

	immeFile []int64
}

type HeartBeat struct {
	wid int64
	sendTime int64
}

type HeartBeatReply struct {
	state int
	lastHeartBeatT int64
	lease int64
}

type JobCompleteSig struct {
	wId int64
	jobType int 
	mapFile string 
	reduceId int

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
