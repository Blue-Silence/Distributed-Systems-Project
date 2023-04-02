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
	wId int
	jobType int 
	file string 
	reduceId int
	startT int64
	lease int64
}

type HeartBeat struct {
	wid int
}

type HeartBeatReply struct {
	state int
}

type JobCompleteSig struct {
	wId int
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
