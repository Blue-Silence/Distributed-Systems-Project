package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "time"
import "sync"



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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	c := Coordinator{}
	c.mapInputFiles = make(map[string]*Job)
	c.reduceInputFiles = make([]Job,nReduce)
	c.workers = make([]WorkerStat,0)
	for _, fname := range files {
		var a Job
		a.complete = false
		a.vaild = false
		a.jobType = JMap 
		a.file = fname
		c.mapInputFiles[fname] = &a
	}
	for i, _ := range c.reduceInputFiles {
		c.reduceInputFiles[i].complete = false
		c.reduceInputFiles[i].vaild = false
		c.reduceInputFiles[i].reduceId = i
		c.reduceInputFiles[i].jobType = JReduce
	}
	c.jobType = JMap 

	// Your code here.


	c.server()
	return &c
}




/////////////////////////////////////////////////////////////////////






const workerLifeTimeOnServer int64 = 12
const workerLifeTimeOnWorker int64 = 10

const (
	Running		= 0
	Killed		= 1
	Exit		= 2
)

const (
	JMap int	= 0
	JReduce		= JMap+1
	JDone		= JReduce+1
)


type WorkerStat struct {
	wId int 
	startT int64 //time.Time 
	lastHeartBeat int64 //time.Time
	state int
	l sync.Mutex
	vaild bool
}

type Job struct {
	jobType int

	file string
	reduceId int

	w *WorkerStat
	l sync.Mutex
	complete bool
	vaild bool
}

type Coordinator struct {
	// Your definitions here.
	mapInputFiles map[string]*Job
	//mapOuputFiles [][]string
	reduceInputFiles []Job
	workers []WorkerStat
	jobType int
	l sync.Mutex

}

func (c *Coordinator) GetJob(args *JobRequest, reply *JobReply) error {
	reply.vaild = true
	reply.exit = false
	c.l.Lock()
	jobType := c.jobType
	c.l.Unlock()
	var job *Job
	switch  jobType {
		case JMap:
			job = getFreeMapJ(&c.mapInputFiles)
		case JReduce:
			job = getFreeReduceJ(&c.reduceInputFiles)
		case JDone:
			reply.vaild = false
			reply.exit = true
	}

	switch {
		case reply.vaild && job != nil :
			w :=  getFreeWID(c)
			initWorker(w)
			job.w = w 

			reply.wId = w.wId
			reply.vaild = true 
			reply.jobType = job.jobType 
			reply.file = job.file
			reply.reduceId = job.reduceId
			reply.startT = w.startT
			reply.lease = workerLifeTimeOnWorker

			w.l.Unlock()
			job.l.Unlock()
		case !reply.vaild : 
		case job == nil : 
			forwardStat(c, jobType)
		default :
	}
		return nil

}

func (c *Coordinator) MkHeartBeat(args *HeartBeat, reply *HeartBeatReply) error {

	c.workers[args.wid].l.Lock()
	switch {
		case !c.workers[args.wid].vaild :
			reply.state = Killed 
		case c.workers[args.wid].startT >= args.sendTime :
			reply.state = Killed
		default :
			reply.lease = workerLifeTimeOnWorker
			reply.lastHeartBeatT = time.Now().Unix()
			if (c.workers[args.wid].lastHeartBeat + workerLifeTimeOnServer<time.Now().Unix()) {
				reply.state = Killed
			} else {
				reply.state = Running
			}
	}

	return nil
}

func (c *Coordinator) FinishJob(args *JobCompleteSig, reply *int) error {
	//reply.Y = args.X + 1
	var j *Job
	switch args.jobType {
		case JMap :
			j = c.mapInputFiles[args.mapFile]
		case JReduce:
			j = &c.reduceInputFiles[args.reduceId]
		}

	j.l.Lock()
	j.complete = true
	j.w.l.Lock()
	j.w.state = Killed 
	j.w.l.Unlock()
	j.l.Unlock()

	*reply = 0
	
	return nil
}


func  getFreeWID(c *Coordinator) *WorkerStat {
	defer c.l.Unlock()
	c.l.Lock()
	i := 0
	for _,_ = range c.workers {
		i++
		w := &c.workers[i]
		w.l.Lock()
		if(!w.vaild || timeExpire(w)){
			return w
		}
	}

	c.workers = append(c.workers, WorkerStat{})
	c.workers[i].l.Lock()
	c.workers[i].wId = i 
	return &c.workers[i]
}

func  initWorker(w *WorkerStat) {
		w.startT = time.Now().Unix()
		w.lastHeartBeat = time.Now().Unix()
		w.state = Running 
}


func  timeExpire(w *WorkerStat) bool {
	if(time.Now().Unix()-w.lastHeartBeat>workerLifeTimeOnServer) { 
			return true
	}	else {
			return false
	}
}

func  getFreeMapJ(m *map[string]*Job) *Job {
	for _,j := range *m {
		j.l.Lock()
		if(j.vaild){
			j.w.l.Lock()
			if(!j.complete && timeExpire(j.w)) {
				j.w.l.Unlock()
				return j
			} else {
				j.w.l.Unlock()
				j.l.Unlock()
			}
		} else {
			return j
		}	
	}

	return nil
}

func  getFreeReduceJ(m *[]Job) *Job {
	for _,j := range *m {
		j.l.Lock()
		if(j.vaild){
			j.w.l.Lock()
			if(!j.complete && timeExpire(j.w)) {
				j.w.l.Unlock()
				return &j
			} else {
				j.w.l.Unlock()
				j.l.Unlock()
			}
		} else {
			return &j
		}	
	}

	return nil
}

func  forwardStat(c *Coordinator, currentState int) {
	defer c.l.Unlock()
	c.l.Lock()
	if(c.jobType<currentState+1){
		c.jobType = currentState+1
	}

}