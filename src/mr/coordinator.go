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
	c.workers.lt = make(map[int64]*WorkerStat,0)
	c.workers.counter = 0
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
	go cleaning(&c)
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
	wId int64 
	startT int64 //time.Time 
	lastHeartBeat int64 //time.Time
	state int
	l sync.Mutex
	vaild bool
}

type Job struct {
	jobType int

	file string
	fileId int64

	reduceId int
	immeFile []string

	w *WorkerStat
	l sync.Mutex
	complete bool
	vaild bool
}

type WorkerLst struct {
	counter int64 
	lt map[int64]*WorkerStat
}

type Coordinator struct {
	// Your definitions here.
	mapInputFiles map[string]*Job
	//mapOuputFiles [][]string
	reduceInputFiles []Job
	workers WorkerLst
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
			job = getFreeReduceJ(&c.mapInputFiles, &c.reduceInputFiles)
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

	c.l.Lock()
	w := c.workers.lt[args.wid] //[args.wid].l.Lock()
	if w == nil {
		reply.state = Killed
		c.l.Unlock()
		return nil
	}
	c.l.Unlock()
	w.l.Lock()
	switch {
		case !w.vaild :
			reply.state = Killed 
		default :
			reply.lease = workerLifeTimeOnWorker
			reply.lastHeartBeatT = time.Now().Unix()
			if (w.lastHeartBeat + workerLifeTimeOnServer<time.Now().Unix()) {
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
	j.fileId = args.wId
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

	ws := c.workers
	w := WorkerStat{wId : ws.counter}
	w.l.Lock()
	ws.lt[ws.counter] = &w 
	ws.counter++

	return &w
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

func  getFreeReduceJ(mf *map[string]*Job , m *[]Job) *Job {
	immeF := []int64{}

	for _,j := range *mf {
		j.l.Lock()
		immeF = append(immeF, j.fileId)
		j.l.Unlock()
	}

	for i,_ := range *m {
		j := &((*m)[i])
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

func  forwardStat(c *Coordinator, currentState int) {
	defer c.l.Unlock()
	c.l.Lock()
	if(c.jobType<currentState+1){
		c.jobType = currentState+1
	}

}

func cleaning(c *Coordinator) {
	for {
		c.l.Lock()
		ws := &c.workers.lt  
		for id,w := range *ws {
			w.l.Lock()
			if (!w.vaild || timeExpire(w)) {
				delete(*ws,id)
			}
			w.l.Unlock()
		}
		time.Sleep(time.Duration(workerLifeTimeOnServer*2) * time.Second)

	}
}