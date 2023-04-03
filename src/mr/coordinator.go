package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "time"
import "sync"
import "fmt"


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
	c.l.Lock()
	defer c.l.Unlock()
	if(c.jobType == JDone) {
		ret = true
	}
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
	for n,_ := range c.mapInputFiles {
		c.mapInputFiles[n] = &(Job{complete : false,vaild : false})
	}
	c.reduceInputFiles = make([]Job,nReduce)
	c.workers.lt = make(map[int64]*WorkerStat,0)
	c.nReduce = nReduce
	c.workers.counter = 0
	c.mapOutputId = make(map[string]int64)
	c.reduceOutputId = make(map[int]int64)
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






const workerLifeTimeOnServer int64 = 2
const workerLifeTimeOnWorker int64 = 1
const jobLifeTimeOnServer int64 = 10

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
	immeFile []int64

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
	counter int64

	nReduce int
	mapInputFiles map[string]*Job

	mapOutputId map[string]int64
	reduceInputFiles []Job
	reduceOutputId map[int]int64
	workers WorkerLst
	jobType int
	l sync.Mutex

}

func (c *Coordinator) GetJob(args *JobRequest, reply *JobReply) error {
	
	reply.Vaild = true
	reply.Exit = false
	c.l.Lock()
	jobType := c.jobType
	reply.NReduce = c.nReduce
	c.counter++
	c.l.Unlock()
	var job *Job
	switch  jobType {
		case JMap:
			job = getFreeMapJ(&c.mapInputFiles)
		case JReduce:
			job = getFreeReduceJ(c, &c.reduceInputFiles)
		case JDone:
			reply.Vaild = false
			reply.Exit = true
	}
	switch {
		case reply.Vaild && job != nil :
			job.vaild = true
			w :=  getFreeWID(c)
			initWorker(w)

			job.w = w 
			reply.WId = w.wId
			reply.Vaild = true 
			reply.JobType = job.jobType 
			reply.File = job.file
			reply.ReduceId = job.reduceId
			reply.StartT = w.startT
			reply.Lease = workerLifeTimeOnWorker

			reply.ImmeFile = job.immeFile

			w.l.Unlock()
			job.l.Unlock()

		case !reply.Vaild : 
		case job == nil : 
			forwardStat(c, jobType)
			reply.Vaild = false
		default :
	}
		if(reply.Vaild) {
			fmt.Println("Deliver job:", *reply)
		}
		return nil

}

func (c *Coordinator) MkHeartBeat(args *HeartBeat, reply *HeartBeatReply) error {

	c.l.Lock()
	w := c.workers.lt[args.Wid] 
	if w == nil {
		reply.State = Killed
		c.l.Unlock()
		return nil
	}
	c.l.Unlock()
	w.l.Lock()
	switch {
		case !w.vaild :
			reply.State = Killed 
		default :
			reply.Lease = workerLifeTimeOnWorker
			reply.LastHeartBeatT = time.Now().Unix()
			if (w.lastHeartBeat + workerLifeTimeOnServer<time.Now().Unix()) {
				reply.State = Killed
			} else {
				reply.State = Running
			}
	}

	return nil
}

func (c *Coordinator) FinishJob(args *JobCompleteSig, reply *int) error {
	var j *Job
	c.l.Lock()
	switch args.JobType {
		case JMap :
			j = c.mapInputFiles[args.MapFile]
			c.mapOutputId[args.MapFile] = args.WId
		case JReduce:
			j = &c.reduceInputFiles[args.ReduceId]
			c.reduceOutputId[args.ReduceId] = args.WId
		}
	c.l.Unlock()

	j.l.Lock()
	j.complete = true
	j.fileId = args.WId
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
	ws := &c.workers
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
	if(time.Now().Unix()-w.lastHeartBeat>workerLifeTimeOnServer || time.Now().Unix()-w.startT>jobLifeTimeOnServer) { 
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

func  getFreeReduceJ(c *Coordinator, m *[]Job) *Job {
	immeF := []int64{}
	defer c.l.Unlock()
	c.l.Lock()
	for _,v := range c.mapOutputId {
		immeF = append(immeF,v)
	} 

	for i,_ := range *m {
		j := &((*m)[i])
		j.l.Lock()
		j.immeFile = immeF
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
	state := JDone 

	for i,_ := range c.reduceInputFiles {
		j := &c.reduceInputFiles[i]
		j.l.Lock()
		if(!j.complete){
			state = JReduce
		}
		j.l.Unlock()
	}  

	for _,v := range c.mapInputFiles{
		v.l.Lock()
		if(!v.complete) {
			state = JMap
		}

		v.l.Unlock()
	}

	c.jobType = state

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
		c.l.Unlock()
		time.Sleep(time.Duration(workerLifeTimeOnServer*2) * time.Second)

	}
}