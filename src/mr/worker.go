package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "sync"
import "time"
import "os"
import "io/ioutil"
//import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerState struct {
	l sync.Mutex
	vaild bool
	wId int64
	jobType int 
	file string 
	reduceId int
	lastHeartBeatT int64
	lease int64

	nReduce int
	immeFile []int64
}


const (
	WNormal		= 1
	WEmptyJob	= 0
	WCallFail	= -1
	WExit		= -2
)

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
		var state WorkerState
		var heartBeatChan chan int 
		//var finishChan chan int 
		go mkHearBeat(&state, heartBeatChan)
		switch state.getJob(heartBeatChan){
			case WEmptyJob :
				continue 
			case WCallFail : 
				continue 
			case WExit : 
				break
			case WNormal :
				state.runJob(mapf, reducef)
				mkCompleteSig(&state)

		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := CorReply{}

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
}*/



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

	fmt.Println(err)
	return false
}


func (w *WorkerState) getJob(c chan int) int {
	args := JobRequest{}
	reply := JobReply{}
	ok := call("Coordinator.GetJob", &args, &reply)
	if ok {
		switch {
			case reply.vaild == false :
				return WEmptyJob
			case reply.exit :
				c <- 0
				return WExit
			default : 
				w.vaild = reply.vaild
				w.wId = reply.wId
				w.jobType = reply.jobType
				w.file = reply.file
				w.reduceId = reply.reduceId
				w.lastHeartBeatT = reply.startT 
				w.lease = reply.lease
				w.nReduce = reply.nReduce
				w.immeFile = reply.immeFile
				c <- 1
				return WNormal
		}
	} else {
		return WCallFail
	}
}

func mkHearBeat(w *WorkerState, c chan int) {
	

	var args HeartBeat
	var reply HeartBeatReply
	var a int 
	for {

		w.l.Lock()
		for (!w.vaild) {
			w.l.Unlock()
			a = <- c 
			if(a == 0) {
				return
			}
			w.l.Lock()
		}	
		args = HeartBeat{wid : w.wId, sendTime: time.Now().Unix()}
		ok := call("Coordinator.GetJob", &args, &reply)
		switch {
			case !ok :
				w.vaild = false
			case reply.state == Killed || reply.state == Exit :
				w.vaild = false
			default :
				w.lastHeartBeatT = reply.lastHeartBeatT
				w.lease = reply.lease
				w.l.Unlock()
				time.Sleep(time.Duration((w.lease-(time.Now().Unix()-w.lastHeartBeatT))/2) * time.Second)
		}
		
	}
}

func mkCompleteSig(w *WorkerState) {
	w.l.Lock()
	defer w.l.Unlock()
	if(!w.vaild){
		return
	}
	args := JobCompleteSig{wId : w.wId, 
							jobType : w.jobType,
							mapFile : w.file,
							reduceId : w.reduceId,
						}
	w.vaild = false
	var reply int 
	call("Coordinator.FinishJob", &args, &reply)

}

func (w *WorkerState) runJob(mapf func(string, string) []KeyValue,
			reducef func(string, []string) string){
	w.l.Lock()
	switch w.jobType {
		case JMap :
			//abababa
		case JReduce :
			//abababa
	}

}

func (w *WorkerState) runMapJob(mapf func(string, string) []KeyValue,) {
	filename := w.file
	wid := w.wId
	nReduce := w.nReduce
	//intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		w.vaild = false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		w.vaild = false
	}
	file.Close()
	w.l.Unlock()

	kva := mapf(filename, string(content))
	//intermediate = append(intermediate, kva...)

	var fLt []*os.File 
	for i:=0;i<nReduce;i++ {
		n := fmt.Sprintf("WID%v-%v",wid,i)
		f,_ := os.Create(n)
		fLt = append(fLt,f)
	}

	for _,v := range kva {
		fmt.Fprintf(fLt[ihash(v.Key)], "%s %s\n", v.Key, v.Value)
	}

}

func (w *WorkerState) runReduceJob(reducef func(string, []string) string) {
	immeFile := w.immeFile
	wid := w.wId
	nReduce := w.nReduce
	reduceId := w.reduceId 
}