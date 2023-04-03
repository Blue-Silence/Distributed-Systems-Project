package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "sync"
import "time"
import "os"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	
		
		var state WorkerState
		heartBeatChan := make(chan int) 
		//var finishChan chan int 
		go mkHearBeat(&state, heartBeatChan)

	for {
		//fmt.Println("Hello from work")
		switch state.getJob(heartBeatChan){
			case WEmptyJob :
				//fmt.Println("T 1")
				continue 
			case WCallFail : 
				//fmt.Println("T 2")
				continue 
			case WExit : 
				//fmt.Println("T 3")
				return
			case WNormal :
				//fmt.Println("moving!")
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

	//fmt.Println(err)
	return false
}


func (w *WorkerState) getJob(c chan int) int {
	args := JobRequest{}
	reply := JobReply{}
	////fmt.Println("What happen?")
	ok := call("Coordinator.GetJob", &args, &reply)
	//fmt.Println("Hello from worker",reply)
	////fmt.Println("Is it exit?:",reply.Exit)
	if ok {
		switch {
			case reply.Vaild == false && reply.Exit == false:
				//fmt.Println("Worker pending")
				return WEmptyJob
			case reply.Exit :
				w.l.Lock()
				//fmt.Println("EXITING!!!")
				w.vaild = false 
				w.l.Unlock()
				//c <- 0
				//fmt.Println("Ready to exit!")
				return WExit
			default : 
				w.l.Lock()
				w.vaild = reply.Vaild
				w.wId = reply.WId
				w.jobType = reply.JobType
				w.file = reply.File
				w.reduceId = reply.ReduceId
				w.lastHeartBeatT = reply.StartT 
				w.lease = reply.Lease
				w.nReduce = reply.NReduce
				w.immeFile = reply.ImmeFile
				////fmt.Println("Hello from worker 111")
				w.l.Unlock()
				c <- 1
				////fmt.Println("Hello from worker")
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

		////fmt.Println("Beating!")
		w.l.Lock()
		for (!w.vaild) {
			w.l.Unlock()
			////fmt.Println("Stuck!")
			a = <- c 
			////fmt.Println("GO!")
			if(a == 0) {
				return
			}
			w.l.Lock()
		}	
		args = HeartBeat{Wid : w.wId, SendTime: time.Now().Unix()}
		ok := call("Coordinator.GetJob", &args, &reply)
		switch {
			case !ok :
				w.vaild = false
			case reply.State == Killed || reply.State == Exit :
				w.vaild = false
			default :
				w.lastHeartBeatT = reply.LastHeartBeatT
				w.lease = reply.Lease
				time.Sleep(time.Duration((w.lease-(time.Now().Unix()-w.lastHeartBeatT))/2) * time.Second)
		}
		w.l.Unlock()
		
	}
}

func mkCompleteSig(w *WorkerState) {
	w.l.Lock()
	defer w.l.Unlock()
	if(!w.vaild){
		return
	}
	args := JobCompleteSig{WId : w.wId, 
							JobType : w.jobType,
							MapFile : w.file,
							ReduceId : w.reduceId,
						}
	//fmt.Println("Complete sign: ", w)
	w.vaild = false
	var reply int 
	call("Coordinator.FinishJob", &args, &reply)

}

func (w *WorkerState) runJob(mapf func(string, string) []KeyValue,
			reducef func(string, []string) string){
	w.l.Lock()
	switch w.jobType {
		case JMap :
			w.runMapJob(mapf)
		case JReduce :
			//fmt.Println("Working on reduce")
			//w.l.Unlock()
			w.runReduceJob(reducef)
	}

}

func (w *WorkerState) runMapJob(mapf func(string, string) []KeyValue,) {
	filename := w.file
	wid := w.wId
	nReduce := w.nReduce
	
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
		//n := fmt.Sprintf("/home/usera/6.5840/src/main/tmp/IN-WID%v-%v",wid,i)
		n := fmt.Sprintf("IN-WID%v-%v",wid,i)
		f,_ := os.Create(n)
		fLt = append(fLt,f)
	}

	for _,v := range kva {
		fmt.Fprintf(fLt[ihash(v.Key)%nReduce], "%v %v\n", v.Key, v.Value)
	}

}

func (w *WorkerState) runReduceJob(reducef func(string, []string) string) {
	immeFile := w.immeFile
	//wid := w.wId
	//nReduce := w.nReduce
	reduceId := w.reduceId 
	//fmt.Println("Reduce running! ID: ",reduceId)
	w.l.Unlock()
	intermediate := []KeyValue{}
	for _,fileID := range immeFile {
		//file,err := os.Open(fmt.Sprintf("/home/usera/6.5840/src/main/tmp/IN-WID%v-%v",fileID,reduceId))
		file,err := os.Open(fmt.Sprintf("IN-WID%v-%v",fileID,reduceId))
		if(err != nil) {
			fmt.Println("ERR in open file: ", err)
		}
		for {
			t := KeyValue{}
			n,err := fmt.Fscanf(file, "%v %v\n", &t.Key, &t.Value)
			if	(n<2 || err != nil) {
				//fmt.Println("ERR: ", err,)
				break
			} else {
				intermediate = append(intermediate, t)
			}
		}
		file.Close()

	}

	////fmt.Println(intermediate)
	
	sort.Sort(ByKey(intermediate))

	//oname := fmt.Sprintf("mr-tmp/WID-%v-mr-out-%v", wid, reduceId)
	//ofile, _ := os.Create(oname)
	ofile,_ := os.CreateTemp("","*")//"/home/usera/6.5840/src/main/mr-out", "")

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(),fmt.Sprintf("./mr-out-%v", reduceId))
	ofile.Close()

}




