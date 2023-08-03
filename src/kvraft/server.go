package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PutF    = 1
	GetF    = 2
	AppendF = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	Key   string
	Value string

	Id RpcId

	Server int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	callbackLt CallBackList
	KvS        KvStorage
	AppliedRPC map[int64]int64
	persister  *raft.Persister
}

type KvStorage struct {
	mu           sync.Mutex
	appliedIndex int
	s            map[string]string
}

type CallBackList struct {
	mu         sync.Mutex
	callbackLt map[ActId]CallBackTuple
}

type ActId struct {
	term  int
	index int
}

type CallBackTuple struct {
	succeedFun func(string)
	failFun    func(string)
	valid      bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//reply.Err = Err("Timeout")
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(Op{GetF, args.Key, "", args.Id, args.Server})
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = Err(fmt.Sprint("Not leader"))
		return
	}

	var finished sync.Mutex
	finished.Lock()

	kv.callbackLt.reg(term,
		index,
		func(re string) {
			reply.Value = re
			finished.Unlock()
		},
		func(re string) {
			reply.Err = Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//reply.Err = Err("Timeout")

	index := 0
	term := 0
	isLeader := false
	kv.mu.Lock()
	if args.Op == "Put" {
		index, term, isLeader = kv.rf.Start(Op{PutF, args.Key, args.Value, args.Id, args.Server})
	} else {
		index, term, isLeader = kv.rf.Start(Op{AppendF, args.Key, args.Value, args.Id, args.Server})
	}

	kv.mu.Unlock()

	if !isLeader {
		reply.Err = Err(fmt.Sprint("Not leader:", args))
		return
	}

	var finished sync.Mutex
	finished.Lock()

	kv.callbackLt.reg(term,
		index,
		func(re string) {
			finished.Unlock()
		},
		func(re string) {
			reply.Err = Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.callbackLt.callbackLt = make(map[ActId]CallBackTuple)
	kv.KvS.s = make(map[string]string)
	kv.KvS.appliedIndex = -1
	kv.AppliedRPC = make(map[int64]int64)

	kv.persister = persister
	kv.installSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyF()
	go kv.clearReg()
	//go kv.autoSnapshot()

	return kv
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	kv.KvS.mu.Lock()
	defer kv.KvS.mu.Unlock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var AppliedRPC map[int64]int64
	var appliedIndex int
	var s map[string]string
	if d.Decode(&AppliedRPC) != nil ||
		d.Decode(&appliedIndex) != nil ||
		d.Decode(&s) != nil {
		return
	} else {
		if appliedIndex > kv.KvS.appliedIndex {
			kv.AppliedRPC = AppliedRPC
			kv.KvS.appliedIndex = appliedIndex
			kv.KvS.s = s
		}
	}
}

func (kv *KVServer) autoSnapshot() {
	for {
		ms := 300
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//term, _ := kv.rf.GetState()
		kv.testTrim()
	}
}

func (kv *KVServer) testTrim() {
	if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		kv.mu.Lock()
		kv.KvS.mu.Lock()
		defer kv.KvS.mu.Unlock()
		defer kv.mu.Unlock()

		if kv.KvS.appliedIndex < 0 {
			log.Panic("What???less then zero???")
		}
		e.Encode(kv.AppliedRPC)
		e.Encode(kv.KvS.appliedIndex)
		e.Encode(kv.KvS.s)
		kv.rf.Snapshot(kv.KvS.appliedIndex, w.Bytes())
	}
}

func (kv *KVServer) clearReg() {
	for {
		ms := 3000
		time.Sleep(time.Duration(ms) * time.Millisecond)
		term, _ := kv.rf.GetState()
		if true { //!isLeader {
			kv.callbackLt.clearF(term)
		}
	}
}

func (kv *KVServer) applyF() {
	for {
		a := <-kv.applyCh
		if a.SnapshotValid {
			kv.installSnapshot(a.Snapshot)
		}
		op, ok := a.Command.(Op)

		if !ok {
			//fmt.Println("Warning!")
			continue
		}

		re := ""
		kv.mu.Lock()
		kv.KvS.mu.Lock()
		if kv.KvS.appliedIndex+1 != a.CommandIndex && kv.KvS.appliedIndex != -1 {
			log.Panic(kv.KvS.appliedIndex, "+1 != ", a.CommandIndex, "  me:", kv.me)
		}
		kv.KvS.appliedIndex = a.CommandIndex
		if op.Id.RpcSeq > kv.AppliedRPC[op.Id.ClientId] {
			//if true {
			switch op.Type {
			case GetF:
				re = kv.KvS.s[op.Key]
			case PutF:
				kv.KvS.s[op.Key] = op.Value
				re = op.Value
			case AppendF:
				kv.KvS.s[op.Key] = kv.KvS.s[op.Key] + op.Value
				re = kv.KvS.s[op.Key]

			}

			if op.Id.RpcSeq != (kv.AppliedRPC[op.Id.ClientId] + 1) {
				debug.Stack()
				log.Fatal(op.Id.RpcSeq, " != ", kv.AppliedRPC[op.Id.ClientId], "+1")
			}
			kv.AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
		}

		re = kv.KvS.s[op.Key]
		kv.KvS.mu.Unlock()
		kv.mu.Unlock()

		term, isLeader := kv.rf.GetState()
		succeed, _ := kv.callbackLt.popF(term, a.CommandIndex)
		if isLeader {
			succeed(re)
		} else {
			kv.callbackLt.clearF(term)
		}

		kv.testTrim()
	}
}

func (lt *CallBackList) reg(term int, index int, succeed func(string), fail func(string)) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.callbackLt[ActId{term, index}] = CallBackTuple{succeed, fail, true}
}

func (lt *CallBackList) popF(term int, index int) (func(string), func(string)) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	re := lt.callbackLt[ActId{term, index}]
	delete(lt.callbackLt, ActId{term, index})
	if re.valid {
		return re.succeedFun, re.failFun
	} else {
		return func(x string) {}, func(x string) {}
	}
}

func (lt *CallBackList) clearF(term int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	for i, v := range lt.callbackLt {
		if i.term < term {
			v.failFun("")
			delete(lt.callbackLt, i)
		}
	}
}
