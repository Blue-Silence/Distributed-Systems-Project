package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	SetConfig = 0
	PutF      = 1
	GetF      = 2
	AppendF   = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	Key   string
	Value string

	CFG shardctrler.Config

	Id RpcId

	Server int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	callbackLt CallBackList
	KvS        KvStorage
	AppliedRPC map[int64]int64
	persister  *raft.Persister

	configs []shardctrler.Config

	mck *shardctrler.Clerk

	unique int64
}

type KvStorage struct {
	mu              sync.Mutex
	shardsAppointed []int
	shardsGot       []int
	appliedIndex    int
	s               map[int](map[string]string)
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

func isIn(lt []int, t int) bool {
	for _, v := range lt {
		if v == t {
			return true
		}
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.KvS.mu.Lock()
	if !(isIn(kv.KvS.shardsGot, key2shard(args.Key)) && isIn(kv.KvS.shardsAppointed, key2shard(args.Key))) {
		reply.Err = ErrWrongGroup
		kv.KvS.mu.Unlock()
		return
	}
	kv.KvS.mu.Unlock()

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(Op{GetF, args.Key, "", shardctrler.Config{}, args.Id, args.Server})
	kv.mu.Unlock()

	//if
	if !isLeader {
		reply.Err = ErrWrongLeader
		fmt.Println("555")
		return
	}
	fmt.Println("Starting:", args.Id, " on me:", kv.me, "  index:", index, "  unique:", kv.unique)
	var finished sync.Mutex
	finished.Lock()

	kv.callbackLt.reg(term,
		index,
		func(re string) {
			reply.Value = re
			reply.Err = OK
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.KvS.mu.Lock()
	if !(isIn(kv.KvS.shardsGot, key2shard(args.Key)) && isIn(kv.KvS.shardsAppointed, key2shard(args.Key))) {
		reply.Err = ErrWrongGroup
		fmt.Println("444")
		fmt.Println(kv.KvS.shardsGot, "  ", kv.KvS.shardsAppointed, "   ", key2shard(args.Key))
		kv.KvS.mu.Unlock()
		return
	}
	kv.KvS.mu.Unlock()
	fmt.Println("777")

	index := 0
	term := 0
	isLeader := false
	kv.mu.Lock()
	if args.Op == "Put" {
		index, term, isLeader = kv.rf.Start(Op{PutF, args.Key, args.Value, shardctrler.Config{}, args.Id, args.Server})
	} else {
		index, term, isLeader = kv.rf.Start(Op{AppendF, args.Key, args.Value, shardctrler.Config{}, args.Id, args.Server})
	}

	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		fmt.Println("888")
		return
	}

	fmt.Println("Starting:", args.Id, " on me:", kv.me, "  index:", index, "  unique:", kv.unique)
	fmt.Println("999")

	var finished sync.Mutex
	finished.Lock()

	kv.callbackLt.reg(term,
		index,
		func(re string) {
			reply.Err = OK
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// My initialization code here.
	kv.callbackLt.callbackLt = make(map[ActId]CallBackTuple)
	kv.KvS.s = make(map[int](map[string]string))
	kv.KvS.appliedIndex = -1
	kv.AppliedRPC = make(map[int64]int64)
	fmt.Println("What??? From:", kv.me)

	kv.persister = persister
	//kv.installSnapshot(persister.ReadSnapshot())

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	/*shardsAppointed := []int{}
	qS := kv.mck.Query(-1).Shards
	fmt.Println("Shards:", qS)
	for i, v := range qS {
		if v == gid {
			shardsAppointed = append(shardsAppointed, i)
		}
	}
	kv.KvS.shardsAppointed = shardsAppointed
	for _, v := range shardsAppointed {
		kv.KvS.s[v] = make(map[string]string)
	}*/

	kv.installSnapshot(persister.ReadSnapshot())

	kv.unique = nrand()

	go kv.applyF()
	go kv.clearReg()

	go kv.autoUpdateShards()

	return kv
}

func (kv *ShardKV) autoUpdateShards() {
	for {
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)

		configTail := kv.mck.Query(-1)
		for i := len(kv.configs); i <= configTail.Num; i++ {
			config := kv.mck.Query(i)
			kv.rf.Start(Op{SetConfig, "", "", config, RpcId{-1, 1024}, -1})
		}
		//if(config.Num)

		kv.KvS.mu.Lock()
		//term, _ := kv.rf.GetState()
		shardsAppointed := []int{}

		//fmt.Println("Shards:", qS)
		for i, v := range configTail.Shards {
			if v == kv.gid {
				shardsAppointed = append(shardsAppointed, i)
			}
		}

		//fmt.Println()

		kv.KvS.shardsAppointed = shardsAppointed

		//The following is evil.Should be corrected later.
		for _, v := range shardsAppointed {
			if !isIn(kv.KvS.shardsGot, v) {
				kv.KvS.s[v] = make(map[string]string)
			}
		}
		kv.KvS.shardsGot = make([]int, len(shardsAppointed))
		copy(kv.KvS.shardsGot, shardsAppointed)
		//Evil stops here.

		kv.KvS.mu.Unlock()
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	kv.KvS.mu.Lock()
	defer kv.KvS.mu.Unlock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var AppliedRPC map[int64]int64
	var appliedIndex int
	var shardsAppointed []int
	var shardsGot []int
	var s map[int](map[string]string)
	if d.Decode(&AppliedRPC) != nil ||
		d.Decode(&appliedIndex) != nil ||
		d.Decode(&shardsAppointed) != nil ||
		d.Decode(&shardsGot) != nil ||
		d.Decode(&s) != nil {
		return
	} else {
		if appliedIndex > kv.KvS.appliedIndex {
			kv.AppliedRPC = AppliedRPC
			kv.KvS.appliedIndex = appliedIndex
			kv.KvS.shardsAppointed = shardsAppointed
			kv.KvS.shardsGot = shardsGot
			kv.KvS.s = s
		}
	}
}

func (kv *ShardKV) testTrim() {
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
		e.Encode(kv.KvS.shardsAppointed)
		e.Encode(kv.KvS.shardsGot)
		e.Encode(kv.KvS.s)
		kv.rf.Snapshot(kv.KvS.appliedIndex, w.Bytes())
	}
}

func (kv *ShardKV) clearReg() {
	for {
		ms := 3000
		time.Sleep(time.Duration(ms) * time.Millisecond)
		term, _ := kv.rf.GetState()
		if true { //!isLeader {
			kv.callbackLt.clearF(term)
		}
	}
}

func (kv *ShardKV) applyF() {
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
		fmt.Println("Completing:", op.Id, " on me:", kv.me, "  index:", a.CommandIndex, " applied:", kv.AppliedRPC, "  unique:", kv.unique)
		kv.KvS.appliedIndex = a.CommandIndex
		if op.Id.RpcSeq > kv.AppliedRPC[op.Id.ClientId] {
			//if true {
			switch op.Type {
			case GetF:
				//re = kv.KvS.s[key2shard(op.Key)][op.Key]
				fmt.Println("111")
			case PutF:
				kv.KvS.s[key2shard(op.Key)][op.Key] = op.Value
				//re = op.Value
				fmt.Println("222")
			case AppendF:
				kv.KvS.s[key2shard(op.Key)][op.Key] = kv.KvS.s[key2shard(op.Key)][op.Key] + op.Value
				//re = kv.KvS.s[key2shard(op.Key)][op.Key]
				fmt.Println("333")
			case SetConfig:
				// Do SOMETHING TOMORROW. TO BE DONE

			}

			log.Println("From:", op.Id.ClientId, "  to:", kv.me)
			if op.Type != SetConfig {
				kv.AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
			}
		}

		re = kv.KvS.s[key2shard(op.Key)][op.Key]
		kv.KvS.mu.Unlock()
		kv.mu.Unlock()

		term, isLeader := kv.rf.GetState()
		succeed, _ := kv.callbackLt.popF(term, a.CommandIndex)
		if isLeader {
			succeed(re)
		} else {
			kv.callbackLt.clearF(term)
		}
		fmt.Println("AfterAfter:", kv.AppliedRPC, "  on me:", kv.me)
		kv.testTrim()
		fmt.Println("AfterAfterAfter:", kv.AppliedRPC, "  on me:", kv.me)
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
