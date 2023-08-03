package shardkv

import (
	"bytes"
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

	CfgGen int
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
	//AppliedRPC map[int64]int64
	persister *raft.Persister

	configs      []shardctrler.Config
	isInTransfer sync.Mutex

	mck *shardctrler.Clerk

	unique int64
}

type KvStorage struct {
	//mu              sync.Mutex
	ShardsAppointed []int
	AppliedIndex    int
	S               map[int](ShardState)
	OldS            [](map[int](ShardState))
	/*
			S               map[int](map[string]string)
		OldS            [](map[int](map[string]string))
	*/
	CurrentConfig int64
}

type ShardState struct {
	AppliedRPC map[int64]int64
	S          (map[string]string)
}

func newShardState() ShardState {
	var n ShardState
	n.AppliedRPC = make(map[int64]int64)
	n.S = make(map[string]string)
	return n
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
	////fmt.Println("Before Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 1")
	kv.mu.Lock()
	////fmt.Println("After Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 1")
	if !isIn(kv.KvS.ShardsAppointed, key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	cfgGen := len(kv.configs) - 1
	kv.mu.Unlock()

	////fmt.Println("Before Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 2")
	kv.mu.Lock()
	////fmt.Println("After Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 2")
	index, term, isLeader := kv.rf.Start(Op{GetF, args.Key, "", shardctrler.Config{}, args.Id, args.Server, cfgGen})
	kv.mu.Unlock()

	//if
	if !isLeader {
		reply.Err = ErrWrongLeader
		////fmt.Println("555")
		return
	}
	//fmt.Println("Starting:", args, " on me:", kv.me, "  index:", index, "  unique:", kv.unique)
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
			reply.Err = ErrWrongLeader //Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	////fmt.Println("Before Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 3")
	kv.mu.Lock()
	////fmt.Println("After Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 3")
	if !isIn(kv.KvS.ShardsAppointed, key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		////fmt.Println("444")
		////fmt.Println(kv.KvS.ShardsAppointed, "   ", key2shard(args.Key))
		kv.mu.Unlock()
		return
	}
	cfgGen := len(kv.configs) - 1
	kv.mu.Unlock()
	////fmt.Println("777")

	index := 0
	term := 0
	isLeader := false
	////fmt.Println("Before Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 4")
	kv.mu.Lock()
	////fmt.Println("After Lock :", args.Id, " on me:", kv.me, "  unique:", kv.unique, "  Lock 4")
	if args.Op == "Put" {
		index, term, isLeader = kv.rf.Start(Op{PutF, args.Key, args.Value, shardctrler.Config{}, args.Id, args.Server, cfgGen})
	} else {
		index, term, isLeader = kv.rf.Start(Op{AppendF, args.Key, args.Value, shardctrler.Config{}, args.Id, args.Server, cfgGen})
	}

	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		////fmt.Println("888")
		return
	}

	//fmt.Println("Starting:", args, " on me:", kv.me, "  index:", index, "  unique:", kv.unique)
	////fmt.Println("999")

	var finished sync.Mutex
	finished.Lock()

	kv.callbackLt.reg(term,
		index,
		func(re string) {
			reply.Err = OK
			finished.Unlock()
		},
		func(re string) {
			reply.Err = ErrWrongLeader //Err("Exec fail")
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
	kv.KvS.S = make(map[int]ShardState)
	kv.KvS.AppliedIndex = -1
	kv.KvS.CurrentConfig = -1

	kv.unique = nrand()
	////fmt.Println("Starting From:", kv.me, "  gid:", gid, "  unique:", kv.unique)

	kv.persister = persister
	//kv.installSnapshot(persister.ReadSnapshot())

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	/*shardsAppointed := []int{}
	qS := kv.mck.Query(-1).Shards
	////fmt.Println("Shards:", qS)
	for i, v := range qS {
		if v == gid {
			shardsAppointed = append(shardsAppointed, i)
		}
	}
	kv.KvS.ShardsAppointed = shardsAppointed
	for _, v := range shardsAppointed {
		kv.KvS.S[v] = make(map[string]string)
	}*/

	kv.installSnapshot(persister.ReadSnapshot())

	go kv.applyF()
	go kv.clearReg()

	go kv.autoUpdateShards()

	//go kv.autoBeep()

	return kv
}

func (kv *ShardKV) autoBeep() {
	for {

		ms := 500

		time.Sleep(time.Duration(ms) * time.Millisecond)

		kv.isInTransfer.Lock()
		////fmt.Println("Beep", "  on me:", kv.me, "  unique:", kv.unique)
		kv.isInTransfer.Unlock()
		//kv.mu.Unlock()
	}
}

func (kv *ShardKV) autoUpdateShards() {
	for {

		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//////fmt.Println("Step A", "  on me:", kv.me, "  unique:", kv.unique)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			//////fmt.Println("Step B", "  on me:", kv.me, "  unique:", kv.unique)
			continue
		}

		kv.mu.Lock()
		configTail := kv.mck.Query(-1)
		var config shardctrler.Config
		cfgGen := len(kv.configs) - 1
		if len(kv.configs) <= configTail.Num {
			config = kv.mck.Query(len(kv.configs))
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
			continue
		}
		////fmt.Println("Step S", "  on me:", kv.me, "  unique:", kv.unique)
		kv.rf.Start(Op{SetConfig, "", "", config, RpcId{-1, int64(config.Num)}, -1, cfgGen})
		continue
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 5")
	kv.mu.Lock()
	////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 5")
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var unique int64
	var KvS KvStorage
	var configs []shardctrler.Config
	if d.Decode(&unique) != nil ||
		d.Decode(&KvS) != nil ||
		d.Decode(&configs) != nil {
		return
	} else {
		if KvS.AppliedIndex > kv.KvS.AppliedIndex {
			kv.unique = unique
			kv.KvS = KvS
			kv.configs = configs

			kv.testConsistency("  A")
		}
	}
}

func (kv *ShardKV) testTrim() {
	if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 6")
		kv.mu.Lock()
		////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 5")
		defer kv.mu.Unlock()

		if kv.KvS.AppliedIndex < 0 {
			log.Panic("What???less then zero???")
		}
		kv.testConsistency("  B0")
		e.Encode(kv.unique)
		e.Encode(kv.KvS)
		e.Encode(kv.configs)
		kv.testConsistency("  B1")
		kv.rf.Snapshot(kv.KvS.AppliedIndex, w.Bytes())
		kv.testConsistency("  B2")
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
		////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 7")
		kv.isInTransfer.Lock()
		kv.isInTransfer.Unlock()
		////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 7")

		a := <-kv.applyCh
		if a.SnapshotValid {
			kv.installSnapshot(a.Snapshot)
		}
		op, ok := a.Command.(Op)

		if !ok {
			//////fmt.Println("Warning!")
			continue
		}

		re := ""
		////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 8")
		kv.mu.Lock()
		////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 8")
		if kv.KvS.AppliedIndex+1 != a.CommandIndex && kv.KvS.AppliedIndex != -1 {
			log.Panic(kv.KvS.AppliedIndex, "+1 != ", a.CommandIndex, "  me:", kv.me)
		}
		//fmt.Println("\nCompleting:", op.Id, "   shard:", key2shard(op.Key), " on me:", kv.me, "  index:", a.CommandIndex, " applied:", kv.KvS.S, "  unique:", kv.unique, "\n  gid:", kv.gid, "  gen:", len(kv.configs)-1)
		kv.KvS.AppliedIndex = a.CommandIndex
		if op.CfgGen != len(kv.configs)-1 {
			////fmt.Println("Outdated.Skip", op.Id, "   shard:", key2shard(op.Key), " on me:", kv.me, "  index:", a.CommandIndex, " applied:", kv.AppliedRPC, "  unique:", kv.unique, "  gid:", kv.gid)
			term, _ := kv.rf.GetState()
			_, fail := kv.callbackLt.popF(term, a.CommandIndex)
			fail("")
			kv.mu.Unlock()
			continue
		}
		if (op.Type != SetConfig && op.Id.RpcSeq > kv.KvS.S[key2shard(op.Key)].AppliedRPC[op.Id.ClientId]) || (op.Type == SetConfig && op.Id.RpcSeq > int64(kv.KvS.CurrentConfig)) {
			//if true {
			switch op.Type {
			case GetF:
				//fmt.Println("GET -- Key:", op.Key, "  Value:", kv.KvS.S[key2shard(op.Key)].S[op.Key])
				//re = kv.KvS.S[key2shard(op.Key)][op.Key]
				////fmt.Println("111")
			case PutF:
				//fmt.Println("PUT -- Key:", op.Key, "  Value(Old):", kv.KvS.S[key2shard(op.Key)].S[op.Key], "  Value(New):", op.Value)
				kv.KvS.S[key2shard(op.Key)].S[op.Key] = op.Value

				//re = op.Value
				////fmt.Println("222")
			case AppendF:
				//fmt.Println("APPEND -- Key:", op.Key, "  Value(Old):", kv.KvS.S[key2shard(op.Key)].S[op.Key], "  APPENDING:", op.Value, "  Value(New):", kv.KvS.S[key2shard(op.Key)].S[op.Key]+op.Value)
				kv.KvS.S[key2shard(op.Key)].S[op.Key] = kv.KvS.S[key2shard(op.Key)].S[op.Key] + op.Value
				//re = kv.KvS.S[key2shard(op.Key)][op.Key]
				////fmt.Println("333")
			case SetConfig:
				// Do SOMETHING TOMORROW. TO BE DONE
				kv.applyNewConfig(op.CFG)

			}

			//log.Println("From:", op.Id.ClientId, "  to:", kv.me)
			if op.Type != SetConfig {
				kv.KvS.S[key2shard(op.Key)].AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
			} else {
				kv.KvS.CurrentConfig = op.Id.RpcSeq
			}

			//}
		}

		re = kv.KvS.S[key2shard(op.Key)].S[op.Key]
		kv.mu.Unlock()

		term, isLeader := kv.rf.GetState()
		succeed, _ := kv.callbackLt.popF(term, a.CommandIndex)
		if isLeader {
			succeed(re)
		} else {
			kv.callbackLt.clearF(term)
		}
		////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 9")
		kv.isInTransfer.Lock()
		kv.isInTransfer.Unlock()
		////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 9")
		kv.testTrim()
	}
}

func deepCopy(old map[int]ShardState) map[int]ShardState {
	new := make(map[int]ShardState)
	for i1, v1 := range old {
		var n ShardState
		n.AppliedRPC = make(map[int64]int64)
		n.S = make(map[string]string)
		for i2, v2 := range v1.AppliedRPC {
			n.AppliedRPC[i2] = v2
		}
		for i2, v2 := range v1.S {
			n.S[i2] = v2
		}
		new[i1] = n
	}

	return new
}

func (kv *ShardKV) applyNewConfig(CFG shardctrler.Config) {

	////fmt.Println("unique:", kv.unique, "  Enter:", CFG, " on me:", kv.me, " applied:", kv.AppliedRPC, "  unique:", kv.unique)
	////fmt.Println("Print Configuration:", CFG)
	////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 10")
	kv.isInTransfer.Lock()
	////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 10")
	if len(kv.configs) == len(kv.KvS.OldS) {
		kv.KvS.OldS = append(kv.KvS.OldS, deepCopy(kv.KvS.S))
	} else {
		kv.testConsistency("  F")
		log.Panicln(len(kv.configs), " != ", len(kv.KvS.OldS), "    unique:", kv.unique)
	}

	shardsAppointed := []int{}
	shardsNew := []int{}
	shardsNeedGet := []int{}
	shardsHave := []int{}

	var CfgOld shardctrler.Config

	for i, v := range CFG.Shards {
		if v == kv.gid {
			shardsAppointed = append(shardsAppointed, i)
		}
	}

	//The following is evil.Should be corrected later.
	if len(kv.configs) == 0 {
		shardsNew = make([]int, len(shardsAppointed))
		copy(shardsNew, shardsAppointed)
	} else {
		CfgOld = kv.configs[len(kv.configs)-1]
		for _, shard := range shardsAppointed {
			switch {
			case CfgOld.Shards[shard] == 0:
				shardsNew = append(shardsNew, shard)
			case CfgOld.Shards[shard] == kv.gid:
				shardsHave = append(shardsHave, shard)
			default:
				shardsNeedGet = append(shardsNeedGet, shard)
			}

		}
	}

	////fmt.Println("shardsAppointed:", shardsAppointed, "  shardsNew", shardsNew)

	for _, v := range shardsNew {
		kv.KvS.S[v] = newShardState()
		////fmt.Println("Creating map:", v, "  on me:", kv.me, "  unique:", kv.unique)
	}

	//kv.KvS.ShardsAppointed = append(kv.KvS.ShardsAppointed, shardsNew...)
	kv.KvS.ShardsAppointed = append(shardsHave, shardsNew...)

	////fmt.Println("Setting own:", kv.KvS.ShardsAppointed, "  on me:", kv.me, "  unique:", kv.unique, "  gid:", kv.gid, "  Config:", CFG)

	go kv.getNewShards(len(kv.configs), shardsNeedGet, CfgOld, CFG)

}

func (kv *ShardKV) getNewShards(newIndex int, lt []int, CfgOld shardctrler.Config, CfgNew shardctrler.Config) {

	//kv.mu.Lock()
	//Evil begin.
	/*for _, v := range lt {
		kv.KvS.S[v] = make(map[string]string)
		////fmt.Println("Creating map:", v, "  on me:", kv.me, "  unique:", kv.unique)
	}*/
	for _, v := range lt {
		func(shard int, cfg shardctrler.Config) {
			t := kv.GetShard(shard, cfg)
			////fmt.Println(t)
			////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 11")
			kv.mu.Lock()
			kv.KvS.S[shard] = t
			////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 11")

			kv.mu.Unlock()
		}(v, CfgOld)
	}
	////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 12")
	kv.mu.Lock()
	////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 12")
	kv.KvS.ShardsAppointed = append(kv.KvS.ShardsAppointed, lt...) //make([]int, len(shardsAppointed))
	//Evil end.

	//DO SOMETHING HERE. TO BE DONE.
	kv.isInTransfer.Unlock()

	kv.configs = append(kv.configs, CfgNew)
	kv.testConsistency("  C")

	if len(kv.configs) != len(kv.KvS.OldS) {
		log.Panicln(len(kv.configs), " != ", len(kv.KvS.OldS), "    unique:", kv.unique, "   tag:aaa")
	}
	////fmt.Println("Need to get:", lt, "  on me:", kv.me, "  unique:", kv.unique, "  gid:", kv.gid, "  Config:", CfgNew)
	kv.mu.Unlock()
	////fmt.Println("unique:", kv.unique, "  Exit:", CfgNew, " on me:", kv.me, " applied:", kv.AppliedRPC, "  unique:", kv.unique)
	//////fmt.Println("Cfg:")

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

func (kv *ShardKV) testConsistency(tag string) {
	if len(kv.configs) == len(kv.KvS.OldS) {
		//kv.KvS.OldS = append(kv.KvS.OldS, deepCopy(kv.KvS.S))
	} else {
		log.Panicln(len(kv.configs), " != ", len(kv.KvS.OldS), "    unique:", kv.unique, "TAG:", tag)
	}
}

func (kv *ShardKV) GetShard(shard int, CFG shardctrler.Config) ShardState {
	args := RetriveShardArgs{}
	args.GenNum = CFG.Num
	args.ShardNum = shard

	//ck.mu.Lock()
	//defer ck.mu.Unlock()

	//args.Id.ClientId = ck.clienrId
	//args.Id.RpcSeq = ck.rpcSeq
	//ck.rpcSeq++

	for {
		gid := CFG.Shards[shard]
		if servers, ok := CFG.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply RetriveShardReply
				ok := srv.Call("ShardKV.GetShardRecv", &args, &reply)
				if ok && reply.Valid {
					//fmt.Println("Getting shard:", args, "  on me:", kv.me, "  unique:", kv.unique, "  gid:", kv.gid)
					return reply.Shard
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) GetShardRecv(args *RetriveShardArgs, reply *RetriveShardReply) {

	////fmt.Println("Before Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 16")
	kv.mu.Lock()
	////fmt.Println("After Lock :", " on me:", kv.me, "  unique:", kv.unique, "  Lock 16")
	defer kv.mu.Unlock()
	if args.GenNum+1 >= len(kv.KvS.OldS) {
		////fmt.Println("Not having:", args, "  on me:", kv.me, "  unique:", kv.unique, "  gid:", kv.gid)
		reply.Valid = false
		return
	} else {
		reply.Shard = kv.KvS.OldS[args.GenNum+1][args.ShardNum]
		////fmt.Println("Sending:", args, "  on me:", kv.me, "  unique:", kv.unique, "  gid:", kv.gid)
		if reply.Shard.S == nil {
			log.Panicln("I'm sending nil???:", kv.KvS.OldS[args.GenNum])
		} else {
			//fmt.Println("Sending shard:", args, " on me:", kv.me, "  unique:", kv.unique, "  \n contnt:", reply.Shard) //, "\n history:", kv.KvS.OldS)

		}
		reply.Valid = true
		return
	}
}
