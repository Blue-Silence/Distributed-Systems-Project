package shardkv

import (
	"bytes"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	AddShard  = -1
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

	SID   ShardID
	Shard ShardState
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
	persister  *raft.Persister

	configs Configs

	mck *shardctrler.Clerk

	unique int64
}

type mem struct{}
type IntSet map[int]mem

type KvStorage struct {
	AppliedIndex  int
	S             map[ShardID](ShardState)
	CurrentConfig int64
	ToBePoll      IntSet
	ShardGen      map[int]int64
}

func (k *KvStorage) Init() {
	k.S = make(map[ShardID]ShardState)
	k.AppliedIndex = -1
	k.CurrentConfig = -1
	k.ToBePoll = make(IntSet)
	k.ShardGen = make(map[int]int64)
}

type Configs struct {
	CurGen int
	S      map[int]shardctrler.Config
}

func (c *Configs) Init() {
	c.CurGen = -1
	c.S = make(map[int]shardctrler.Config)
}

func (c *Configs) Add(cfg shardctrler.Config) {
	if c.CurGen+1 != cfg.Num {
		debug.PrintStack()
		log.Panicln(c.CurGen+1, " != ", cfg.Num)
	}
	c.CurGen = cfg.Num
	c.S[cfg.Num] = cfg
}

func (c *Configs) Get(gen int) shardctrler.Config {
	re, ok := c.S[gen]
	if !ok {
		debug.PrintStack()
		log.Panicln("Not exist:", gen)
	}
	return re
}

type ShardState struct {
	AppliedRPC map[int64]int64
	S          (map[string]string)
}

type ShardID struct {
	Gen      int
	ShardNum int
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
	kv.mu.Lock()
	if !kv.CheckAvailable(key2shard(args.Key)) {

		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	cfgGen := kv.configs.CurGen
	kv.mu.Unlock()

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(Op{GetF, args.Key, "", shardctrler.Config{}, args.Id, args.Server, cfgGen, ShardID{}, newShardState()})
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
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
	kv.mu.Lock()
	if !kv.CheckAvailable(key2shard(args.Key)) {

		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	cfgGen := kv.configs.CurGen
	kv.mu.Unlock()

	index := 0
	term := 0
	isLeader := false
	kv.mu.Lock()

	if args.Op == "Put" {
		index, term, isLeader = kv.rf.Start(Op{PutF, args.Key, args.Value, shardctrler.Config{}, args.Id, args.Server, cfgGen, ShardID{}, newShardState()})
	} else {
		index, term, isLeader = kv.rf.Start(Op{AppendF, args.Key, args.Value, shardctrler.Config{}, args.Id, args.Server, cfgGen, ShardID{}, newShardState()})
	}

	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var finished sync.Mutex
	finished.Lock()

	kv.callbackLt.reg(term,
		index,
		func(re string) {
			reply.Err = OK
			////fmt.Println("Exec succeed:", args)
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
	kv.KvS.Init()
	kv.configs.Init()

	kv.unique = nrand()
	kv.persister = persister

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.installSnapshot(persister.ReadSnapshot())

	go kv.applyF()
	go kv.clearReg()

	go kv.autoUpdateShards()
	go kv.autoGC()

	return kv
}

func (kv *ShardKV) autoUpdateShards() {
	for {

		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		kv.mu.Lock()
		configTail := kv.mck.Query(-1)
		var config shardctrler.Config
		cfgGen := kv.configs.CurGen
		if kv.configs.CurGen < configTail.Num {
			config = kv.mck.Query(kv.configs.CurGen + 1)
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
			continue
		}
		kv.rf.Start(Op{SetConfig, "", "", config, RpcId{-1, int64(config.Num)}, -1, cfgGen, ShardID{}, newShardState()})
		continue
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var unique int64
	var KvS KvStorage
	var configs Configs
	if d.Decode(&unique) != nil ||
		d.Decode(&KvS) != nil ||
		d.Decode(&configs) != nil {
		return
	} else {
		if KvS.AppliedIndex > kv.KvS.AppliedIndex {
			kv.unique = unique
			kv.KvS = KvS
			kv.configs = configs

			for i, _ := range kv.KvS.ToBePoll {
				go kv.PollShard(i, true)
			}
		}
	}
}

func (kv *ShardKV) testTrim() {
	if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.takeSnapshot()

	}
}

func (kv *ShardKV) takeSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.KvS.AppliedIndex < 0 {
		log.Panic("What???less then zero???")
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.unique)
	e.Encode(kv.KvS)
	e.Encode(kv.configs)
	kv.rf.Snapshot(kv.KvS.AppliedIndex, w.Bytes())

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
			continue
		}

		re := ""

		kv.mu.Lock()

		if kv.KvS.AppliedIndex+1 != a.CommandIndex && kv.KvS.AppliedIndex != -1 {
			log.Panic(kv.KvS.AppliedIndex, "+1 != ", a.CommandIndex, "  me:", kv.me)
		}
		kv.KvS.AppliedIndex = a.CommandIndex
		if op.CfgGen != kv.configs.CurGen {
			term, _ := kv.rf.GetState()
			_, fail := kv.callbackLt.popF(term, a.CommandIndex)
			fail("")
			kv.mu.Unlock()
			continue
		}
		curGen := kv.configs.CurGen
		if ((op.Type == GetF || op.Type == PutF || op.Type == AppendF) && op.Id.RpcSeq > kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].AppliedRPC[op.Id.ClientId]) ||
			(op.Type == SetConfig && op.Id.RpcSeq > int64(kv.KvS.CurrentConfig)) ||
			(op.Type == AddShard && kv.KvS.ShardGen[op.SID.ShardNum] < int64(op.SID.Gen)) {

			switch op.Type {
			case GetF:
				kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
			case PutF:
				kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].S[op.Key] = op.Value
				kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
			case AppendF:
				kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].S[op.Key] = kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].S[op.Key] + op.Value
				kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
			case SetConfig:
				kv.applyNewConfig(op.CFG)
				kv.KvS.CurrentConfig = op.Id.RpcSeq
				go kv.GC()
			case AddShard:
				kv.KvS.S[op.SID] = op.Shard
				kv.KvS.ShardGen[op.SID.ShardNum] = int64(op.SID.Gen)

			}
		}

		re = kv.KvS.S[ShardID{curGen, key2shard(op.Key)}].S[op.Key]
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

	kv.configs.Add(CFG)

	kv.KvS.ToBePoll[CFG.Num] = mem{}

	kv.PollShard(CFG.Num, false)
	go func() {
		for {
			time.Sleep(20 * time.Millisecond)
			kv.mu.Lock()
			if _, ok := kv.KvS.ToBePoll[CFG.Num]; !ok {
				kv.mu.Unlock()
				break
			}
			kv.mu.Unlock()
		}
	}()

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

func (kv *ShardKV) GetShard(shard int, CFG shardctrler.Config) ShardState {
	args := RetriveShardArgs{}
	args.GenNum = CFG.Num
	args.ShardNum = shard

	for {
		gid := CFG.Shards[shard]
		if servers, ok := CFG.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply RetriveShardReply
				ok := srv.Call("ShardKV.GetShardRecv", &args, &reply)
				if ok && reply.Valid {
					return reply.Shard
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) GetShardRecv(args *RetriveShardArgs, reply *RetriveShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	curGen := kv.configs.CurGen
	if shard, ok := kv.KvS.S[ShardID{args.GenNum, args.ShardNum}]; !(ok && curGen > args.GenNum) {
		reply.Valid = false
		return
	} else {
		reply.Shard = shard
		reply.Valid = true
		return
	}
}

func (kv *ShardKV) CheckAvailable(shard int) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	curGen := kv.configs.CurGen
	_, ok := kv.KvS.S[ShardID{curGen, shard}]
	return ok
}

func (kv *ShardKV) PollShard(gen int, isAsync bool) {
	var old shardctrler.Config
	var new shardctrler.Config
	oldGen := gen - 1

	if isAsync {
		kv.mu.Lock()
	}

	gid := kv.gid
	if gen != 0 {
		old = kv.configs.Get(gen - 1)
	}
	new = kv.configs.Get(gen)
	//kv.mu.Unlock()

	shardsDeleted, shardsMoved, shardsStay, shardsGet, shardsNew := calcDiff(old, new, gid)

	func(a []int) {}(shardsMoved)

	//kv.mu.Lock()
	curGen := kv.configs.CurGen
	for _, s := range shardsDeleted {
		if _, ok := kv.KvS.S[ShardID{oldGen, s}]; ok {
			delete(kv.KvS.S, ShardID{oldGen, s})
		}
	}

	for _, s := range shardsNew {
		if _, ok := kv.KvS.S[ShardID{gen, s}]; !ok {
			kv.KvS.S[ShardID{gen, s}] = newShardState()
			kv.KvS.ShardGen[s] = int64(curGen)
		}
	}
	if isAsync {
		kv.mu.Unlock()
	}

	var threadCount int64 = 0

	for _, s := range shardsStay {
		atomic.AddInt64(&threadCount, 1)
		go func(s int) {
			for {
				time.Sleep(30 * time.Millisecond)
				kv.mu.Lock()
				curGen := kv.configs.CurGen
				if _, ok := kv.KvS.S[ShardID{oldGen, s}]; ok {
					t := kv.KvS.S[ShardID{oldGen, s}]
					kv.rf.Start(Op{AddShard, "", "", shardctrler.Config{}, RpcId{}, -1, curGen, ShardID{gen, s}, t})
				}
				if _, ok := kv.KvS.S[ShardID{gen, s}]; ok {
					atomic.AddInt64(&threadCount, -1)
					//delete(kv.KvS.S, ShardID{oldGen, s})
					kv.mu.Unlock()
					break
				}
				kv.mu.Unlock()
			}
		}(s)
	}

	for _, s := range shardsGet {
		atomic.AddInt64(&threadCount, 1)
		go func(s int) {
			t := kv.GetShard(s, old)
			for {
				time.Sleep(30 * time.Millisecond)
				kv.mu.Lock()
				curGen := kv.configs.CurGen
				kv.rf.Start(Op{AddShard, "", "", shardctrler.Config{}, RpcId{}, -1, curGen, ShardID{gen, s}, t})
				if _, ok := kv.KvS.S[ShardID{gen, s}]; ok {
					atomic.AddInt64(&threadCount, -1)
					kv.mu.Unlock()
					break
				}
				kv.mu.Unlock()
			}
		}(s)
	}

	go func() {
		for {
			time.Sleep(30 * time.Millisecond)
			t := atomic.LoadInt64(&threadCount)
			if t == 0 {
				kv.mu.Lock()
				delete(kv.KvS.ToBePoll, gen)
				kv.mu.Unlock()
				break
			}
		}
	}()
}

func calcDiff(old, new shardctrler.Config, self int) ([]int, []int, []int, []int, []int) {
	shardsDeleted := []int{}
	shardsMoved := []int{}
	shardsStay := []int{}
	shardsGet := []int{}
	shardsNew := []int{}

	maxL := func(a, b int) int {
		if a > b {
			return a
		} else {
			return b
		}
	}(len(old.Shards), len(new.Shards))

	for i := 0; i < maxL; i++ {
		switch {
		case i >= len(old.Shards):
			shardsNew = append(shardsNew, i)
		case i >= len(new.Shards):
			shardsDeleted = append(shardsDeleted, i)
		case new.Shards[i] == self:
			switch {
			case old.Shards[i] == self:
				shardsStay = append(shardsStay, i)
			case old.Shards[i] == 0:
				shardsNew = append(shardsNew, i)
			default:
				shardsGet = append(shardsGet, i)
			}
		case old.Shards[i] == self:
			switch {
			case new.Shards[i] == 0:
				shardsDeleted = append(shardsDeleted, i)
			default:
				shardsMoved = append(shardsMoved, i)
			}
		}
	}

	return shardsDeleted, shardsMoved, shardsStay, shardsGet, shardsNew
}

func (kv *ShardKV) calcOwner() map[ShardID]int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ownerMap := make(map[ShardID]int)
	curGen := kv.configs.CurGen
	for id, _ := range kv.KvS.S {
		if id.Gen != curGen {
			ownerMap[id] = kv.configs.Get(id.Gen + 1).Shards[id.ShardNum] //Find the next owner
		}
	}

	return ownerMap
}

func (kv *ShardKV) autoGC() {
	for {
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		go kv.GC()
	}
}

func (kv *ShardKV) GC() {
	ownerMap := kv.calcOwner()
	kv.mu.Lock()
	if kv.configs.CurGen <= 0 {
		kv.mu.Unlock()
		return
	}

	allServer := make(map[int][]string)
	for _, v := range kv.configs.S {
		for i, s := range v.Groups {
			allServer[i] = s
		}
	}

	kv.mu.Unlock()
	for id, owner := range ownerMap {
		go func(id ShardID, owner int) {
			if ownerGen, isAlive := kv.CheckOwner(owner, id.ShardNum, allServer); ownerGen > id.Gen || !isAlive || (ownerGen == id.Gen && owner != kv.gid) {
				kv.mu.Lock()
				delete(kv.KvS.S, id)
				kv.takeSnapshot()
				kv.mu.Unlock()
			}
		}(id, owner)
	}

	kv.configsGC()

}

func (kv *ShardKV) CheckOwner(ownerGid int, shard int, allServer map[int][]string) (int, bool) {
	for {
		args := CheckOwnerArgs{shard}
		gid := ownerGid
		if servers, ok := allServer[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply CheckOwnerReply
				ok := srv.Call("ShardKV.CheckOwnerRecv", &args, &reply)
				if ok {
					return reply.GenNum, true
				}
			}
		} else {
			log.Panic("Missing:", ownerGid)
			return -1, false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return -1, true
}

func (kv *ShardKV) CheckOwnerRecv(args *CheckOwnerArgs, reply *CheckOwnerReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if re, ok := kv.KvS.ShardGen[(args.ShardNum)]; ok {
		reply.GenNum = int(re)
	} else {
		reply.GenNum = -1
	}
}

func (kv *ShardKV) calcConfigNeeded() IntSet {
	m := make(IntSet)
	for i, _ := range kv.KvS.ToBePoll {
		m[i] = mem{}
		m[i+1] = mem{}
		m[i-1] = mem{}
	}

	for i, _ := range kv.KvS.S {
		m[i.Gen+1] = mem{}
	}

	m[kv.configs.CurGen] = mem{}

	return m
}

func (kv *ShardKV) configsGC() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	m := kv.calcConfigNeeded()
	for gen, _ := range kv.configs.S {
		if _, needed := m[gen]; !needed {
			delete(kv.configs.S, gen)
		}
	}
}
