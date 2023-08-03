package shardctrler

import (
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs      []Config // indexed by config num
	appliedIndex int
	callbackLt   CallBackList
	AppliedRPC   map[int64]int64
	persister    *raft.Persister
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
	succeedFun func(Config)
	failFun    func(Config)
	valid      bool
}

const (
	JoinF  = 1
	LeaveF = 2
	MoveF  = 3
	QueryF = 4
)

type Op struct {
	// Your data here.
	Type int

	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
	QueryNum    int

	Id RpcId

	Server int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	// Your code here.
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(Op{JoinF, args.Servers, []int{}, -1, -1, -1, args.Id, args.Server})
	sc.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		//reply.Err = Err(fmt.Sprint("Not leader"))
		return
	}

	var finished sync.Mutex
	finished.Lock()

	sc.callbackLt.reg(term,
		index,
		func(re Config) {
			reply.Config = re
			finished.Unlock()
		},
		func(re Config) {
			reply.Err = Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(Op{LeaveF, make(map[int][]string), args.GIDs, -1, -1, -1, args.Id, args.Server})
	sc.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		//reply.Err = Err(fmt.Sprint("Not leader"))
		return
	}

	var finished sync.Mutex
	finished.Lock()

	sc.callbackLt.reg(term,
		index,
		func(re Config) {
			reply.Config = re
			finished.Unlock()
		},
		func(re Config) {
			reply.Err = Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(Op{MoveF, make(map[int][]string), []int{}, args.Shard, args.GID, -1, args.Id, args.Server})
	sc.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		//reply.Err = Err(fmt.Sprint("Not leader"))
		return
	}

	var finished sync.Mutex
	finished.Lock()

	sc.callbackLt.reg(term,
		index,
		func(re Config) {
			reply.Config = re
			finished.Unlock()
		},
		func(re Config) {
			reply.Err = Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(Op{QueryF, make(map[int][]string), []int{}, -1, -1, args.Num, args.Id, args.Server})
	sc.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		//reply.Err = Err(fmt.Sprint("Not leader"))
		return
	}

	//fmt.Println("Query start:  -- ", args)
	//argsD := *args //Debug info

	var finished sync.Mutex
	finished.Lock()

	sc.callbackLt.reg(term,
		index,
		func(re Config) {
			reply.Config = re
			//fmt.Println("Exec complete:", argsD)
			finished.Unlock()
		},
		func(re Config) {
			reply.Err = Err("Exec fail")
			finished.Unlock()
		},
	)
	finished.Lock()
	finished.Unlock()

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.appliedIndex = -1
	sc.AppliedRPC = make(map[int64]int64)
	sc.callbackLt.callbackLt = make(map[ActId]CallBackTuple)
	sc.persister = persister

	go sc.applyF()
	go sc.clearReg()

	return sc
}

/*
func (sc *ShardCtrler) installSnapshot(snapshot []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var AppliedRPC map[int64]int64
	var appliedIndex int
	var configs []Config
	if d.Decode(&AppliedRPC) != nil ||
		d.Decode(&appliedIndex) != nil ||
		d.Decode(&configs) != nil {
		return
	} else {
		if appliedIndex > sc.appliedIndex {
			sc.AppliedRPC = AppliedRPC
			sc.appliedIndex = appliedIndex
			sc.configs = configs
		}
	}
}

func (sc *ShardCtrler) testTrim() {
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
*/

func (sc *ShardCtrler) clearReg() {
	for {
		ms := 3000
		time.Sleep(time.Duration(ms) * time.Millisecond)
		term, _ := sc.rf.GetState()
		if true { //!isLeader {
			sc.callbackLt.clearF(term)
		}
	}
}

func (sc *ShardCtrler) applyF() {
	for {
		a := <-sc.applyCh
		/*if a.SnapshotValid {
			sc.installSnapshot(a.Snapshot)
		}*/
		op, ok := a.Command.(Op)

		if !ok {
			//fmt.Println("Warning!")
			continue
		}

		var re Config
		sc.mu.Lock()
		if sc.appliedIndex+1 != a.CommandIndex && sc.appliedIndex != -1 {
			log.Panic(sc.appliedIndex, "+1 != ", a.CommandIndex, "  me:", sc.me)
		}
		sc.appliedIndex = a.CommandIndex
		if op.Id.RpcSeq > sc.AppliedRPC[op.Id.ClientId] {
			switch op.Type {
			case JoinF:
				sc.joinAction(op)
				//fmt.Println("Join --- op:", op, "   Configs:", sc.configs)
			case LeaveF:
				sc.leaveAction(op)
				//fmt.Println("Leave --- op:", op, "   Configs:", sc.configs)
			case MoveF:
				sc.moveAction(op)
				//fmt.Println("Move --- op:", op, "   Configs:", sc.configs)
			case QueryF:
				sc.queryAction(op)
				//fmt.Println("Query --- op:", op, "   Configs:", sc.configs)
			}

			if op.Id.RpcSeq != (sc.AppliedRPC[op.Id.ClientId] + 1) {
				debug.PrintStack()
				log.Fatal(op.Id.RpcSeq, " != ", sc.AppliedRPC[op.Id.ClientId], "+1")
			}
			sc.AppliedRPC[op.Id.ClientId] = op.Id.RpcSeq
		}

		if op.QueryNum >= 0 && op.QueryNum < len(sc.configs) {
			re = sc.configs[op.QueryNum]
		} else {
			re = sc.configs[len(sc.configs)-1]
		}

		sc.mu.Unlock()

		term, isLeader := sc.rf.GetState()
		succeed, _ := sc.callbackLt.popF(term, a.CommandIndex)
		if isLeader {
			succeed(re)
		} else {
			sc.callbackLt.clearF(term)
		}

		//sc.testTrim()
	}
}

func (lt *CallBackList) reg(term int, index int, succeed func(Config), fail func(Config)) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.callbackLt[ActId{term, index}] = CallBackTuple{succeed, fail, true}
}

func (lt *CallBackList) popF(term int, index int) (func(Config), func(Config)) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	re := lt.callbackLt[ActId{term, index}]
	delete(lt.callbackLt, ActId{term, index})
	if re.valid {
		return re.succeedFun, re.failFun
	} else {
		return func(x Config) {}, func(x Config) {}
	}
}

func (lt *CallBackList) clearF(term int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	for i, v := range lt.callbackLt {
		if i.term < term {
			var emptyC Config
			v.failFun(emptyC)
			delete(lt.callbackLt, i)
		}
	}
}

func copyConfig(old Config) Config {
	var new Config
	new.Num = old.Num
	new.Shards = old.Shards
	new.Groups = make(map[int][]string)

	for i, v := range old.Groups {
		new.Groups[i] = v
	}
	return new
}

func config2Map(c Config) map[int]([]int) {
	m := make(map[int]([]int))
	for i, _ := range c.Groups {
		m[i] = make([]int, 0)
	}
	for i, v := range c.Shards {
		m[v] = append(m[v], i)
	}
	return m
}

func map2Config(m map[int]([]int), s []int) {
	for i, v := range m {
		for _, v1 := range v {
			s[v1] = i
		}
	}
}

func distributeShard(newG map[int][]string, shards []int, mapping []int, num int) {
	for group, _ := range newG {
		for count := 0; count < num; count++ {
			//log.Panicln("Beeping!")
			if len(shards) != 0 {
				mapping[shards[0]] = group
				shards = shards[1:]
			}
		}
	}
}

type shardSet struct {
	GIP    int
	shards []int
	Valid  bool
}

// for sorting .
type ByGIP []shardSet

func (a ByGIP) Len() int           { return len(a) }
func (a ByGIP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByGIP) Less(i, j int) bool { return a[i].GIP < a[j].GIP }

func rebalance(old Config) Config {
	if len(old.Groups) == 0 {
		new := copyConfig(old)
		for i, _ := range new.Shards {
			new.Shards[i] = 0
		}
		return new
	}
	numPerG := len(old.Shards) / len(old.Groups)

	gip2shard := make(map[int]shardSet)
	for i, _ := range old.Groups {
		gip2shard[i] = shardSet{i, make([]int, 0), true}
	}

	freed := []int{}
	new := copyConfig(old)
	for i, v := range new.Shards {
		if v != 0 && len(gip2shard[v].shards) < numPerG && gip2shard[v].Valid {
			tmp := gip2shard[v]
			tmp.shards = append(tmp.shards, i)
			gip2shard[v] = tmp
		} else {
			new.Shards[i] = 0
			freed = append(freed, i)
		}
	}

	//fmt.Println("Freed:", freed)

	var lt ByGIP
	for _, v := range gip2shard {
		lt = append(lt, v)
	}
	sort.Sort(ByGIP(lt))
	fmt.Println("1  lt:", lt)
	for i, v := range lt {
		for len(v.shards) < numPerG && len(freed) > 0 {
			v.shards = append(v.shards, freed[0])
			freed = freed[1:]
			lt[i] = v
		}
	}
	//fmt.Println("2  lt:", lt)
	for i, v := range lt {
		for len(freed) > 0 {
			v.shards = append(v.shards, freed[0])
			freed = freed[1:]
			lt[i] = v
		}
	}
	//fmt.Println("3  lt:", lt)
	for _, v := range lt {
		for _, v1 := range v.shards {
			new.Shards[v1] = v.GIP
		}
	}
	return new

}

func (sc *ShardCtrler) joinAction(op Op) {
	newNum := len(sc.configs)
	newConfig := copyConfig(sc.configs[newNum-1])
	newConfig.Num = newNum

	newCount := 0
	for i, v := range op.JoinServers {
		newConfig.Groups[i] = v
		newCount++
	}
	newConfig = rebalance(newConfig)
	/*
		shardPerG := len(newConfig.Shards) / len(newConfig.Groups)
		fmt.Println("For any group, num:", shardPerG)

		shardsD := []int{}
		{
			count := 0
			m := config2Map(newConfig)
			for {
				if count == shardPerG*newCount {
					break
				}
				for i, v := range m {
					//log.Println("Emmmm Beeping!")
					if count == shardPerG*newCount {
						break
					}
					if len(v) > 0 {
						fmt.Println("appending;", v[0], "  from:", v)
						shardsD = append(shardsD, v[0])
						//v = v[1:]
						m[i] = v[1:]
						count++
					}
				}
			}
		}
		fmt.Println("shardsD:", shardsD)
		fmt.Println("newConfigBefore:", newConfig)
		distributeShard(op.JoinServers, shardsD, newConfig.Shards[:], shardPerG)

		shardsD = []int{}
		for i, v := range newConfig.Shards {
			if v == 0 {
				shardsD = append(shardsD, i)
			}
		}
		distributeShard(op.JoinServers, shardsD, newConfig.Shards[:], 1)

		fmt.Println("newConfig:", newConfig)*/
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) leaveAction(op Op) {
	newNum := len(sc.configs)
	newConfig := copyConfig(sc.configs[newNum-1])
	newConfig.Num = newNum

	for _, v := range op.LeaveGIDs {
		delete(newConfig.Groups, v)
	}

	/*shardsD := []int{}
	for i, v := range newConfig.Shards {
		for _, v1 := range op.LeaveGIDs {
			if v1 == v {
				shardsD = append(shardsD, i)
			}
		}
	}
	if len(newConfig.Groups) != 0 {
		shardPerG := len(shardsD)/len(newConfig.Groups) + 1
		if (shardPerG * len(newConfig.Groups)) < len(shardsD) {
			log.Panic((shardPerG * len(newConfig.Groups)), "<len", (shardsD))
		}
		distributeShard(newConfig.Groups, shardsD, newConfig.Shards[:], shardPerG)
	} else {
		for i, _ := range newConfig.Shards {
			newConfig.Shards[i] = 0
		}
	}*/
	newConfig = rebalance(newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) moveAction(op Op) {
	newNum := len(sc.configs)
	newConfig := copyConfig(sc.configs[newNum-1])
	newConfig.Num = newNum

	newConfig.Shards[op.MoveShard] = op.MoveGID
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) queryAction(op Op) {
}
