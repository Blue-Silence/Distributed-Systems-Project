package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	//"fmt"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	follower  int = 0
	candidate     = 1
	leader        = 2
)

const bufferSize int = 3
const electionTimeOut int64 = 200

type LogInfo struct {
	Term       int
	Index      int
	OuterIndex int
}

type Log struct {
	Command interface{}
	Info    LogInfo
	IsNOP   bool
}

type Peer struct {
	mu          sync.Mutex
	sendSuccess bool
	nextIndex   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state
	//peers     []*labrpc.ClientEnd // RPC end points of all peers
	peers     []*Peer
	peersE    []*labrpc.ClientEnd
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	dead      int32      // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	leaderID int
	term     int
	//termVoted int
	voteFor *int
	state   int
	//isLeader bool
	recvHeartbeat bool

	//2B
	tailLogInfo LogInfo
	CommitIndex int
	LastApplied int
	logs        []Log
	copyCount   map[int]int
	applyCh     chan ApplyMsg
	logHistory  int

	forwardCh   chan int
	countBuffer chan int

	//2D
	logOffset    int
	snapshot     []byte
	snapshotTail LogInfo
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here (2A).

	//rf.mkHeartBeat()
	/*rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if(state == leader) {
		rf.mkHeartBeat()
	}

	ms := 50
	time.Sleep(time.Duration(ms) * time.Millisecond)*/

	rf.mu.Lock()
	term = rf.term
	if rf.state == leader {
		isleader = true
	}
	rf.mu.Unlock()

	////////////////////fmt.Println("GetState term:", term, " state:", rf.state , "  ID: ", rf.me)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {

	upper := rf.tailLogInfo.Index - rf.snapshotTail.Index
	rf.logs = rf.logs[0:upper]
	copy(rf.logs, rf.logs)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	if rf.voteFor == nil {
		e.Encode(-1)
	} else {
		e.Encode(*rf.voteFor)
	}
	e.Encode(rf.logs)
	e.Encode(rf.tailLogInfo)
	e.Encode(rf.logHistory)
	e.Encode((rf.snapshotTail.Index + 1))
	e.Encode(rf.snapshotTail)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logs []Log
	var tailLogInfo LogInfo
	var logHistory int
	var logOffset int
	//var snapshot []byte
	var snapshotTail LogInfo

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&tailLogInfo) != nil ||
		d.Decode(&logHistory) != nil ||
		d.Decode(&logOffset) != nil ||
		d.Decode(&snapshotTail) != nil {
		return
	} else {
		rf.mu.Lock()
		rf.term = term
		rf.voteFor = &voteFor
		rf.logs = logs
		rf.tailLogInfo = tailLogInfo
		rf.logHistory = logHistory
		(rf.logOffset) = logOffset
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.snapshotTail = snapshotTail
		rf.CommitIndex = snapshotTail.Index
		rf.LastApplied = snapshotTail.Index
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if index == rf.snapshotTail.OuterIndex {
		rf.snapshot = snapshot
		return
	}

	if index <= rf.snapshotTail.OuterIndex {
		return
	}

	logIndex := -1
	for i, v := range rf.logs {
		if !v.IsNOP && v.Info.OuterIndex == index {
			logIndex = i
			break
		}
	}

	if logIndex == -1 {
		log.Panic("Can't find matching index. Index required:", index, "   logTail:", rf.logs[0], "   snapshotTail:", rf.snapshotTail.OuterIndex)
	}

	(rf.logOffset) = rf.logs[logIndex].Info.Index + 1
	upper := rf.tailLogInfo.Index - rf.snapshotTail.Index
	rf.snapshotTail = rf.logs[logIndex].Info
	rf.logs = rf.logs[logIndex+1 : upper]
	copy(rf.logs, rf.logs)
	//rf.logs = append(rf.logs,{})
	rf.snapshot = snapshot
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	From        int
	TailLogInfo LogInfo
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = args.Term
	switch {
	case args.Term > rf.term:
		rf.state = follower
		rf.voteFor = nil
		rf.term = args.Term
		rf.voteFor = &args.From
		reply.VoteGranted = true
	case args.Term == rf.term && (rf.voteFor == nil || *rf.voteFor == args.From):
		rf.voteFor = &args.From
		reply.VoteGranted = true
		rf.state = follower
	default:
		reply.Term = rf.term
		reply.VoteGranted = false
	}

	if !(args.TailLogInfo.Term > rf.tailLogInfo.Term || ((args.TailLogInfo.Term == rf.tailLogInfo.Term) && (args.TailLogInfo.Index >= rf.tailLogInfo.Index))) {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		rf.recvHeartbeat = true
	}
	rf.persist()
	rf.mu.Unlock()

}

type AppendEntriesArgs struct {
	LeaderId int
	Term     int

	Entries     []Log
	PrevLog     LogInfo
	CommitIndex int

	IsSnapshot   bool
	Snapshot     []byte
	SnapshotTail LogInfo

	From int
	Tag  int64
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	Conflict LogInfo

	From int
	//Tag int64
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.From = rf.me
	reply.Success = true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term >= rf.term {

		if args.Term > rf.term {
			rf.voteFor = nil
			rf.term = args.Term
		}

		rf.leaderID = args.LeaderId
		rf.recvHeartbeat = true
		rf.term = args.Term
		rf.state = follower

	} else {
		reply.Success = false
		reply.Term = rf.term
		return
	}

	//If the leader is stale,won't got below.
	if args.IsSnapshot {
		reply.Success = true
		if args.SnapshotTail.Index > rf.snapshotTail.Index {

			rf.snapshot = args.Snapshot
			if rf.tailLogInfo.Index <= args.SnapshotTail.Index {
				rf.logs = []Log{}
				rf.tailLogInfo = args.SnapshotTail
			} else {
				rf.logs = rf.logs[args.SnapshotTail.Index-rf.snapshotTail.Index:]
				copy(rf.logs, rf.logs)
			}
			rf.snapshotTail = args.SnapshotTail
			if rf.CommitIndex < args.SnapshotTail.Index {
				rf.CommitIndex = args.SnapshotTail.Index
			}
		}
		return
	}

	if args.PrevLog.Index <= rf.tailLogInfo.Index && args.PrevLog.Index >= (rf.snapshotTail.Index+1) {
		reply.Conflict.Term = rf.logs[args.PrevLog.Index-(rf.snapshotTail.Index+1)].Info.Term
		for _, v := range rf.logs {
			if v.Info.Term == reply.Conflict.Term {
				reply.Conflict.Index = v.Info.Index
				break
			}
		}
	} else {
		reply.Conflict.Term = -1 //rf.tailLogInfo.Term
		reply.Conflict.Index = rf.tailLogInfo.Index + 1
	}

	switch {
	case args.PrevLog.Index > rf.tailLogInfo.Index:
		reply.Success = false
		return
	case args.PrevLog.Index < (rf.snapshotTail.Index+1)-1:
		reply.Success = true
		return // Stale append
	case args.PrevLog.Index != (rf.snapshotTail.Index+1)-1 && rf.logs[args.PrevLog.Index-(rf.snapshotTail.Index+1)].Info != args.PrevLog:
		reply.Success = false
		return
	default:
		if len(args.Entries) > 0 {
			//////////////////fmt.Println("Append  ",args.Entries, " on ID:",rf.me, "  tag:",args.Tag, "  from:",args.From, "Term:", rf.term, "  tail:",rf.tailLogInfo)
		}

		for _, v := range args.Entries {
			//////////////////////////////fmt.Println("Append  ",v, " on ID:",rf.me, "  tag:",args.Tag)
			switch {
			case v.Info.Index == len(rf.logs)+rf.snapshotTail.Index+1:
				rf.logs = append(rf.logs, v)
			case v.Info.Index < len(rf.logs)+rf.snapshotTail.Index+1:
				rf.logs[v.Info.Index-(rf.snapshotTail.Index+1)] = v
			default:
				////////////////////////////fmt.Println("Warning.Inconsistency between taillog and log[].")
			}
			// This is used to deal with self-overwritten that results in recounting.
			switch {
			case rf.logHistory < args.Term:
				rf.logHistory = args.Term
				rf.tailLogInfo = v.Info
			case rf.logHistory > args.Term:
				////////////////////fmt.Println("Warning!!!")
			case rf.tailLogInfo.Index < v.Info.Index:
				rf.tailLogInfo = v.Info
			default:
			}
		}
		if rf.tailLogInfo.Index >= args.CommitIndex && rf.CommitIndex < args.CommitIndex && rf.logHistory == args.Term {
			rf.CommitIndex = args.CommitIndex
		}

	}

	reply.Term = rf.term

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peersE[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peersE[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	return rf.insert(command, false, true)

}

func (rf *Raft) insert(command interface{}, IsNOP bool, block bool) (int, int, bool) {
	indexO := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	if rf.state != leader {
		isLeader = false
		rf.mu.Unlock()

	} else {
		rf.tailLogInfo.Index++
		rf.tailLogInfo.Term = rf.term
		rf.logs = append(rf.logs, Log{})
		if !IsNOP {
			rf.tailLogInfo.OuterIndex++
			rf.logs[rf.tailLogInfo.Index-(rf.snapshotTail.Index+1)] = Log{Command: command, Info: rf.tailLogInfo, IsNOP: false}
		} else {
			rf.logs[rf.tailLogInfo.Index-(rf.snapshotTail.Index+1)] = Log{IsNOP: true, Info: rf.tailLogInfo, Command: command}
		}

		term = rf.term
		indexO = rf.tailLogInfo.OuterIndex
		rf.copyCount[rf.tailLogInfo.Index] = 1

		rf.persist()
		rf.mu.Unlock()
		if block {
			rf.countBuffer <- 0
		}
	}

	return indexO, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		f := rf.recvHeartbeat
		state := rf.state
		peers := rf.peers
		me := rf.me

		rf.recvHeartbeat = false

		if rf.state == leader {
			i := 1
			for _, p := range rf.peers {
				if p.sendSuccess {
					i++
					p.sendSuccess = false
				}
			}
			if i <= len(rf.peers)/3 {
				rf.state = follower
				f = false
			}
		}
		rf.mu.Unlock()

		if !f && state == follower {
			rf.mu.Lock()
			rf.state = candidate
			rf.term++
			term := rf.term
			rf.voteFor = &rf.me

			state := rf.state
			logInfo := rf.tailLogInfo
			rf.persist()
			rf.mu.Unlock()

			num := 0
			voted := 1
			for i, _ := range peers {
				rf.mu.Lock()
				state = rf.state
				rf.mu.Unlock()
				if state != candidate {
					break
				}
				num++
				if i == me {
					continue
				}
				go func(i int, depth int) {
					if depth == 0 {
						return
					}
					args := RequestVoteArgs{Term: term, From: rf.me, TailLogInfo: logInfo}
					reply := RequestVoteReply{}

					if !rf.sendRequestVote(i, &args, &reply) {
						//ms := 50
						//time.Sleep(time.Duration(ms) * time.Millisecond)
						rf.sendRequestVote(i, &args, &reply)
					}
					rf.mu.Lock()
					switch {
					case reply.VoteGranted:
						voted++
					case rf.state != candidate:

					default:
						if reply.Term > rf.term {
							rf.state = follower
							rf.term = reply.Term
							rf.persist()
						}
					}
					//rf.persist()
					rf.mu.Unlock()

				}(i, 2)
			}
			//ms := (100 + (rand.Int63() % 100))
			ms := 10
			for i := 0; i < 10; i++ {
				rf.mu.Lock()
				if voted > num/2 || rf.state != candidate {
					rf.mu.Unlock()
					break

				}
				rf.mu.Unlock()
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}

			rf.mu.Lock()
			if voted > num/2 && rf.state == candidate {
				rf.voteFor = &rf.me
				rf.term = term
				rf.state = leader
				rf.logHistory = rf.term
				rf.copyCount = make(map[int]int)
				for i, _ := range rf.peers {
					rf.peers[i] = &Peer{nextIndex: rf.tailLogInfo.Index + 1, sendSuccess: false}
				}
				rf.persist()
				rf.mu.Unlock()
				rf.insert(0, true, false)
				rf.forwardCh <- 0
				ms := (50 + (rand.Int63() % 200))
				//ms := (150 + (rand.Int63() % 200))
				time.Sleep(time.Duration(ms) * time.Millisecond)
			} else {
				rf.state = follower
				//rf.persist()
				rf.mu.Unlock()

				ms := (200 + (rand.Int63() % 100))
				time.Sleep(time.Duration(ms) * time.Millisecond)

			}

		} else {
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			//ms := (150 + (rand.Int63() % 200))
			ms := (200 + (rand.Int63() % 200))
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

	}
}

func (rf *Raft) heartBeat() {

	go func() {
		for {
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			//ms := (50 + (rand.Int63() % 300)) /5
			ms := 60 //100 //20 // 50
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.forwardCh <- 0
		}
	}()

	for rf.killed() == false {

		_ = <-rf.forwardCh
		rf.mkHeartBeat()
	}
}

func (rf *Raft) mkHeartBeat() {
	rf.mu.Lock()
	state := rf.state
	peers := rf.peers
	me := rf.me
	rf.mu.Unlock()
	if state == leader {

		for i, _ := range peers {

			if i == me {
				continue
			}

			go rf.requestForwardEntries(i)
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = make([]*Peer, len(peers))
	rf.persister = persister
	rf.me = me

	rf.term = 0
	rf.voteFor = nil
	rf.leaderID = 0
	rf.state = follower
	rf.recvHeartbeat = false

	rf.tailLogInfo.Term = 0
	rf.tailLogInfo.Index = 0
	// Your initialization code here (2A, 2B, 2C).

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.logs = []Log{Log{Info: LogInfo{0, 0, 0}, IsNOP: true}}
	rf.copyCount = make(map[int]int)
	rf.applyCh = applyCh
	rf.forwardCh = make(chan int, 4096)
	rf.countBuffer = make(chan int, bufferSize*10)

	rf.peersE = peers
	for i, _ := range peers {
		rf.peers[i] = &Peer{nextIndex: 1}
	}

	(rf.logOffset) = 0
	rf.snapshot = []byte{}
	rf.snapshotTail = LogInfo{-1, -1, -1}
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	go rf.scanCommitable()
	go rf.finalCommit()
	go rf.batching()
	go rf.singlePack()

	go rf.beep()

	return rf
}

func (rf *Raft) requestForwardEntries(server int) (bool, bool, int) {
	stillLeader := true
	succeedForward := true
	succeedIndex := 0

	rf.mu.Lock()

	term := rf.term
	pe := rf.peers[server]
	if rf.state != leader {
		rf.mu.Unlock()
		return false, false, -1
	}
	rf.mu.Unlock()
	pe.mu.Lock()
	defer pe.mu.Unlock()

	rf.mu.Lock()

	if term != rf.term {
		rf.mu.Unlock()
		return false, false, -1
	}

	reply := AppendEntriesReply{}
	nextIndex := pe.nextIndex

	args := AppendEntriesArgs{LeaderId: rf.me, Term: rf.term, CommitIndex: rf.CommitIndex, From: rf.me, Entries: []Log{}, IsSnapshot: false}
	switch {
	case nextIndex-1 == rf.snapshotTail.Index:
		args.PrevLog = rf.snapshotTail
	case nextIndex-1 > rf.snapshotTail.Index:
		args.PrevLog = rf.logs[nextIndex-1-(rf.snapshotTail.Index+1)].Info
	case nextIndex <= rf.snapshotTail.Index:
		args.IsSnapshot = true
		args.Snapshot = rf.snapshot
		args.SnapshotTail = rf.snapshotTail
	default:
	}
	if nextIndex <= rf.tailLogInfo.Index && nextIndex > rf.snapshotTail.Index {
		for i := nextIndex; i <= rf.tailLogInfo.Index; i++ {
			args.Entries = append(args.Entries, rf.logs[i-(rf.snapshotTail.Index+1)])
		}
	}

	state := rf.state
	//rf.persist()
	rf.mu.Unlock()

	if state != leader {
		return false, false, -1
	}
	ok1 := false
	go func() {
		ok1 = rf.sendAppendEntries(server, &args, &reply)
	}()
	ms := 10
	for i := 0; i < 10 && !ok1; i++ {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	ok := ok1

	rf.mu.Lock()

	if ok {
		pe.sendSuccess = true
	}

	switch {
	case !ok:
		succeedForward = false

	case term != rf.term:
		succeedForward = false
	case rf.term < reply.Term:
		stillLeader = false
		rf.state = follower
		rf.term = reply.Term
		rf.persist()
		succeedForward = false
	case !reply.Success:
		pe.nextIndex = reply.Conflict.Index
		i := (rf.snapshotTail.Index + 1)
		for ; i <= rf.tailLogInfo.Index; i++ {
			if rf.logs[i-(rf.snapshotTail.Index+1)].Info.Term > reply.Conflict.Term {
				break
			}
		}
		if reply.Conflict.Term >= 0 && i < reply.Conflict.Index {
			pe.nextIndex = i
		}

		rf.mu.Unlock()
		//go rf.requestForwardEntries(server, true)
		rf.countBuffer <- 0
		return stillLeader, succeedForward, succeedIndex
	default:
		if args.IsSnapshot {
			if pe.nextIndex < rf.snapshotTail.Index+1 {
				pe.nextIndex = rf.snapshotTail.Index + 1
			}
		} else {
			pe.nextIndex = pe.nextIndex + len(args.Entries)
			for _, v := range args.Entries {
				if v.Info.Term == rf.term {
					rf.copyCount[v.Info.Index]++
				}
			}
		}
	}

	succeedIndex = pe.nextIndex - 1
	//rf.persist()
	rf.mu.Unlock()

	return stillLeader, succeedForward, succeedIndex
}

func (rf *Raft) scanCommitableOnce() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}
	//defer rf.persist()
	for i, v := range rf.copyCount {
		switch {
		case i-(rf.snapshotTail.Index+1) < 0 || rf.logs[i-(rf.snapshotTail.Index+1)].Info.Term != rf.term:

		case v > len(rf.peers)/2 && rf.CommitIndex < i:
			rf.CommitIndex = i
		default:
		}
	}
}

func (rf *Raft) scanCommitable() {
	for rf.killed() == false {
		rf.scanCommitableOnce()
		ms := 10 //(500 + (rand.Int63() % 300)) /20
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) beep() {
	for rf.killed() == false {

		ms := 500 //(500 + (rand.Int63() % 300)) /20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		////////fmt.Println("Beep from:",rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) finalCommit() {
	rf.mu.Lock()
	for rf.killed() == false {
		var msg ApplyMsg
		switch {
		case rf.snapshotTail.Index > rf.LastApplied:
			msg = ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: rf.snapshot, SnapshotIndex: rf.snapshotTail.OuterIndex, SnapshotTerm: rf.snapshotTail.Term}
			rf.LastApplied = rf.snapshotTail.Index
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		case rf.CommitIndex == rf.LastApplied:
			//rf.persist()
			rf.mu.Unlock()
			//ms := 10//20 //(500 + (rand.Int63() % 300)) /20
			ms := 5
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.mu.Lock()
		default:
			rf.LastApplied++
			l := rf.logs[rf.LastApplied-(rf.snapshotTail.Index+1)]
			if !rf.logs[rf.LastApplied-(rf.snapshotTail.Index+1)].IsNOP {
				msg = ApplyMsg{CommandValid: true, Command: l.Command, CommandIndex: l.Info.OuterIndex}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			} else {
				msg = ApplyMsg{CommandValid: true, CommandIndex: rf.LastApplied}
			}
		}
	}
	//rf.persist()
	rf.mu.Unlock()

}

func (rf *Raft) batching() {
	i := 0
	for rf.killed() == false {
		_ = <-rf.countBuffer
		i++
		if i == bufferSize-1 {
			i = 0
			rf.forwardCh <- 0
		}
	}
}

func (rf *Raft) singlePack() {
	for rf.killed() == false {
		_ = <-rf.countBuffer
		rf.forwardCh <- 0
		ms := 3
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
