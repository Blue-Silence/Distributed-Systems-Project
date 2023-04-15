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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"fmt"
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
	follower int 	= 0
	candidate 		= 1
	leader			= 2
)


type LogInfo struct {
	Term int 
	Index int
}

type Log struct {
	Command interface{}
	Info LogInfo
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// 2A
	leaderID int 
	term int
	//termVoted int
	voteFor *int
	state int
	//isLeader bool 
	recvHeartbeat bool

	//2B
	tailLogInfo LogInfo
	CommitIndex int 
	LastApplied int
	logs []Log 
	copyCount map[int]int
	nextIndex []int
	applyCh chan ApplyMsg
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here (2A).

	//rf.mkHeartBeat()
	rf.mu.Lock()
	term = rf.term
	if(rf.state == leader){
		isleader = true
	}
	//isleader = rf.isLeader
	rf.mu.Unlock()

	////fmt.Println("GetState term:", term, " state:", rf.state , "  ID: ", rf.me)
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	From int
	TailLogInfo LogInfo
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool 
	Term int
}



// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//////fmt.Println("Enter vote.ID:", rf.me)
	rf.mu.Lock()
	//////fmt.Println("Exit vote.ID:", rf.me)
	reply.Term = args.Term
	switch {
		case args.Term>rf.term :
			fmt.Println(" Become follower for A from ",args.From, " on ID:", rf.me, " in term:",args.Term)
			rf.state = follower
			rf.voteFor = nil
			rf.term = args.Term 
			rf.voteFor = &args.From 
			reply.VoteGranted = true
			////fmt.Println("Vote for:",args.From, " From ID:", rf.me, " in term:",args.Term)
		case args.Term == rf.term && rf.voteFor == nil :
			fmt.Println(" Become follower for B from ",args.From, " on ID:", rf.me, " in term:",args.Term)
			rf.voteFor = &args.From 
			reply.VoteGranted = true 
			////fmt.Println("Vote for:",args.From, " From ID:", rf.me, " in term:",args.Term)
			rf.state = follower
		default :
			reply.Term = rf.term
			reply.VoteGranted = false
			////fmt.Println("Refuse to vote for:",args.From, " From ID:", rf.me, " for term:",args.Term, "   self-term:",rf.term)
			if(rf.term == args.Term){
				////fmt.Println("Already vote for:",*rf.voteFor)
			}
	}

	if !(args.TailLogInfo.Term > rf.tailLogInfo.Term || ((args.TailLogInfo.Term == rf.tailLogInfo.Term) && (args.TailLogInfo.Index >= rf.tailLogInfo.Index))) {
		reply.VoteGranted = false
	}  

	if (reply.VoteGranted) {
		rf.recvHeartbeat = true
	}
	
	rf.mu.Unlock()
	
}

type AppendEntriesArgs struct {
	LeaderId int
	Term int

	Entries []Log
	PrevLog LogInfo
	CommitIndex int

	From int
}

type AppendEntriesReply struct {
	Term int
	Success bool 
	From int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.From = rf.me
	reply.Success = true
	rf.mu.Lock()
	//fmt.Println("(APPEND) term",rf.term," from:",args.From," in ID:",rf.me)
	defer rf.mu.Unlock()
	if(args.Term>=rf.term) {
		//if(args.Term>rf.term){
			////fmt.Println("Term change (APPEND) from:",rf.term," to:",args.Term," in ID:",rf.me)
		//}
		
		if(args.Term > rf.term){
			rf.voteFor = nil
			rf.term = args.Term
		}
		
		rf.leaderID = args.LeaderId
		rf.recvHeartbeat = true
		//if(rf.state == leader) {
			////fmt.Println("Leadersgio clear : B", rf.term, "ID: ", rf.me , " newTerm:",args.Term, "  From:",args.From)
		//}
		rf.term = args.Term
		if(rf.state == leader){
			fmt.Println(" Become follower for C from ",reply.From, " on ID:", rf.me, " in term:",args.Term)
		}
		rf.state = follower
		
	} else {
		reply.Success = false
		reply.Term = rf.term
		return 
	}

	//If the leader is stale,won't got below.
	//fmt.Println("(APPEND) HERE WE ARE! term",rf.term," from:",args.Term," in ID:",rf.me)
	switch {
		case args.PrevLog.Index > rf.tailLogInfo.Index :
			reply.Success = false
			return 
		case rf.logs[args.PrevLog.Index].Info != args.PrevLog :
			reply.Success = false
			return 
		default :
			for _,v := range args.Entries {
				fmt.Println("Append  ",v, " on ID:",rf.me)
				switch {
					case v.Info.Index == len(rf.logs) :
						rf.logs = append(rf.logs, v)
					case v.Info.Index < len(rf.logs) :
					rf.logs[v.Info.Index] = v
					default :
						//fmt.Println("Warning.Inconsistency between taillog and log[].")
				}
				rf.tailLogInfo = v.Info
			}
			if(rf.tailLogInfo.Index >= args.CommitIndex && rf.CommitIndex < args.CommitIndex) {
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int,args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	if rf.state != leader {
		isLeader = false 
	} else {
		rf.tailLogInfo.Index ++ 
		rf.tailLogInfo.Term = rf.term
		rf.logs = append(rf.logs, Log{command, rf.tailLogInfo})
		term = rf.term
		index = rf.tailLogInfo.Index
		rf.copyCount[index] = 1
		fmt.Println("Adding entry:",Log{command, rf.tailLogInfo},"  on Leader:",rf.me)
	}
	
	rf.mu.Unlock()
	// Your code here (2B).


	return index, term, isLeader
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
		rf.mu.Unlock()

		//maxTerm := 0

		if (!f && state == follower) {
			rf.mu.Lock()
			rf.state = candidate
			rf.term ++
			fmt.Println("Timer expired!", "ID: ", rf.me, "  term:", rf.term)
			term := rf.term
			rf.voteFor = &rf.me
			state := rf.state
			logInfo := rf.tailLogInfo
			rf.mu.Unlock()
			
			num := 0
			voted := 1
			for i,_ := range peers {
				rf.mu.Lock()
				state = rf.state
				rf.mu.Unlock()
				if(state!=candidate) {
					break
				}
				num ++
				if(i == me) {
					continue
				}
				go func(i int) {
					args := RequestVoteArgs{Term : term,  From : rf.me, TailLogInfo : logInfo}
					reply := RequestVoteReply{}
					rf.sendRequestVote(i, &args, &reply)
					rf.mu.Lock()
					if (reply.VoteGranted) {
						voted ++
					} else {
						
						if(reply.Term>rf.term){
							rf.state = follower
							fmt.Println(" Become follower for D  on ID:", rf.me, " in term:",reply.Term)
							////fmt.Println("Term change (GETVOTE) from:",rf.term," to:",reply.Term," in ID:",rf.me)
							rf.term = reply.Term
						}
						
					}
					rf.mu.Unlock()
				}(i)
				
					//maxTerm = reply.Term	
			}
			//ms := (100 + (rand.Int63() % 100))
			ms := 10
			for i := 0;i<10;i++{
				rf.mu.Lock()
				if(voted > num/2 || rf.state != candidate) {
					rf.mu.Unlock()
					////fmt.Println("Take ", ms*(i+1), "ms")
					break
					
				}
				rf.mu.Unlock()
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
			

			rf.mu.Lock()
			if (voted > num/2 && rf.state == candidate) {
				rf.voteFor = &rf.me
				rf.term = term
				rf.state = leader 
				fmt.Println("Election succeeded : ", term, "ID: ", rf.me)
				rf.mu.Unlock()
				//rf.mkHeartBeat()
			} else {
				fmt.Println("Election failed : ", term, "ID: ", rf.me)
				//rf.term = maxTerm
				fmt.Println(" Become follower for E ", " on ID:", rf.me)
				rf.state = follower
				rf.mu.Unlock()
				ms := (500 + (rand.Int63() % 300))
				time.Sleep(time.Duration(ms) * time.Millisecond)

			}
			
		} else {
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := (500 + (rand.Int63() % 300))
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		
		
	}
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {

		rf.mkHeartBeat()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := (50 + (rand.Int63() % 300))
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) mkHeartBeat() {

		rf.mu.Lock()
		//term := rf.term
		state := rf.state
		peers := rf.peers
		me := rf.me
		rf.mu.Unlock()

		if (state == leader) {
			
			for i,_ := range peers {

				if(i == me) {
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.term = 0
	rf.voteFor = nil
	//rf.termVoted = 0
	rf.leaderID = 0
	//rf.isLeader = false
	rf.state = follower
	rf.recvHeartbeat = false

	rf.tailLogInfo.Term = 0 
	rf.tailLogInfo.Index = 0
	// Your initialization code here (2A, 2B, 2C).

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.logs = []Log{Log{Info : LogInfo{0,0}}}
	rf.copyCount = make(map[int]int)
	rf.applyCh = applyCh
	rf.nextIndex = []int{}
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 1)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	go rf.scanCommitable()
	go rf.finalCommit()

	//go rf.beep()

	return rf
}


func (rf *Raft) requestForwardEntries(server int) (bool, bool, int) {
	stillLeader := true 
	succeedForward := true 
	succeedIndex := 0

	rf.mu.Lock() 
	//defer rf.mu.Unlock()
	term := rf.term
	if(rf.state != leader) {
		return false, false, -1
	}

	reply := AppendEntriesReply{}
	
	nextIndex := rf.nextIndex[server]

	args := AppendEntriesArgs{LeaderId : rf.me, Term : rf.term, PrevLog : rf.logs[nextIndex-1].Info, CommitIndex : rf.CommitIndex, From : rf.me, Entries : []Log{}}
	if(nextIndex<=rf.tailLogInfo.Index) {
		args.Entries = append(args.Entries, rf.logs[nextIndex])
		succeedIndex = nextIndex
	} else {
		succeedIndex = rf.tailLogInfo.Index
	}

	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, &args, &reply)
	//fmt.Println("Sending out commitIndex:",args.CommitIndex, " from ID:", rf.me, " Carrying:",args, " nextIndex:", nextIndex, "  rf.tailLogInfo.Index",rf.tailLogInfo.Index)
	//rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	switch {
		case !ok :
			succeedForward = false 
		case term != rf.term :
			rf.mu.Unlock()
			return false, false, -1
		case rf.term < reply.Term :
			stillLeader = false 
			rf.state = follower
			fmt.Println(" Become follower for F from ",reply.From, " on ID:", rf.me, " in term:",reply.Term)
			//fmt.Println("Leader term change on ID:",rf.me, " from:",rf.term , "  to:", reply.Term)
			rf.term = reply.Term
			succeedForward = false 
		case !reply.Success :
			rf.nextIndex[server]--
			succeedForward = false 
		default :
			rf.nextIndex[server] = succeedIndex+1

			for _,v := range args.Entries {
				if (v.Info.Term == rf.term) {
					rf.copyCount[v.Info.Index] ++
				}
			}
	}
	rf.mu.Unlock()

	return stillLeader, succeedForward, succeedIndex
}

func (rf *Raft) scanCommitableOnce() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i,v := range rf.copyCount {
		switch {
			case rf.logs[i].Info.Term != rf.term :
				delete(rf.copyCount, i)
			case v > len(rf.peers)/2 && rf.CommitIndex<i:
				rf.CommitIndex = i 
			default :
		}
	}
}

func (rf *Raft) scanCommitable() {
	for {
		rf.scanCommitableOnce()
		ms := (500 + (rand.Int63() % 300))
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
} 

func (rf *Raft) finalCommit() {
	rf.mu.Lock()
	for {
		
		////fmt.Println("ON:",rf.me, " CommitIndex:",rf.CommitIndex, " Lastapp:",rf.LastApplied)
		if (rf.CommitIndex == rf.LastApplied) {
			rf.mu.Unlock()
			ms := (500 + (rand.Int63() % 300))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.mu.Lock()
		} else {
			rf.LastApplied ++
			msg := ApplyMsg{CommandValid : true, Command : rf.logs[rf.LastApplied].Command, CommandIndex : rf.LastApplied}
			fmt.Println("Apply:",msg, "ON:",rf.me, " CommitIndex:",rf.CommitIndex, " Lastapp:",rf.LastApplied, "  len:",len(rf.logs))
			//fmt.Println("  on ID:",rf.me)
			
			rf.applyCh<-msg
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) beep() {
	for {
		rf.mu.Lock()
		fmt.Println("BEEP FROM ID:",rf.me)
			ms := (500 + (rand.Int63() % 300))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.mu.Unlock()
	}
	
}