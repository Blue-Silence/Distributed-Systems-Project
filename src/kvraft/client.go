package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu      sync.Mutex

	clienrId      int64
	rpcSeq        int64
	currentLeader int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clienrId = nrand()
	ck.rpcSeq = 1

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	re := ""

	ck.mu.Lock()
	defer ck.mu.Unlock()

	args.Id.ClientId = ck.clienrId
	args.Id.RpcSeq = ck.rpcSeq
	ck.rpcSeq++

	ct := 1
	for {
		fmt.Println("Get index:", ct, " Sending:", args)

		var reply GetReply
		//log.Println("Client:", args)

		args.Server = ck.currentLeader
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &args, &reply)
		fmt.Println("Get index:", ct, " Sending:", args, "  after call")
		ct++
		if !ok || reply.Err != Err("") {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		} else {
			//log.Println("Client:", args)
			re = reply.Value
			break
		}
	}

	return re
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	var args PutAppendArgs
	args.Key = key
	args.Op = op
	args.Value = value

	ck.mu.Lock()
	defer ck.mu.Unlock()
	args.Id.ClientId = ck.clienrId
	args.Id.RpcSeq = ck.rpcSeq
	ck.rpcSeq++

	ct := 1
	for {
		fmt.Println("PA index:", ct, " Sending:", args)

		var reply PutAppendReply
		args.Server = ck.currentLeader
		//fmt.Println("Args:", args)
		ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &args, &reply)
		fmt.Println("PA index:", ct, " Sending:", args, "  after call")
		ct++

		if !ok || reply.Err != Err("") {
			//fmt.Println("Emm?")
			/*if !ok {
				fmt.Println("Timeout?")
			} else {
				fmt.Println(reply.Err, "  index:", ck.currentLeader)
			}*/
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		} else {
			//fmt.Println("Client:", args)
			break
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
