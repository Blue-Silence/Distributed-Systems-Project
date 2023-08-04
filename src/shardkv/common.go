package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id RpcId

	Server int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id RpcId

	Server int
}

type GetReply struct {
	Err   Err
	Value string
}

type RpcId struct {
	ClientId int64
	RpcSeq   int64
}

type RetriveShardArgs struct {
	GenNum   int
	ShardNum int
}

type RetriveShardReply struct {
	Shard ShardState
	Valid bool
}

type CheckOwnerArgs struct {
	ShardNum int
}

type CheckOwnerReply struct {
	GenNum int
}
