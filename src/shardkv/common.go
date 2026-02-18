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
	ErrNotReady    = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId  int64
	Op        string // "Put" or "Append"
	ShardId   int
}

func (op *PutAppendArgs) String() string {
	return op.Op + "(" + op.Key + "," + op.Value + ")"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	RequestId int64
	ClientId  int64
	ShardId   int
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	Err                  Err
	ConfigId             int
	ShardId              int
	Values               map[string]string
	LastRequestForClient map[int64]int64 // clientId -> lastRequestId for this shard
}

type MoveShardsReply struct {
	Err Err
}

type RequestShardArgs struct {
	ConfigId     int // which config transition we're requesting for
	ShardId      int // which shard to request
	RequestorGid int // gid of the group requesting the shard
}

type RequestShardReply struct {
	Err                  Err               // OK, ErrNotReady, ErrWrongGroup
	Values               map[string]string // KV data for this shard
	LastRequestForClient map[int64]int64   // duplicate detection state
}
