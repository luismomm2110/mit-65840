package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key           string
	Value         string
	OperationType OperationType // "Put" or "Append"
	RequestId     int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	RequestId int64

	// You'll have to add definitions here.
	ClientId int
}

type GetReply struct {
	Err   Err
	Value string
}
