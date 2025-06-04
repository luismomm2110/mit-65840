package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

// String method for PutAppendArgs
func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs{Key: %s, Value: %s, ClientId: %d, RequestId: %d}",
		args.Key, args.Value, args.ClientId, args.RequestId)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("GetArgs{Key: %s, ClientId: %d, RequestId: %d}",
		args.Key, args.ClientId, args.RequestId)
}

type GetReply struct {
	Err   Err
	Value string
}
