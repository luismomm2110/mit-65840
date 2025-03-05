package kvraft

import "strconv"

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

// generate to string for PutAppendArgs
func (args PutAppendArgs) String() string {
	return "PutAppendArgs{key " + args.Key + " value, " + args.Value + "op, " + strconv.Itoa(int(args.OperationType)) + " RequestId, " + strconv.FormatInt(args.RequestId, 10) + " ClientId, " + strconv.Itoa(args.ClientId) + "}"
}

type PutAppendReply struct {
	Err Err
}

// generate to string for PutAppendReply
func (reply PutAppendReply) String() string {
	return "PutAppendReply{" + string(reply.Err) + "}"
}

type GetArgs struct {
	Key       string
	RequestId int64

	// You'll have to add definitions here.
	ClientId int
}

// generate to string for GetArgs
func (args GetArgs) String() string {
	return "GetArgs{key " + args.Key + "RequestId, " + strconv.FormatInt(args.RequestId, 10) + "ClientId, " + strconv.Itoa(args.ClientId) + " }"
}

type GetReply struct {
	Err   Err
	Value string
}

// generate to string for GetReply
func (reply GetReply) String() string {
	return "GetReply{" + string(reply.Err) + ", " + reply.Value + "}"
}
