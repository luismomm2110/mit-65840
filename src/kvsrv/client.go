package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int64 // monotonically increasing
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestId += 1
	DPrintf("get key: %v, reequestId: %v, clientID %v", key, ck.requestId, ck.clientId)
	ok := false
	reply := GetReply{}
	for !ok {
		args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
		ok = ck.server.Call("KVServer."+"Get", &args, &reply)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	if op == "Append" {
		return ck.Append(key, value)
	}

	ck.Put(key, value)
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.requestId += 1
	DPrintf("put key: %v, value: %v , reequestId: %v, clientID %v", key, value, ck.requestId, ck.clientId)
	reply := PutAppendReply{}
	ok := false
	for !ok {
		args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, RequestId: ck.requestId}
		ok = ck.server.Call("KVServer."+"Put", &args, &reply)
	}
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	ck.requestId += 1
	reply := PutAppendReply{}
	ok := false
	for !ok {
		args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, RequestId: ck.requestId}
		DPrintf("append key: %v, value: %v , reequestId: %v, clientID %v", key, value, ck.requestId, ck.clientId)
		ok = ck.server.Call("KVServer."+"Append", &args, &reply)
	}
	DPrintf("append reply: %v, clientID %v, requestId %v", reply.Value, ck.clientId, ck.requestId)
	return reply.Value
}
