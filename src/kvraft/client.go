package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId      int64
	requestId     int64 // monotonically increasing
	currentLeader int
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.currentLeader = 0
	DPrintf("clientID %v", ck.clientId)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
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
		// todo aqui está 0 chumbado apenas
		ok = ck.servers[0].Call("KVServer."+"Get", &args, &reply)
		if reply.Err == OK {
			ok = true
		}
		if reply.Err == ErrNoKey {
			ok = false
			return ""
		}
		if reply.Err == ErrWrongLeader {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			ok = false
		}
	}
	return reply.Value
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
	ck.requestId += 1
	reply := PutAppendReply{}
	ok := false
	for !ok {
		args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, RequestId: ck.requestId}
		//DPrintf("PutAppend key: %v, value: %v , requestId: %v, clientID %v", key, value, ck.requestId, ck.clientId)
		ok = ck.servers[0].Call("KVServer."+"Append", &args, &reply)
		err := reply.Err
		if err == OK {
			ok = true
		} else if err == ErrWrongLeader {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			ok = false
		}
	}
	//DPrintf("append reply: %v, clientID %v, requestId %v", reply.Value, ck.clientId, ck.requestId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
