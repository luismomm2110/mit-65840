package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader           int
	me               int64
	currentRequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.me = int64(nrand())
	DPrintf("Client %d created", ck.me)
	ck.servers = servers
	// You'll have to add code here.
	nBig, _ := rand.Int(rand.Reader, big.NewInt(int64(len(servers))))
	ck.leader = int(nBig.Int64() + 1)
	atomic.StoreInt64(&ck.currentRequestId, 0)
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
	requestId := atomic.AddInt64(&ck.currentRequestId, 1)
	ck.currentRequestId = requestId
	args := GetArgs{
		Key:       key,
		RequestId: requestId,
		ClientId:  ck.me,
	}
	numServers := len(ck.servers)
	start := ck.leader
	for {
		for offset := 0; offset < numServers; offset++ {
			serverIndex := (start + offset) % numServers
			reply := GetReply{}
			ok := make(chan bool)
			DPrintf("Client %d SENDING GET %v to server %v", ck.me, args, serverIndex)
			go func() {
				ok <- ck.servers[serverIndex].Call("KVServer.Get", &args, &reply)
			}()
			select {
			case <-ok:
				{
					if reply.Err == OK {
						ck.leader = serverIndex
						DPrintf("Client %d GET REQUESTID %d COMPLETED and leader is %v", ck.me, args.RequestId, serverIndex)
						return reply.Value
					} else if reply.Err == ErrWrongLeader {
						//DPrintf("found another leader in response from request %v server id %v", args, serverIndex)
					}
				}
			case <-time.After(20 * time.Millisecond):
				{
					//DPrintf("Client %d Get key %v value from server %d timeout", ck.me, key, serverIndex)
					continue
				}
			}
		}
	}
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
	requestId := atomic.AddInt64(&ck.currentRequestId, 1)
	ck.currentRequestId = requestId
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		RequestId: requestId,
		ClientId:  ck.me,
	}

	numServers := len(ck.servers)
	start := ck.leader
	for {
		for offset := 0; offset < numServers; offset++ {
			serverIndex := (start + offset) % numServers
			reply := PutAppendReply{}
			ok := make(chan bool)
			var method string
			if op == "Put" {
				method = "KVServer.Put"
				DPrintf("Client %d SENDING Put %v to server %v", ck.me, args, serverIndex)
			} else {
				DPrintf("Client %d SENDING Append %v to server %v", ck.me, args, serverIndex)
				method = "KVServer.Append"
			}
			go func() {
				ok <- ck.servers[serverIndex].Call(method, &args, &reply)
			}()
			select {
			case ok := <-ok:
				if ok {
					if reply.Err == OK {
						DPrintf("Client %d PUTAPPEND REQUESTID %d COMPLETED and leader is %v", ck.me, args.RequestId, serverIndex)
						ck.leader = serverIndex
						return
					} else if reply.Err == ErrWrongLeader {
						//DPrintf("found another leader in response from request %v server id %v", args, serverIndex)
					}
				}
			case <-time.After(20 * time.Millisecond):
				{
					//DPrintf("Client %d PutAppend key %v value %v to server %d timeout", ck.me, key, value, serverIndex)
					continue
				}
			}
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
