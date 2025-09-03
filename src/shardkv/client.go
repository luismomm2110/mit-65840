package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"
import "sync/atomic"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leader           int
	me               int64
	currentRequestId int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = int64(nrand())
	DPrintf("Client %d created", ck.me)
	ck.leader = 0
	atomic.StoreInt64(&ck.currentRequestId, 0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("ShardKV.Get", &args, &reply)
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

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			numServers := len(servers)
			start := ck.leader
			for offset := 0; offset < numServers; offset++ {
				serverIndex := (start + offset) % numServers
				reply := GetReply{}
				ok := make(chan bool)
				DPrintf("Client %d SENDING GET %v to server %v", ck.me, args, serverIndex)
				go func() {
					srv := ck.make_end(servers[serverIndex])
					ok <- srv.Call("ShardKV.Get", &args, &reply)
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
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("ShardKV.PutAppend", &args, &reply)
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
		Op:        op,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			numServers := len(servers)
			start := ck.leader
			for offset := 0; offset < numServers; offset++ {
				serverIndex := (start + offset) % numServers
				reply := PutAppendReply{}
				ok := make(chan bool)
				go func() {
					srv := ck.make_end(servers[serverIndex])
					ok <- srv.Call("ShardKV.PutAppend", &args, &reply)
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
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
	//DPrintf("append reply: %v, clientID %v, requestId %v", reply.Value, ck.clientId, ck.requestId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
