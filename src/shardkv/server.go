package shardkv

import (
	"6.5840/labrpc"
	"bytes"
	"log"
	"sync/atomic"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	OpTypeGet OpType = iota
	OpTypePut
	OpTypeAppend
	OpTypePutAppend
)

type Snapshot struct {
	Values               map[string]string
	LastRequestForClient map[int64]int64
}

func (opType OpType) String() string {
	switch opType {
	case OpTypeGet:
		return "Get"
	case OpTypePut:
		return "Put"
	case OpTypeAppend:
		return "Append"
	default:
		return "Unknown"
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    OpType
	RequestId int64
	ClientId  int64

	// // specific fields for each operation

}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32

	// Your definitions here.
	kvStore                   map[string]string
	chanByRequestIdByClientId map[int64]map[int64]chan raft.ApplyMsg // ephemeral channels for each requestId by clientId
	lastRequestForClient      map[int64]int64                        // maps clientId to greatest requestId seen so far that we can deduplicate requests
	lastPersistedIndex        int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	lastRequest := kv.lastRequestForClient[args.ClientId]
	if args.RequestId <= lastRequest {
		reply.Err = OK
		reply.Value = kv.kvStore[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:       args.Key,
		OpType:    OpTypeGet,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	clientChans, ok := kv.chanByRequestIdByClientId[args.ClientId]
	if !ok {
		clientChans = make(map[int64]chan raft.ApplyMsg)
		kv.chanByRequestIdByClientId[args.ClientId] = clientChans
	}
	c, ok := clientChans[args.RequestId]
	if !ok {
		c = make(chan raft.ApplyMsg, 1)
		clientChans[args.RequestId] = c
	}
	DPrintf("Server %d waiting get for request %v", kv.me, args)
	kv.mu.Unlock()
	<-c
	DPrintf("Server %d received get request %v from channel %v", kv.me, args, c)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvStore[args.Key]
	DPrintf("Server %d returning value %v for Get request %v", kv.me, reply.Value, args)
	reply.Err = OK
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d received putappend request %v", kv.me, args)
	kv.mu.Lock()
	lastRequest := kv.lastRequestForClient[args.ClientId]
	if args.RequestId <= lastRequest {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	var opType OpType
	if args.Op == "Put" {
		opType = OpTypePut
	}
	if args.Op == "Append" {
		opType = OpTypeAppend
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    opType,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	clientChans, ok := kv.chanByRequestIdByClientId[args.ClientId]
	if !ok {
		clientChans = make(map[int64]chan raft.ApplyMsg)
		kv.chanByRequestIdByClientId[args.ClientId] = clientChans
	}
	c, ok := clientChans[args.RequestId]
	if !ok {
		c = make(chan raft.ApplyMsg, 1)
		clientChans[args.RequestId] = c
	}
	DPrintf("Server %d waiting putappend for request %v", kv.me, args)
	kv.mu.Unlock()
	<-c
	DPrintf("Server %d received putappend request %v from channel %v", kv.me, args, c)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.kvStore = make(map[string]string)
	kv.chanByRequestIdByClientId = make(map[int64]map[int64]chan raft.ApplyMsg)
	kv.lastRequestForClient = make(map[int64]int64)
	kv.lastPersistedIndex = 0
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

func (kv *ShardKV) apply() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh
		if !msg.CommandValid {
			kv.mu.Lock()
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Lock()
		op := msg.Command.(Op)
		clientId := op.ClientId
		lastRequest := kv.lastRequestForClient[clientId]
		if op.RequestId <= lastRequest {
			kv.mu.Unlock()
			continue
		}
		DPrintf("Server %d got command %v", kv.me, msg.Command)
		if op.OpType == OpTypePut {
			DPrintf("Server %d got put request %v", kv.me, msg.Command)
			kv.kvStore[op.Key] = op.Value
		} else if op.OpType == OpTypeAppend {
			kv.kvStore[op.Key] += op.Value
		}
		kv.lastRequestForClient[clientId] = op.RequestId
		c, exists := kv.chanByRequestIdByClientId[op.ClientId][op.RequestId]
		if exists {
			c <- msg
		}
		kv.snapshot(msg.CommandIndex)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) snapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	size := kv.rf.GetSize()
	if index <= kv.lastPersistedIndex {
		return
	}
	snapshot := Snapshot{
		Values:               kv.kvStore,
		LastRequestForClient: kv.lastRequestForClient,
	}
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(snapshot)
	if size > kv.maxraftstate {
		kv.rf.Snapshot(kv.lastPersistedIndex, buffer.Bytes())
	}

	kv.lastPersistedIndex = index
}

func (kv *ShardKV) restoreFromSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := labgob.NewDecoder(&buffer)
	var snapshot Snapshot
	err := decoder.Decode(&snapshot)
	if err != nil {
		log.Fatalf("Server %d restoreFromSnapshot error %v", kv.me, err)
	}
	kv.kvStore = snapshot.Values
	kv.lastRequestForClient = snapshot.LastRequestForClient
	DPrintf("Server %d restoreFromSnapshot with last request for client %v", kv.me, snapshot.LastRequestForClient)
	DPrintf("Server %d restoreFromSnapshot with values %v", kv.me, snapshot.Values)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}
