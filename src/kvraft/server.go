package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	// set flag for miliseconds since epoch

	if Debug {
		log.Printf(format, a...)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}
	return
}

type OpType int

const (
	OpTypeGet OpType = iota
	OpTypePut
	OpTypeAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    OpType
	RequestId int64
	ClientId  int64
}

// translate op type to string for debugging
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

// String method for Op
func (op Op) String() string {
	return fmt.Sprintf("Op{Key: %s, Value: %s, OpType: %d, RequestId: %d, ClientId: %d}",
		op.Key, op.Value, op.OpType, op.RequestId, op.ClientId)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore                   map[string]string
	chanByRequestIdByClientId map[int64]map[int64]chan raft.ApplyMsg // ephemeral channels for each requestId by clientId
	lastRequestForClient      map[int64]int64                        // maps clientId to greatest requestId seen so far that we can deduplicate requests
	lastPersistedIndex        int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	lastRequest := kv.lastRequestForClient[args.ClientId]
	if args.RequestId <= lastRequest {
		reply.Err = OK
		reply.Value = kv.kvStore[args.Key] // return the value from the kvStore
		DPrintf("Server %d returning value %v for Get request alreadt completed %v", kv.me, reply.Value, args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("Server %d received Get request for key %s", kv.me, args)
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
	//clientChans[args.RequestId] = nil // remove the channel after use
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

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	lastRequest := kv.lastRequestForClient[args.ClientId]
	DPrintf("Server %d received Put request for key %s with requestId %d for client %d and last request is %d", kv.me, args.Key, args.RequestId, args.ClientId, lastRequest)
	if args.RequestId <= lastRequest {
		DPrintf("Server %d ignoring Put request with old requestId %d for client %d", kv.me, args.RequestId, args.ClientId)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("Server %d received Put request for key %s", kv.me, args)
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    OpTypePut,
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
	//clientChans[args.RequestId] = nil // remove the channel after use
	kv.mu.Unlock()
	DPrintf("Server %d waiting put for request %v", kv.me, args)
	<-c
	DPrintf("Server %d received put request %v from channel %v", kv.me, args, c)
	reply.Err = OK
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	lastRequest := kv.lastRequestForClient[args.ClientId]
	if args.RequestId <= lastRequest {
		DPrintf("Server %d ignoring Append request with old requestId %d for client %d\n key is %v", kv.me, args.RequestId, args.ClientId, kv.kvStore[args.Key])
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	DPrintf("Server %d received Append request for key %s", kv.me, args)
	kv.mu.Unlock()
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    OpTypeAppend,
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
	DPrintf("Server %d Append request with old requestId %d for client %d\n key is %v", kv.me, args.RequestId, args.ClientId, kv.kvStore[args.Key])
	kv.mu.Unlock()
	<-c
	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.chanByRequestIdByClientId = make(map[int64]map[int64]chan raft.ApplyMsg) // maps requestId to channels by clientId
	kv.kvStore = make(map[string]string)
	kv.lastRequestForClient = make(map[int64]int64)
	data := kv.rf.ReadSnapshot()
	kv.restoreFromSnapshot(data)

	// You may need initialization code here.
	go kv.apply()

	return kv
}

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh
		if !msg.CommandValid {
			kv.mu.Lock()
			DPrintf("Server %d received snapshot %v", kv.me, msg.Command)
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Lock()
		DPrintf("Server %d received message %v", kv.me, msg)
		op := msg.Command.(Op)
		clientId := op.ClientId
		lastRequest := kv.lastRequestForClient[clientId]
		DPrintf("Server %d processing op %v for client %d, lastRequest %v", kv.me, op, clientId, lastRequest)
		if op.RequestId <= lastRequest {
			DPrintf("Server %d ignoring old requestId %d for client %d", kv.me, op.RequestId, clientId)
			kv.mu.Unlock()
			continue
		} else {
			if op.OpType == OpTypePut {
				kv.kvStore[op.Key] = op.Value
			} else if op.OpType == OpTypeAppend {
				kv.kvStore[op.Key] += op.Value
				DPrintf("Server %d value for key %v updated to %v", kv.me, op.Key, kv.kvStore[op.Key])
			}
			kv.lastRequestForClient[clientId] = op.RequestId
			DPrintf("Server %d lastRequestForClient updated for client %d: %v", kv.me, clientId, op)
		}
		c, exists := kv.chanByRequestIdByClientId[op.ClientId][op.RequestId]
		if exists {
			c <- msg
			DPrintf("Server %d sent message to channel for client %d and requestId %d", kv.me, clientId, op.RequestId)
		} else {
			DPrintf("Server %d no channel found for client %d and requestId %d", kv.me, clientId, op.RequestId)
		}
		index := msg.CommandIndex
		kv.snapshot(index)
		kv.mu.Unlock()
	}
}

type Snapshot struct {
	Values               map[string]string
	LastRequestForClient map[int64]int64
}

func (kv *KVServer) snapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	size := kv.rf.GetSize()
	DPrintf("Server %d received snapshot index %v  current index %v", kv.me, index, kv.lastPersistedIndex)
	if index <= kv.lastPersistedIndex {
		return
	}
	snapshot := Snapshot{
		Values:               kv.kvStore,
		LastRequestForClient: kv.lastRequestForClient,
	}

	kv.lastPersistedIndex = index
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(snapshot)
	if size > kv.maxraftstate {
		DPrintf("Server %d making snapshot with last persisted index %d and kvValue is %v", kv.me, kv.lastPersistedIndex, kv.kvStore)
		kv.rf.Snapshot(kv.lastPersistedIndex, buffer.Bytes())
	}
}

func (kv *KVServer) restoreFromSnapshot(data []byte) {
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
	DPrintf("Server %d restoreFromSnapshot with last request for client %v", kv.me, snapshot.LastRequestForClient)
	DPrintf("Server %d restoreFromSnapshot with values %v", kv.me, snapshot.Values)
	kv.lastRequestForClient = snapshot.LastRequestForClient
}
