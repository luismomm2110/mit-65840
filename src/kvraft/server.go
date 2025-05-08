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

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// enum for operation type
type OperationType int

const (
	PutOp    OperationType = 1
	AppendOp OperationType = 2
	GetOp    OperationType = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// should enter an 0p in Raft Log using start and describes a Put/Append/Get operation

	Type      OperationType // operation type
	Key       string        // key
	Value     string        //key
	ClientId  int
	RequestId int64
}

func (op Op) String() string {
	return fmt.Sprintf(
		"Op{Type: %s, Key: %q, Value: %q, ClientId: %d, RequestId: %d}",
		translateOperationType(op.Type), op.Key, op.Value, op.ClientId, op.RequestId,
	)
}

// Helper function to translate OperationType to string
func translateOperationType(opType OperationType) string {
	switch opType {
	case PutOp:
		return "PutOp"
	case AppendOp:
		return "AppendOp"
	case GetOp:
		return "GetOp"
	default:
		return "UnknownOp"
	}
}

type Snapshot struct {
	Values                map[string]string
	CompletedRequestsById map[int]RequestInfo
	LastSeenIndex         int
}

type RequestInfo struct {
	RequestId     int64
	CommitedIndex int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	values                map[string]string
	operations            map[int]Op
	channels              map[int]chan Op
	completedRequestsById map[int]RequestInfo
	cond                  *sync.Cond
	stopCh                chan struct{}
	LastSeenIndex         int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      GetOp,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	// index, term, isLeader
	kv.mu.Lock()
	DPrintf("Server %d Get client %d requestId %d key %s", kv.me, args.ClientId, args.RequestId, args.Key)
	if requestInfo, ok := kv.completedRequestsById[args.ClientId]; ok {
		if args.RequestId <= requestInfo.RequestId {
			DPrintf("Server %d client %d requestId %d already completed key %v",
				kv.me, args.ClientId, args.RequestId, args.Key)
			reply.Err = OK
			reply.Value = kv.values[args.Key]
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	receivedIndex, _, leader := kv.rf.Start(op)
	if !leader {
		DPrintf("Server %d Get client %d requestId %d key %s reply ErrWrongLeader", kv.me, args.ClientId, args.RequestId, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d Get client %d requestId %d key %s", kv.me, args.ClientId, args.RequestId, args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		if lastRequestInfo, ok := kv.completedRequestsById[args.ClientId]; ok {
			if lastRequestInfo.CommitedIndex != receivedIndex {
				reply.Err = ErrWrongLeader
				return
			}
			reply.Value = kv.values[args.Key]
			DPrintf("SERVER %d GET client %d requestId %d key %s value %s reply ok", kv.me, args.ClientId, args.RequestId, args.Key, reply.Value)
			reply.Err = OK
			return
		}
		kv.cond.Wait()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.OperationType,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	kv.mu.Lock()
	DPrintf("Server %d PutAppend client %d requestId %d key %s value %s", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)

	if lastRequestInfo, ok := kv.completedRequestsById[args.ClientId]; ok {
		if args.RequestId <= lastRequestInfo.RequestId {
			DPrintf("Server %d PutAppend client %d requestId %d already completed key %v",
				kv.me, args.ClientId, args.RequestId, args.Key)
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	receivedIndex, _, leader := kv.rf.Start(op)
	if !leader {
		DPrintf("Server %d PutAppend client %d requestId %d key %s reply ErrWrongLeader", kv.me, args.ClientId, args.RequestId, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d PutAppend client %d requestId %d key %s value %s", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		if requestInfo, ok := kv.completedRequestsById[args.ClientId]; ok {
			commitedIndex := requestInfo.CommitedIndex
			if commitedIndex != receivedIndex {
				DPrintf("Server %d PutAppend client %d requestId %d key %s value %s reply ErrWrongLeader", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)
				reply.Err = ErrWrongLeader
				return
			}
			DPrintf("Server %d PutAppend client %d requestId %d key %s value %s reply ok", kv.me, args.ClientId, args.RequestId, args.Key, args.Value)
			reply.Err = OK
			return
		}
		kv.cond.Wait()
	}
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
	kv.dead = 1
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
	labgob.Register(RequestInfo{})
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.mu = sync.Mutex{}
	kv.maxraftstate = maxraftstate
	kv.dead = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.values = make(map[string]string)
	kv.operations = make(map[int]Op)
	kv.completedRequestsById = make(map[int]RequestInfo)
	kv.cond = sync.NewCond(&kv.mu)
	state := kv.rf.ReadSnapshot()
	if len(state) > 0 {
		DPrintf("Server %d restarting from snapshot", kv.me)
		kv.mu.Lock()
		kv.syncWithSnapshot(state)
		kv.mu.Unlock()
	}

	// go routine to apply operations from Raft log to KV store
	go kv.applyOp()

	return kv
}

// lock must be held
func (kv *KVServer) syncWithSnapshot(state []byte) {
	var snapshot Snapshot
	buffer := bytes.NewBuffer(state)
	decoder := labgob.NewDecoder(buffer)
	err := decoder.Decode(&snapshot)
	if err != nil {
		log.Fatalf("Server %d failed to decode snapshot", kv.me)
	}
	kv.values = snapshot.Values
	kv.completedRequestsById = snapshot.CompletedRequestsById
	kv.LastSeenIndex = snapshot.LastSeenIndex
	kv.PrintSnapshot(snapshot)
	DPrintf("server %d snapshot sync completed", kv.me)
}

func (kv *KVServer) PrintSnapshot(snapshot Snapshot) {
	DPrintf("Server %d printing snapshot with last seen index %d", kv.me, kv.LastSeenIndex)
	var formattedValues string
	for key, value := range snapshot.Values {
		formattedValues += fmt.Sprintf("\n\tKey: %q, Value: %q", key, value)
	}

	// Print the formatted snapshot
	DPrintf("Server %d snapshot %s", kv.me, formattedValues)
}

func (kv *KVServer) applyOp() {
	if kv.killed() {
		return
	}
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.SnapshotValid {
			DPrintf("Server %d received snapshot %d in state machine", kv.me, msg.SnapshotIndex)
			kv.syncWithSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}

		index := msg.CommandIndex
		op := msg.Command.(Op)
		if index <= kv.LastSeenIndex {
			DPrintf("Server %d index %d already seen", kv.me, index)
			kv.mu.Unlock()
			continue
		}
		kv.LastSeenIndex = index

		DPrintf("Server %d applying op %v at index %d", kv.me, op, index)
		apply := true
		if _, ok := kv.completedRequestsById[op.ClientId]; ok {
			if op.RequestId <= kv.completedRequestsById[op.ClientId].RequestId {
				DPrintf("Server %d client %d requestId %d already completed key %v index %v", kv.me, op.ClientId, op.RequestId, op.Key, index)
				apply = false
			}
		}

		if apply {
			switch op.Type {
			case PutOp:
				kv.values[op.Key] = op.Value
			case AppendOp:
				kv.values[op.Key] += op.Value
			}
			DPrintf("Server %d applied op %v at index %d with key %v", kv.me, op, index, kv.values[op.Key])
			lastRequestId := kv.completedRequestsById[op.ClientId].RequestId
			if op.RequestId > lastRequestId {
				kv.completedRequestsById[op.ClientId] = RequestInfo{
					RequestId:     op.RequestId,
					CommitedIndex: msg.CommandIndex,
				}

			}
		}

		kv.snapshotRaftState()
		kv.mu.Unlock()
		kv.cond.Broadcast()
	}
}

func (kv *KVServer) snapshotRaftState() {
	if kv.killed() {
		return

	}
	if kv.maxraftstate == -1 {
		return
	}
	// compare with maxraftstate
	snapshotSize := kv.rf.GetSize()
	if snapshotSize >= kv.maxraftstate {
		// sent a snapshot to raft unit
		DPrintf("Server %d making snapshot with last seen index %d", kv.me, kv.LastSeenIndex)
		snapshot := Snapshot{
			Values:                kv.values,
			CompletedRequestsById: kv.completedRequestsById,
			LastSeenIndex:         kv.LastSeenIndex,
		}
		var buffer bytes.Buffer
		encoder := labgob.NewEncoder(&buffer)
		err := encoder.Encode(snapshot)
		if err != nil {
			log.Fatalf("Server %d failed to encode snapshot", kv.me)
		}
		kv.rf.Snapshot(kv.LastSeenIndex, buffer.Bytes())
	}
}
