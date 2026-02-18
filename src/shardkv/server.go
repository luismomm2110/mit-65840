package shardkv

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		now := time.Now()
		prefix := now.Format("15:04:05.000000")
		log.Printf(prefix+" "+format, a...)
	}
	return
}

type OpType int

const (
	OpTypeGet OpType = iota
	OpTypePut
	OpTypeAppend
	OpTypeMoveShard
	OpTypeConfig
)

type Snapshot struct {
	Values                      map[string]string
	LastRequestForClientInShard map[int]map[int64]int64
	LastConfig                  shardctrler.Config
	LastConfigForShard          map[int]int
}

func (opType OpType) String() string {
	switch opType {
	case OpTypeGet:
		return "Get"
	case OpTypePut:
		return "Put"
	case OpTypeAppend:
		return "Append"
	case OpTypeMoveShard:
		return "MoveShard"
	case OpTypeConfig:
		return "Config"
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
	ConfigId             int
	Config               shardctrler.Config
	ShardId              int               // shard being transferred
	KeyValue             map[string]string // values in config
	LastRequestForClient map[int64]int64   // clientId -> lastRequestId for this shard
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
	mck          *shardctrler.Clerk
	dead         int32

	// Your definitions here.
	kvStore                     map[string]string
	chanByRequestIdByClientId   map[int64]map[int64]chan raft.ApplyMsg // ephemeral channels for each requestId by clientId
	chanByConfigId              map[int]chan raft.ApplyMsg
	lastRequestForClientInShard map[int]map[int64]int64 // maps shardId -> clientId -> greatest requestId seen so far
	lastPersistedIndex          int
	lastConfig                  shardctrler.Config

	shardReady   [shardctrler.NShards]bool
	shardReadyCh [shardctrler.NShards]chan struct{}

	lastConfigForShard map[int]int // shardId -> last configId applied for this shard
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isMyShard(args.ShardId) && !kv.shardReady[args.ShardId] {
		ch := kv.shardReadyCh[args.ShardId]
		DPrintf("server %d gid %d cfg %d: waiting for shard %d (key shard %d) to be unblocked in Get reqId %d", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, key2shard(args.Key), args.RequestId)
		kv.mu.Unlock()
		<-ch
		kv.mu.Lock()
		DPrintf("server %d gid %d cfg %d: unblocked shard %d (key shard %d) in Get reqId %d", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, key2shard(args.Key), args.RequestId)
	}
	if !kv.isMyShard(args.ShardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("server %d gid %d cfg %d: Get failed for shard %d (key shard %d) (wrong group)", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, key2shard(args.Key))
		return
	}
	var lastRequest int64
	if shardMap, ok := kv.lastRequestForClientInShard[args.ShardId]; ok {
		lastRequest = shardMap[args.ClientId]
	}
	DPrintf("server %d gid %d cfg %d: Get reqId %d from client %d (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args.RequestId, args.ClientId, key2shard(args.Key))
	if args.RequestId <= lastRequest {
		reply.Err = OK
		DPrintf("server %d gid %d cfg %d: Get duplicate reqId %d from client %d (last was %d), returning cached value for key %v (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args.RequestId, args.ClientId, lastRequest, args.Key, key2shard(args.Key))
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
	DPrintf("server %d gid %d cfg %d: waiting Get for request %v (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args, key2shard(args.Key))
	kv.mu.Unlock()
	<-c
	DPrintf("server %d gid %d cfg %d: received Get request %v from channel (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args, key2shard(args.Key))
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvStore[args.Key]
	DPrintf("server %d gid %d cfg %d: returning value %v for Get reqId %d key %v (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, reply.Value, args.RequestId, args.Key, key2shard(args.Key))
	reply.Err = OK
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isMyShard(args.ShardId) && !kv.shardReady[args.ShardId] {
		ch := kv.shardReadyCh[args.ShardId]
		DPrintf("server %d gid %d cfg %d: waiting for shard %d (key shard %d) to be unblocked in PutAppend reqId %d", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, key2shard(args.Key), args.RequestId)
		kv.mu.Unlock()
		<-ch
		kv.mu.Lock()
		DPrintf("server %d gid %d cfg %d: unblocked shard %d (key shard %d) in PutAppend reqId %d", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, key2shard(args.Key), args.RequestId)
	}
	if !kv.isMyShard(args.ShardId) {
		DPrintf("server %d gid %d cfg %d: PutAppend failed for shard %d (key shard %d) (wrong group)", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, key2shard(args.Key))
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	var lastRequest int64
	if shardMap, ok := kv.lastRequestForClientInShard[args.ShardId]; ok {
		lastRequest = shardMap[args.ClientId]
	}
	DPrintf("server %d gid %d cfg %d: PutAppend request %v (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args, key2shard(args.Key))
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
	DPrintf("server %d gid %d cfg %d: RECEIVED PutAppend request %v (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args, key2shard(args.Key))
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
	DPrintf("server %d gid %d cfg %d: waiting PutAppend for request %v (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args, key2shard(args.Key))
	kv.mu.Unlock()
	<-c
	DPrintf("server %d gid %d cfg %d: received PutAppend request %v from channel (key shard %d)", kv.me, kv.gid, kv.lastConfig.Num, args, key2shard(args.Key))
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
}

func (kv *ShardKV) isMyShard(shardId int) bool {
	gid := kv.lastConfig.Shards[shardId]
	return gid == kv.gid
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
	kv.chanByConfigId = make(map[int]chan raft.ApplyMsg)
	kv.lastRequestForClientInShard = make(map[int]map[int64]int64)
	kv.lastConfigForShard = make(map[int]int)
	kv.lastPersistedIndex = 0
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardReady[i] = true
		kv.lastConfigForShard[i] = -1
	}

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	data := kv.rf.ReadSnapshot()
	kv.restoreFromSnapshot(data)

	go kv.apply()
	go kv.checkConfig()

	DPrintf("server %d gid %d cfg %d: starting", kv.me, kv.gid, kv.lastConfig.Num)

	return kv
}

func (kv *ShardKV) checkConfig() {
	for {
		if kv.killed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
		//DPrintf("Server %d checkConfig ends", kv.me)
		kv.mu.Lock()
		var lastConfigNum int
		if kv.lastConfig.Num == 0 {
			lastConfigNum = 1
		} else {
			lastConfigNum = kv.lastConfig.Num + 1
		}
		kv.mu.Unlock()

		newConfig := kv.mck.Query(lastConfigNum)
		kv.mu.Lock()
		if newConfig.Num == 1 {
			for key, _ := range kv.lastConfigForShard {
				kv.lastConfigForShard[key] = newConfig.Num
			}
		}
		op := Op{
			Key:       "",
			OpType:    OpTypeConfig,
			RequestId: -1,
			ClientId:  -1,
			Config:    newConfig,
		}
		if newConfig.Num <= kv.lastConfig.Num {
			kv.mu.Unlock()
			continue
		}
		DPrintf("server %d gid %d cfg %d: queried config %d got config %v", kv.me, kv.gid, kv.lastConfig.Num, lastConfigNum, newConfig)
		_, _, isLeader := kv.rf.Start(op)
		DPrintf("server %d gid %d cfg %d: applied config %d and is leader %v", kv.me, kv.gid, kv.lastConfig.Num, lastConfigNum, isLeader)
		if !isLeader {
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("server %d gid %d cfg %d: RequestShard RPC received FROM gid %d for shard %d configId %d (lastConfigForShard[%d]=%d)", kv.me, kv.gid, kv.lastConfig.Num, args.RequestorGid, args.ShardId, args.ConfigId, args.ShardId, kv.lastConfigForShard[args.ShardId])

	if kv.lastConfigForShard[args.ShardId] < args.ConfigId-1 {
		DPrintf("server %d gid %d cfg %d: RequestShard NOT READY for gid %d - shard %d at config %d, need %d", kv.me, kv.gid, kv.lastConfig.Num, args.RequestorGid, args.ShardId, kv.lastConfigForShard[args.ShardId], args.ConfigId-1)
		reply.Err = ErrNotReady
		return
	}

	reply.Values = make(map[string]string)
	for k, v := range kv.kvStore {
		if key2shard(k) == args.ShardId {
			reply.Values[k] = v
		}
	}

	reply.LastRequestForClient = make(map[int64]int64)
	if clientMap, ok := kv.lastRequestForClientInShard[args.ShardId]; ok {
		for clientId, reqId := range clientMap {
			reply.LastRequestForClient[clientId] = reqId
		}
	}

	DPrintf("server %d gid %d cfg %d: RequestShard OK TO gid %d for shard %d configId %d, returning %d keys", kv.me, kv.gid, kv.lastConfig.Num, args.RequestorGid, args.ShardId, args.ConfigId, len(reply.Values))
	reply.Err = OK
}

// Pull shard data from old owner (runs as goroutine)
func (kv *ShardKV) pullShard(shardId int, oldConfig, newConfig shardctrler.Config) {
	DPrintf("server %d gid %d cfg %d: pullShard starting for shard %d (oldConfig %d -> newConfig %d)", kv.me, kv.gid, kv.lastConfig.Num, shardId, oldConfig.Num, newConfig.Num)

	// Find old owner
	oldGid := oldConfig.Shards[shardId]
	if oldGid == 0 || oldGid == kv.gid {
		// No previous owner or we already own it - unblock immediately with empty data
		DPrintf("server %d gid %d cfg %d: pullShard shard %d has no previous owner (oldGid=%d), submitting empty MoveShard TO gid %d", kv.me, kv.gid, kv.lastConfig.Num, shardId, oldGid, kv.gid)
		kv.mu.Lock()
		op := Op{
			OpType:               OpTypeMoveShard,
			ConfigId:             newConfig.Num,
			ShardId:              shardId,
			KeyValue:             make(map[string]string),
			LastRequestForClient: make(map[int64]int64),
		}
		kv.rf.Start(op)
		kv.mu.Unlock()
		return
	}

	servers := oldConfig.Groups[oldGid]
	if servers == nil {
		// Old group was never in any config (shouldn't happen) - unblock with empty data
		DPrintf("server %d gid %d cfg %d: pullShard shard %d old group %d not in oldConfig, submitting empty MoveShard TO gid %d", kv.me, kv.gid, kv.lastConfig.Num, shardId, oldGid, kv.gid)
		kv.mu.Lock()
		op := Op{
			OpType:               OpTypeMoveShard,
			ConfigId:             newConfig.Num,
			ShardId:              shardId,
			KeyValue:             make(map[string]string),
			LastRequestForClient: make(map[int64]int64),
		}
		kv.rf.Start(op)
		kv.mu.Unlock()
		return
	}

	args := RequestShardArgs{
		ConfigId:     newConfig.Num,
		ShardId:      shardId,
		RequestorGid: kv.gid,
	}

	// Retry loop with exponential backoff
	backoff := 10 * time.Millisecond
	for !kv.killed() {
		// Try each server in the old owner group
		for si := 0; si < len(servers); si++ {
			reply := RequestShardReply{}
			srv := kv.make_end(servers[si])

			kv.mu.Lock()
			DPrintf("server %d gid %d cfg %d: pullShard sending RequestShard FROM gid %d TO gid %d server %d for shard %d configId %d", kv.me, kv.gid, kv.lastConfig.Num, kv.gid, oldGid, si, shardId, newConfig.Num)
			kv.mu.Unlock()

			okCh := make(chan bool, 1)
			go func() {
				okCh <- srv.Call("ShardKV.RequestShard", &args, &reply)
			}()

			var ok bool
			select {
			case ok = <-okCh:
			case <-time.After(100 * time.Millisecond):
				ok = false
				DPrintf("server %d gid %d cfg %d: pullShard timeout for shard %d from gid %d server %d", kv.me, kv.gid, kv.lastConfig.Num, shardId, oldGid, si)
			}

			if ok && reply.Err == OK {
				// Got the data! Submit to Raft
				DPrintf("server %d gid %d cfg %d: pullShard SUCCESS for shard %d, got %d keys FROM gid %d TO gid %d, submitting MoveShard op", kv.me, kv.gid, kv.lastConfig.Num, shardId, len(reply.Values), oldGid, kv.gid)
				kv.mu.Lock()
				op := Op{
					OpType:               OpTypeMoveShard,
					ConfigId:             newConfig.Num,
					ShardId:              shardId,
					KeyValue:             reply.Values,
					LastRequestForClient: reply.LastRequestForClient,
				}
				kv.rf.Start(op)
				kv.mu.Unlock()
				return // Success!
			} else if ok && reply.Err == ErrNotReady {
				DPrintf("server %d gid %d cfg %d: pullShard shard %d FROM gid %d server %d NOT READY", kv.me, kv.gid, kv.lastConfig.Num, shardId, oldGid, si)
			}
		}

		// All servers failed or returned ErrNotReady - retry with backoff
		DPrintf("server %d gid %d cfg %d: pullShard shard %d failed, retrying after %v", kv.me, kv.gid, kv.lastConfig.Num, shardId, backoff)
		time.Sleep(backoff)
		if backoff < 1*time.Second {
			backoff *= 2
		}
	}
}

// need to be locked
func (kv *ShardKV) changeConfig(newConfig shardctrler.Config) {
	if newConfig.Num == 1 {
		// First config - initialize all shards
		for key := range kv.lastConfigForShard {
			kv.lastConfigForShard[key] = newConfig.Num
		}
		kv.lastConfig = newConfig
		DPrintf("server %d gid %d cfg %d: setting first config %v", kv.me, kv.gid, kv.lastConfig.Num, newConfig)
		return
	}

	DPrintf("server %d gid %d cfg %d: applying config change to config %v", kv.me, kv.gid, kv.lastConfig.Num, newConfig)
	gained, _ := kv.shardChanges(newConfig)

	// Block gained shards
	for _, shard := range gained {
		kv.shardReady[shard] = false
		kv.shardReadyCh[shard] = make(chan struct{})
		DPrintf("server %d gid %d cfg %d: blocking shard %d (gained, waiting for data) in newConfig %d", kv.me, kv.gid, kv.lastConfig.Num, shard, newConfig.Num)
	}

	// Update lastConfigForShard for shards we continue to own (not gained or lost)
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.lastConfig.Shards[i] == kv.gid && newConfig.Shards[i] == kv.gid {
			// We owned this shard before and still own it - update to new config
			kv.lastConfigForShard[i] = newConfig.Num
			DPrintf("server %d gid %d cfg %d: shard %d continues ownership, updating lastConfigForShard to %d", kv.me, kv.gid, kv.lastConfig.Num, i, newConfig.Num)
		}
	}

	oldConfig := kv.lastConfig
	kv.lastConfig = newConfig

	// Launch pull goroutines for gained shards
	for _, shardId := range gained {
		go kv.pullShard(shardId, oldConfig, newConfig)
	}
}

// shardChanges returns which shards were gained and which were lost
// comparing the current config with newConfig.
func (kv *ShardKV) shardChanges(newConfig shardctrler.Config) (gained []int, lost []int) {
	for i := 0; i < len(newConfig.Shards); i++ {
		oldMine := kv.lastConfig.Shards[i] == kv.gid
		newMine := newConfig.Shards[i] == kv.gid
		if !oldMine && newMine {
			gained = append(gained, i)
		} else if oldMine && !newMine {
			lost = append(lost, i)
		}
	}
	return
}

func (kv *ShardKV) replacedShards(newConfig shardctrler.Config) []int {
	oldShards := make(map[int]bool, 10)
	for i, n := range kv.lastConfig.Shards {
		if n == kv.gid {
			oldShards[i] = true
		} else {
			oldShards[i] = false
		}
	}

	currentShards := make(map[int]bool, 10)
	for i, n := range newConfig.Shards {
		if n == kv.gid {
			currentShards[i] = true
		} else {
			currentShards[i] = false
		}
	}

	var replacedShards []int
	for i, v := range oldShards {
		if v {
			if !currentShards[i] {
				replacedShards = append(replacedShards, i)
			}
		}
	}
	DPrintf("server %d gid %d cfg %d: replacedShards %v for newConfig %d", kv.me, kv.gid, kv.lastConfig.Num, replacedShards, newConfig.Num)

	return replacedShards
}

func (kv *ShardKV) apply() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh
		DPrintf("server %d gid %d cfg %d: got msg %+v", kv.me, kv.gid, kv.lastConfig.Num, msg)
		if !msg.CommandValid {
			DPrintf("server %d gid %d cfg %d: got msg %+v", kv.me, kv.gid, kv.lastConfig.Num, msg)
			kv.mu.Lock()
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Lock()
		op := msg.Command.(Op)
		clientId := op.ClientId
		DPrintf("server %d gid %d cfg %d: applying op reqId %d type %v", kv.me, kv.gid, kv.lastConfig.Num, op.RequestId, op.OpType)
		if op.OpType == OpTypeConfig {
			kv.changeConfig(op.Config)
			kv.mu.Unlock()
			continue
		}
		if op.OpType == OpTypeMoveShard {
			shardId := op.ShardId
			receivedKvs := op.KeyValue
			DPrintf("server %d gid %d cfg %d: applying MoveShard TO gid %d for shard %d configId %d with %d keys", kv.me, kv.gid, kv.lastConfig.Num, kv.gid, shardId, op.ConfigId, len(receivedKvs))
			DPrintf("server %d gid %d cfg %d: kvStore before MoveShard %v", kv.me, kv.gid, kv.lastConfig.Num, kv.kvStore)
			for k, v := range receivedKvs {
				kv.kvStore[k] = v
			}
			// Apply lastRequestForClient from the transfer for this shard
			if kv.lastRequestForClientInShard[shardId] == nil {
				kv.lastRequestForClientInShard[shardId] = make(map[int64]int64)
			}
			for cId, reqId := range op.LastRequestForClient {
				if reqId > kv.lastRequestForClientInShard[shardId][cId] {
					kv.lastRequestForClientInShard[shardId][cId] = reqId
				}
			}
			// Unblock this shard
			if !kv.shardReady[shardId] {
				DPrintf("server %d gid %d cfg %d: MoveShard COMPLETE - unblocking shard %d (received by gid %d)", kv.me, kv.gid, kv.lastConfig.Num, shardId, kv.gid)
				kv.shardReady[shardId] = true
				kv.lastConfigForShard[shardId] = kv.lastConfig.Num
				close(kv.shardReadyCh[shardId])
			}
			DPrintf("server %d gid %d cfg %d: kvStore after MoveShard: %v (configId %d cmdIndex %d)", kv.me, kv.gid, kv.lastConfig.Num, kv.kvStore, op.ConfigId, msg.CommandIndex)
			kv.mu.Unlock()
			continue
		}
		// For normal operations, get lastRequest from the shard
		shard := key2shard(op.Key)
		var lastRequest int64
		if shardMap, ok := kv.lastRequestForClientInShard[shard]; ok {
			lastRequest = shardMap[clientId]
		}
		if op.RequestId <= lastRequest {
			kv.mu.Unlock()
			continue
		}
		if op.OpType == OpTypePut {
			//DPrintf("Server %d got put request %v", kv.me, msg.Command)
			kv.kvStore[op.Key] = op.Value
		} else if op.OpType == OpTypeAppend {
			kv.kvStore[op.Key] += op.Value
		}
		DPrintf("server %d gid %d cfg %d: kvStore[%v]=%v after applying %v reqId %d cmdIndex %d",
			kv.me, kv.gid, kv.lastConfig.Num, op.Key, kv.kvStore[op.Key], op.OpType, op.RequestId, msg.CommandIndex)
		// Update lastRequestForClientInShard
		if kv.lastRequestForClientInShard[shard] == nil {
			kv.lastRequestForClientInShard[shard] = make(map[int64]int64)
		}
		kv.lastRequestForClientInShard[shard][clientId] = op.RequestId
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
		Values:                      kv.kvStore,
		LastRequestForClientInShard: kv.lastRequestForClientInShard,
		LastConfig:                  kv.lastConfig,
		LastConfigForShard:          kv.lastConfigForShard,
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
	kv.lastRequestForClientInShard = snapshot.LastRequestForClientInShard
	kv.lastConfigForShard = snapshot.LastConfigForShard
	kv.lastConfig = snapshot.LastConfig
	DPrintf("server %d gid %d cfg %d: restored from snapshot, kvStore=%v", kv.me, kv.gid, kv.lastConfig.Num, kv.kvStore)

	// Restart pull goroutines for blocked shards
	// Note: We need to restart pulls because they may have been interrupted
	// However, we don't have oldConfig saved, so we use lastConfig
	// This works because blocked shards are waiting for data from previous config
	for i := 0; i < shardctrler.NShards; i++ {
		if !kv.shardReady[i] && kv.isMyShard(i) {
			DPrintf("server %d gid %d cfg %d: restarting pull for blocked shard %d after snapshot restore", kv.me, kv.gid, kv.lastConfig.Num, i)
			go kv.pullShard(i, kv.lastConfig, kv.lastConfig)
		}
	}
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}
