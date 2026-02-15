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
		var lastConfigNum int
		if kv.lastConfig.Num == 0 {
			lastConfigNum = 1
		} else {
			lastConfigNum = kv.lastConfig.Num + 1
		}

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
		if !isLeader {
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()
	}
}

// RPC to move shard
func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardsReply) {
	kv.mu.Lock()
	op := Op{
		ConfigId:             args.ConfigId,
		ShardId:              args.ShardId,
		KeyValue:             args.Values,
		OpType:               OpTypeMoveShard,
		LastRequestForClient: args.LastRequestForClient,
	}
	DPrintf("server %d gid %d cfg %d: MoveShard RPC received, shard %d with values %v (args.ConfigId=%d)", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, args.Values, args.ConfigId)
	if kv.lastConfigForShard[op.ShardId] >= op.ConfigId {
		DPrintf("server %d gid %d cfg %d: MoveShard DUPLICATE shard %d configId %d already applied (last configId for this shard: %d), returning OK", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, args.ConfigId, kv.lastConfigForShard[op.ShardId])
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("server %d gid %d cfg %d: finished MoveShard RPC, shard %d with values %v", kv.me, kv.gid, kv.lastConfig.Num, args.ShardId, args.Values)
	kv.mu.Unlock()
	reply.Err = OK
}

// need to be locked
func (kv *ShardKV) changeConfig(newConfig shardctrler.Config) {

	//IMPORTANTE AVISO
	//You'll need to provide at-most-once semantics (duplicate detection) for client requests across shard movement.

	//If one of your RPC handlers includes in its reply a map (e.g. a key/value map) that's part of your server's state, you may get bugs due to races.
	//The RPC system has to read the map in order to send it to the caller, but it isn't holding a lock that covers the map.
	//Your server, however, may proceed to modify the same map while the RPC system is reading it. The solution is for the RPC handler to include a copy of the map in the reply.
	//  If you put a map or a slice in a Raft log entry, and your key/value server subsequently sees the entry on the applyCh and
	//T saves a reference to the map/slice in your key/value server's state,
	////T you may have a race. Make a copy of the map/slice, and store the copy in your key/value server's state.
	////The race is between your key/value server modifying the map/slice and Raft reading it while persisting its log.
	//
	//for {
	//	// PSEUDOCODIGO
	//	// pego meus shards atuais e que não são mais meus
	if newConfig.Num == 1 {
		kv.lastConfig = newConfig
		DPrintf("server %d gid %d cfg %d: setting first config %v", kv.me, kv.gid, kv.lastConfig.Num, newConfig)
		return
	}
	DPrintf("server %d gid %d cfg %d: applying config change to config %v", kv.me, kv.gid, kv.lastConfig.Num, newConfig)
	gained, _ := kv.shardChanges(newConfig)
	for _, shard := range gained {
		kv.shardReady[shard] = false
		kv.shardReadyCh[shard] = make(chan struct{})
		DPrintf("server %d gid %d cfg %d: blocking shard %d (gained, waiting for data) in newConfig %d", kv.me, kv.gid, kv.lastConfig.Num, shard, newConfig.Num)
	}

	replacedShards := kv.replacedShards(newConfig)
	kv.lastConfig = newConfig
	if len(replacedShards) == 0 {
		return
	}
	gidsToReplacedShards := make(map[int][]int)
	// gids dos shards que preciso mandar
	for _, shard := range replacedShards {
		gid := newConfig.Shards[shard]
		DPrintf("server %d gid %d cfg %d: found target gid %d for shard %d", kv.me, kv.gid, kv.lastConfig.Num, gid, shard)
		_, ok := gidsToReplacedShards[gid]
		if !ok {
			gidsToReplacedShards[gid] = []int{shard}
		} else {
			gidsToReplacedShards[gid] = append(gidsToReplacedShards[gid], shard)
		}
	}

	DPrintf("server %d gid %d cfg %d: when changing to newConfig %d has kvStore %v", kv.me, kv.gid, kv.lastConfig.Num, newConfig.Num, kv.kvStore)
	DPrintf("server %d gid %d cfg %d: change config got newConfig %v and gidsToReplacedShards %v", kv.me, kv.gid, kv.lastConfig.Num, newConfig, gidsToReplacedShards)

	// Send shards in parallel
	var wg sync.WaitGroup
	for gid, shards := range gidsToReplacedShards {
		if gid == kv.gid {
			continue
		}
		servers, ok := newConfig.Groups[gid]
		if !ok {
			continue
		}
		// Send each shard in parallel
		for _, shardId := range shards {
			// Build map with only keys for this shard
			mapToSend := make(map[string]string)
			for k, v := range kv.kvStore {
				if key2shard(k) == shardId {
					mapToSend[k] = v
				}
			}
			// Build lastRequestForClient for this shard only
			lastRequestToSend := make(map[int64]int64)
			if clientMap, ok := kv.lastRequestForClientInShard[shardId]; ok {
				for clientId, reqId := range clientMap {
					lastRequestToSend[clientId] = reqId
				}
			}

			args := MoveShardArgs{
				ConfigId:             newConfig.Num,
				ShardId:              shardId,
				Values:               mapToSend,
				LastRequestForClient: lastRequestToSend,
			}

			wg.Add(1)
			go func(shardId int, servers []string, args MoveShardArgs) {
				defer wg.Done()
				// Try each server in the group
				for offset := 0; offset < len(servers); offset++ {
					reply := MoveShardsReply{}
					okCh := make(chan bool)
					go func(offset int) {
						srv := kv.make_end(servers[offset])
						DPrintf("server %d gid %d cfg %d: sending MoveShard shard %d with values %v configId %d to server %d", kv.me, kv.gid, kv.lastConfig.Num, shardId, args.Values, args.ConfigId, offset)
						okCh <- srv.Call("ShardKV.MoveShard", &args, &reply)
					}(offset)
					select {
					case ok := <-okCh:
						DPrintf("server %d gid %d cfg %d: got reply %v for shard %d", kv.me, kv.gid, kv.lastConfig.Num, reply, shardId)
						if ok && reply.Err == OK {
							DPrintf("server %d gid %d cfg %d: completed MoveShard shard %d to server %d", kv.me, kv.gid, kv.lastConfig.Num, shardId, offset)
							return // Success
						}
						if ok && reply.Err == ErrWrongLeader {
							DPrintf("server %d gid %d cfg %d: found another leader in MoveShard response for shard %d server %d", kv.me, kv.gid, kv.lastConfig.Num, shardId, offset)
						}
					case <-time.After(20 * time.Millisecond):
						DPrintf("server %d gid %d cfg %d: MoveShard shard %d to server %d timeout", kv.me, kv.gid, kv.lastConfig.Num, shardId, offset)
					}
				}
			}(shardId, servers, args)
		}
	}
	wg.Wait()
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
			DPrintf("server %d gid %d cfg %d: applying shard move configId %d shard %d with values %v", kv.me, kv.gid, kv.lastConfig.Num, op.ConfigId, shardId, receivedKvs)
			DPrintf("server %d gid %d cfg %d: kvStore before resharding %v", kv.me, kv.gid, kv.lastConfig.Num, kv.kvStore)
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
				DPrintf("server %d gid %d cfg %d: unblocking shard %d (data received)", kv.me, kv.gid, kv.lastConfig.Num, shardId)
				kv.shardReady[shardId] = true
				kv.lastConfigForShard[shardId] = kv.lastConfig.Num
				close(kv.shardReadyCh[shardId])
			}
			DPrintf("server %d gid %d cfg %d: kvStore after resharding %v configId %d cmdIndex %d", kv.me, kv.gid, kv.lastConfig.Num, kv.kvStore, op.ConfigId, msg.CommandIndex)
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
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}
