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

const Debug = false

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
	OpTypeConfig
)

type Snapshot struct {
	Values                      map[string]string
	LastRequestForClientInShard map[int]map[int64]int64
}

func (opType OpType) String() string {
	switch opType {
	case OpTypeGet:
		return "Get"
	case OpTypePut:
		return "Put"
	case OpTypeAppend:
		return "Append"
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
	ConfigId                    int
	KeyValue                    map[string]string       // values in config
	LastRequestForClientInShard map[int]map[int64]int64 // for shard transfer
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
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isMyShard(args.ShardId) && !kv.shardReady[args.ShardId] {
		ch := kv.shardReadyCh[args.ShardId]
		kv.mu.Unlock()
		<-ch
		kv.mu.Lock()
	}
	if !kv.isMyShard(args.ShardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("server %d gid [%d] Get failed for %d", kv.me, kv.gid, args.ShardId)
		return
	}
	var lastRequest int64
	if shardMap, ok := kv.lastRequestForClientInShard[args.ShardId]; ok {
		lastRequest = shardMap[args.ClientId]
	}
	DPrintf("Server %d gid %v get request %v", kv.me, kv.gid, args.RequestId)
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
	DPrintf("Server %d gid %d returning value %v for Get request %v", kv.me, kv.gid, reply.Value, args.RequestId)
	reply.Err = OK
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isMyShard(args.ShardId) && !kv.shardReady[args.ShardId] {
		ch := kv.shardReadyCh[args.ShardId]
		kv.mu.Unlock()
		<-ch
		kv.mu.Lock()
	}
	if !kv.isMyShard(args.ShardId) {
		DPrintf("server %d gid [%d] PutAppend failed for %d", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	var lastRequest int64
	if shardMap, ok := kv.lastRequestForClientInShard[args.ShardId]; ok {
		lastRequest = shardMap[args.ClientId]
	}
	DPrintf("Server %d with gid %d waiting putappend for request %v", kv.me, kv.gid, args)
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
	DPrintf("Server %d RECEIVED putappend request %v", kv.me, args)
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
	kv.lastPersistedIndex = 0
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardReady[i] = true
		// Channel starts closed (nil reads block forever, but we check shardReady first)
	}

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	go kv.checkConfig()
	DPrintf("kv.startServer %d starting with gid %d \n", kv.me, kv.gid)

	return kv
}

func (kv *ShardKV) checkConfig() {
	for {
		if kv.killed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
		//DPrintf("Server %d checkConfig ends", kv.me)
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		op := Op{
			Key:       "",
			OpType:    OpTypeConfig,
			RequestId: -1,
			ClientId:  -1,
			ConfigId:  newConfig.Num,
		}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			continue
		}
		if kv.lastConfig.Num >= newConfig.Num {
			kv.mu.Unlock()
			continue
		}
		DPrintf("Server %d gid %d checkConfig got config %v with num %d", kv.me, kv.gid, newConfig, newConfig.Num)
		if newConfig.Num == 1 {
			// First config: all shards are ready, no data to transfer
			kv.lastConfig = newConfig
			kv.mu.Unlock()
			continue
		}
		gained, _ := kv.shardChanges(newConfig)
		for _, shard := range gained {
			kv.shardReady[shard] = false
			kv.shardReadyCh[shard] = make(chan struct{})
			DPrintf("Server %d gid %d blocking shard %d (gained, waiting for data)", kv.me, kv.gid, shard)
		}
		kv.changeConfig(newConfig)
		kv.lastConfig = newConfig
		kv.mu.Unlock()
	}
}

// RPC to move shard
func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardsReply) {
	kv.mu.Lock()
	op := Op{
		ConfigId:                    args.ConfigId,
		KeyValue:                    args.Values,
		OpType:                      OpTypeConfig,
		LastRequestForClientInShard: args.LastRequestForClientInShard,
	}
	DPrintf("Server %d gid %d  config %d move shard with values %v", kv.me, kv.gid, args.ConfigId, args.Values)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server %d gid %d finished move shard %d with values %v", kv.me, kv.gid, args.ConfigId, args.Values)
	kv.mu.Unlock()
	//DPrintf("Server %d received get request %v from channel %v", kv.me, args, c)
	//DPrintf("Server %d returning value %v for Get request %v", kv.me, reply.Value, args)
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
	// todo aqui não preciso ser o lider?
	if newConfig.Num == 1 {
		return
	}
	replacedShards := kv.replacedShards(newConfig)
	if len(replacedShards) == 0 {
		return
	}
	gidsToReplacedShards := make(map[int][]int)
	// gids dos shards que preciso mandar
	for _, shard := range replacedShards {
		gid := newConfig.Shards[shard]
		DPrintf("server %d gid %d found gid %d for shard %d\n", kv.me, kv.gid, gid, shard)
		_, ok := gidsToReplacedShards[gid]
		if !ok {
			gidsToReplacedShards[gid] = []int{shard}
		} else {
			gidsToReplacedShards[gid] = append(gidsToReplacedShards[gid], shard)
		}
	}

	DPrintf("Server %d gid %d when changing config %d has kvValue %v", kv.me, kv.gid, newConfig.Num, kv.kvStore)
	DPrintf("Server %d gid %d change config got config %v and gidsToreplaceshard %v", kv.me, kv.gid, newConfig, gidsToReplacedShards)

	// todo maybe a goroutine here ? check how raft send

	for gid, shards := range gidsToReplacedShards {
		if gid == kv.gid {
			continue
		}
		if servers, ok := newConfig.Groups[gid]; ok {
			numServers := len(servers)
			for offset := 0; offset < numServers; offset++ {
				mapToSend := make(map[string]string)
				for k, v := range kv.kvStore {
					shard := key2shard(k)
					DPrintf("Reconfiguration: server %d gid %d sending key %v value %v with shard %d\n", kv.me, kv.gid, k, v, shard)
					// todo aqui tem erro. precisa estar apenas na config atual ou seja pegar das key
					for _, s := range shards {
						if s == shard {
							mapToSend[k] = v
						}
					}
				}
				// Build lastRequestForClientInShard only for shards being transferred
				lastRequestToSend := make(map[int]map[int64]int64)
				for _, s := range shards {
					if clientMap, ok := kv.lastRequestForClientInShard[s]; ok {
						lastRequestToSend[s] = make(map[int64]int64)
						for clientId, reqId := range clientMap {
							lastRequestToSend[s][clientId] = reqId
						}
					}
				}
				// last request before reshard
				reply := MoveShardsReply{}
				args := MoveShardArgs{
					ConfigId:                    newConfig.Num,
					Values:                      mapToSend,
					LastRequestForClientInShard: lastRequestToSend,
				}
				ok := make(chan bool)
				offset := offset
				go func() {
					srv := kv.make_end(servers[offset])
					DPrintf("Server %d gid %d move shard with values %v and config id %d sending to %d", kv.me, kv.gid, args.Values, args.ConfigId, offset)
					ok <- srv.Call("ShardKV.MoveShard", &args, &reply)
				}()
				select {
				case ok := <-ok:
					DPrintf("Server %d got reply %v", kv.me, reply)
					if ok {
						if reply.Err == OK {
							DPrintf("Server [%d] gid %d completed move shard %d to %d", kv.me, kv.gid, args.ConfigId, offset)
						}
						if reply.Err == ErrWrongLeader {
							DPrintf("Server [%d] gid %d found another leader in response from moveshard %v server id %v", kv.me, kv.gid, args.ConfigId, offset)
						}
					}
				case <-time.After(20 * time.Millisecond):
					{
						DPrintf("Server %d MoveShard to server %d timeout", kv.me, offset)
						continue
					}
				}
			}

		}
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
	DPrintf("Server %d gid %d replacedShards %v and config id %d", kv.me, kv.gid, replacedShards, newConfig.Num)

	return replacedShards
}

func (kv *ShardKV) apply() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh
		DPrintf("Server %d gid %d got command %v with index %d and request id %d", kv.me, kv.gid, msg.Command, msg.CommandIndex, msg.CommandIndex)
		if !msg.CommandValid {
			kv.mu.Lock()
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Lock()
		op := msg.Command.(Op)
		clientId := op.ClientId
		DPrintf("Server %d gid %v lastRequest %v of type %v", kv.me, kv.gid, op.RequestId, op.OpType)
		if op.OpType == OpTypeConfig {
			receivedKvs := op.KeyValue
			DPrintf("Server %d gid %v applying config num %d with values %v", kv.me, kv.gid, op.ConfigId, receivedKvs)
			DPrintf("Server %d gid %d receivedKV %v", kv.me, kv.gid, receivedKvs)
			DPrintf("Server %d gid %d kv store before resharding %v", kv.me, kv.gid, kv.kvStore)
			for k, v := range receivedKvs {
				kv.kvStore[k] = v
			}
			// Apply lastRequestForClientInShard from the transfer
			for shardId, clientMap := range op.LastRequestForClientInShard {
				if kv.lastRequestForClientInShard[shardId] == nil {
					kv.lastRequestForClientInShard[shardId] = make(map[int64]int64)
				}
				for cId, reqId := range clientMap {
					if reqId > kv.lastRequestForClientInShard[shardId][cId] {
						kv.lastRequestForClientInShard[shardId][cId] = reqId
					}
				}
			}
			// Unblock shards that received data
			for k := range receivedKvs {
				shard := key2shard(k)
				if !kv.shardReady[shard] {
					kv.shardReady[shard] = true
					close(kv.shardReadyCh[shard])
					DPrintf("Server %d gid %d unblocking shard %d (data received)", kv.me, kv.gid, shard)
				}
			}
			DPrintf("Server %d gid %d kv store after resharding %v config id %d and command index %d \n", kv.me, kv.gid, kv.kvStore, op.ConfigId, msg.CommandIndex)
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
		DPrintf("server %d gid %d kvvalue after apllying command %v in key %v is %v and command index %d",
			kv.me, kv.gid, op.RequestId, op.Key, kv.kvStore[op.Key], msg.CommandIndex)
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
	DPrintf("After snapshot %v\n", kv.kvStore)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}
