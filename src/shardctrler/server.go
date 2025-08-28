package shardctrler

import (
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync/atomic"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs                   []Config // indexed by config num
	gids                      map[int]struct{}
	lastRequestForClient      map[int64]int64 // maps clientId to greatest requestId seen so far that we can deduplicate requests
	lastPersistedIndex        int
	chanByRequestIdByClientId map[int64]map[int64]chan raft.ApplyMsg
	dead                      int32
}

// enum for op type arg
// OpType is the type of operation
type OpType int

// Operation types
const (
	JoinOp OpType = iota
	LeaveOp
	MoveOp
	QueryOp
)

type Op struct {
	// Your data here.
	// common fields for all operations
	Type      OpType
	RequestId int64
	ClientId  int64

	// specific fields for each operation
	Servers map[int][]string // for Join
	GIDs    []int            // for Leave
	Shard   int              // for Move
	GID     int              // for Move
	Num     int              // for Query
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	lastRequest := sc.lastRequestForClient[args.ClientId]
	DPrintf("[%d] Join command with args %v", sc.me, args)

	if args.LastRequest <= lastRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received join command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastRequestForClient[args.ClientId])
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		Type:      JoinOp,
		RequestId: args.LastRequest,
		ClientId:  args.ClientId,
		Servers:   args.Servers,
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}
	sc.mu.Lock()
	clientChans, ok := sc.chanByRequestIdByClientId[args.ClientId]
	if !ok {
		clientChans = make(map[int64]chan raft.ApplyMsg)
		sc.chanByRequestIdByClientId[args.ClientId] = clientChans
	}
	c, ok := clientChans[args.LastRequest]
	if !ok {
		c = make(chan raft.ApplyMsg, 1)
		clientChans[args.LastRequest] = c
	}
	sc.mu.Unlock()
	DPrintf("[%d] Join command after channel with args %v", sc.me, args)
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = false
	sc.lastRequestForClient[args.ClientId] = args.LastRequest
	reply.Err = OK
	DPrintf("[%d] configs are %v", sc.me, sc.configs)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	lastRequest := sc.lastRequestForClient[args.ClientId]

	if args.LastRequest <= lastRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received leave command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastRequestForClient[args.ClientId])
		return
	}
	sc.mu.Unlock()
	DPrintf("[%d] leave command with args %v", sc.me, args)
	op := Op{
		Type:      LeaveOp,
		RequestId: args.LastRequest,
		ClientId:  args.ClientId,
		GIDs:      args.GIDs,
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	sc.mu.Lock()
	clientChans, ok := sc.chanByRequestIdByClientId[args.ClientId]
	if !ok {
		clientChans = make(map[int64]chan raft.ApplyMsg)
		sc.chanByRequestIdByClientId[args.ClientId] = clientChans
	}
	c, ok := clientChans[args.LastRequest]
	if !ok {
		c = make(chan raft.ApplyMsg, 1)
		clientChans[args.LastRequest] = c
	}
	DPrintf("[%d] Leave command with args %v", sc.me, args)
	sc.mu.Unlock()
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf("[%d] Leave command after channel with args %v", sc.me, args)
	sc.lastRequestForClient[args.ClientId] = args.LastRequest
	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	lastRequest := sc.lastRequestForClient[args.ClientId]

	DPrintf("[%d] Move command with args %v", sc.me, args)
	if args.LastRequest <= lastRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received move command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastRequestForClient[args.ClientId])
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()
	_, _, isLeader := sc.rf.Start(Op{
		Type:      MoveOp,
		RequestId: args.LastRequest,
		ClientId:  args.ClientId,
		Shard:     args.Shard,
		GID:       args.GID,
	})

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	sc.mu.Lock()
	clientChans, ok := sc.chanByRequestIdByClientId[args.ClientId]
	if !ok {
		clientChans = make(map[int64]chan raft.ApplyMsg)
		sc.chanByRequestIdByClientId[args.ClientId] = clientChans
	}
	c, ok := clientChans[args.LastRequest]
	if !ok {
		c = make(chan raft.ApplyMsg, 1)
		clientChans[args.LastRequest] = c
	}
	sc.mu.Unlock()
	// wait for the command to be applied
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = false
	reply.Err = OK
	sc.lastRequestForClient[args.ClientId] = args.LastRequest
	DPrintf("[%d] Move command applied with args %v", sc.me, args)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	lastRequest := sc.lastRequestForClient[args.ClientId]

	DPrintf("[%d]  Query command with last request %v", sc.me, args.LastRequest)

	if args.LastRequest <= lastRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received query command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastRequestForClient[args.ClientId])
		reply.Config = sc.configs[len(sc.configs)-1]
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	_, _, isLeader := sc.rf.Start(Op{
		Type:      QueryOp,
		RequestId: args.LastRequest,
		ClientId:  args.ClientId,
		Num:       args.Num,
	})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	sc.mu.Lock()
	reply.WrongLeader = false
	clientChans, ok := sc.chanByRequestIdByClientId[args.ClientId]
	if !ok {
		clientChans = make(map[int64]chan raft.ApplyMsg)
		sc.chanByRequestIdByClientId[args.ClientId] = clientChans
	}
	c, ok := clientChans[args.LastRequest]
	if !ok {
		c = make(chan raft.ApplyMsg, 1)
		clientChans[args.LastRequest] = c
	}
	sc.mu.Unlock()
	// wait for the command to be applied
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf("[%d] Query command with args %v and len config %d", sc.me, args, len(sc.configs))
	reply.Err = OK
	sc.lastRequestForClient[args.ClientId] = args.LastRequest
	if args.Num == -1 || len(sc.configs) <= args.Num {
		config := sc.configs[len(sc.configs)-1]
		reply.WrongLeader = false
		reply.Config = config
		return
	}
	config := sc.configs[args.Num]
	reply.WrongLeader = false
	reply.Config = config
	DPrintf("[%d] Query command applied with args %v response %v", sc.me, args, reply.Config)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.

	atomic.StoreInt32(&sc.dead, 1)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

type Snapshot struct {
	Configs              []Config
	Gids                 map[int]struct{}
	LastRequestForClient map[int64]int64 // maps clientId to greatest requestId seen so far that we can deduplicate requests
}

func (sc *ShardCtrler) restoreSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("[%d] no snapshot data to restore", sc.me)
		return
	}
	DPrintf("[%d] restoring snapshot with data %v", sc.me, data)
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := labgob.NewDecoder(&buffer)
	var snapshot Snapshot
	err := decoder.Decode(&snapshot)
	if err != nil {
		log.Fatalf("[restoreSnapshot] Error restoring snapshot %v", err)
	}
	sc.configs = snapshot.Configs
	sc.gids = snapshot.Gids
	sc.lastRequestForClient = snapshot.LastRequestForClient
	DPrintf("[%d] restored snapshot with configs %v, gids %v, lastRequestForClient %v", sc.me, sc.configs, sc.gids, sc.lastRequestForClient)
}

func (sc *ShardCtrler) apply() {
	for {
		if sc.killed() {
			return
		}

		msg := <-sc.applyCh
		DPrintf("[%d] received command %v", sc.me, msg.Command)
		if !msg.CommandValid {
			sc.mu.Lock()
			DPrintf("[%d]  received command %v", sc.me, msg.Command)
			sc.mu.Unlock()
			continue
		}

		sc.mu.Lock()
		op := msg.Command.(Op)
		clientId := op.ClientId
		lastRequest := sc.lastRequestForClient[clientId]
		DPrintf("[%d] applying command %v with clientId %d and lastRequest %d", sc.me, op, clientId, lastRequest)
		if op.RequestId <= lastRequest {
			DPrintf("[%d]  ignoring command %v with clientId %d and lastRequest %d", sc.me, op, clientId, lastRequest)
			sc.mu.Unlock()
			continue
		}
		opType := op.Type
		switch opType {
		case JoinOp:
			sc.applyJoin(op.Servers)
		case LeaveOp:
			sc.applyLeave(op.GIDs)
		case MoveOp:
			sc.applyMove(op.GID, op.Shard)
		case QueryOp:
		}
		c, ok := sc.chanByRequestIdByClientId[clientId][op.RequestId]
		if ok {
			DPrintf("[%d]  sending response to client %d for request %d", sc.me, clientId, op.RequestId)
			c <- msg
		} else {
			DPrintf("[%d]  no channel found for client %d and request %d", sc.me, clientId, op.RequestId)
		}
		sc.lastRequestForClient[clientId] = op.RequestId
		sc.snapshot(msg.CommandIndex)
		sc.mu.Unlock()
	}
}
func (sc *ShardCtrler) snapshot(index int) {
	if sc.rf == nil || sc.lastPersistedIndex >= index {
		return
	}
	snapshot := Snapshot{
		Configs:              sc.configs,
		Gids:                 sc.gids,
		LastRequestForClient: sc.lastRequestForClient,
	}
	sc.lastPersistedIndex = index
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(snapshot)
	//size := sc.rf.GetSize()
	//if sc.rf.MaxRaftState() != -1 && size > sc.rf.MaxRaftState() {
	DPrintf("[%d] fazendo snapshot com índice %d e configs %v", sc.me, sc.lastPersistedIndex, sc.configs)
	sc.rf.Snapshot(sc.lastPersistedIndex, buffer.Bytes())
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.gids = map[int]struct{}{}
	DPrintf("[%v] starting server", sc.me)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.chanByRequestIdByClientId = make(map[int64]map[int64]chan raft.ApplyMsg) // maps requestId to channels by clientId
	sc.lastRequestForClient = make(map[int64]int64)
	data := sc.rf.ReadSnapshot()
	sc.restoreSnapshot(data)
	// Your code here.
	go sc.apply()

	return sc
}

func (sc *ShardCtrler) applyJoin(receivedServers map[int][]string) {
	DPrintf("[%d] applying join command with receivedServers %v", sc.me, receivedServers)
	DPrintf("[%d] configs before join %v", sc.me, sc.configs)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]
	groups := cloneGroups(lastConfig.Groups)
	var newGid int
	for k, v := range receivedServers {
		groups[k] = v
		newGid = k
	}
	if _, ok := sc.gids[newGid]; ok {
		msg := fmt.Sprintf("newGid %d already exists", newGid)
		panic(msg)
	}

	gids := cloneGids(lastConfig)
	gids[newGid] = struct{}{}
	shardByGroup := NShards/(len(groups)) - 1
	var newShards [NShards]int
	indexKeys := make([]int, 0, len(groups))
	for k := range groups {
		indexKeys = append(indexKeys, k)
	}
	sort.Ints(indexKeys) // força ordem determinística
	index := 0
	for i := range newShards {
		newShards[i] = indexKeys[index]
		if i == shardByGroup {
			index++
		}
	}
	config := Config{
		Num:    numConfig,
		Shards: newShards,
		Groups: groups,
	}
	sc.gids = gids
	sc.configs = append(sc.configs, config)
	DPrintf("[%d] new config after join %v with len %v", sc.me, config, len(sc.configs))
}

func (sc *ShardCtrler) applyLeave(receivedGids []int) {
	DPrintf("[%d] applying leave command with receivedServers %v", sc.me, receivedGids)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]
	DPrintf("[%d] last config %v before leave", sc.me, lastConfig)
	defer DPrintf("[%d] last config %v after leave", sc.me, lastConfig)
	groups := cloneGroups(lastConfig.Groups)
	var newGid int
	for _, v := range receivedGids {
		DPrintf("[%d] received gid %d from gids %v", sc.me, v, groups)
		delete(groups, v)
	}
	DPrintf("[%d] configs after leave %v", sc.me, sc.configs)
	sc.gids[newGid] = struct{}{}
	if len(groups) > 0 {
		DPrintf("[%d]  groups after leave %v with len %v", sc.me, groups, len(groups))
		shardByGroup := NShards/(len(groups)) - 1
		var newShards [NShards]int
		indexKeys := make([]int, 0, len(groups))
		for k := range groups {
			indexKeys = append(indexKeys, k)
		}
		index := 0
		for i := range newShards {
			newShards[i] = indexKeys[index]
			if i == shardByGroup {
				index++
			}
		}
		DPrintf("[%d] newShards after leave %v with len %v", sc.me, newShards, len(newShards))
		config := Config{
			Num:    numConfig,
			Shards: newShards,
			Groups: groups,
		}
		sc.configs = append(sc.configs, config)
		DPrintf("[%d] new config after leave %v with len %v", sc.me, config, sc.configs)
		return
	}
	var newShards [NShards]int
	newShards = [10]int{}
	config := Config{
		Num:    numConfig,
		Shards: newShards,
		Groups: map[int][]string{},
	}
	sc.configs = append(sc.configs, config)
	DPrintf("[%d] new config after leave %v with len %v with group zero", sc.me, config, len(sc.configs))
}

func (sc *ShardCtrler) applyQuery(index int) Config {
	if index == -1 || len(sc.configs) <= index {
		config := sc.configs[len(sc.configs)-1]
		return config
	}

	config := sc.configs[index]
	return config
}

func (sc *ShardCtrler) applyMove(GID int, shard int) {
	DPrintf("[%d] applying move command with GID %d and shard %d", sc.me, GID, shard)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]
	DPrintf("[%d] last config %v before move", sc.me, lastConfig)
	defer DPrintf("[%d] last config %v after move", sc.me, lastConfig)
	groups := cloneGroups(lastConfig.Groups)
	if _, ok := groups[GID]; !ok {
		msg := fmt.Sprintf("GID %d does not exist", GID)
		panic(msg)
	}
	newShards := cloneShards(lastConfig.Shards)
	newShards[shard] = GID
	config := Config{
		Num:    numConfig,
		Shards: newShards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, config)
	DPrintf("[%d] new config after move %v with len %v", sc.me, config, len(sc.configs))
}

func cloneGroups(src map[int][]string) map[int][]string {
	dst := make(map[int][]string, len(src))
	for gid, servers := range src {
		ss := make([]string, len(servers))
		copy(ss, servers)
		dst[gid] = ss
	}
	return dst
}

func cloneShards(src [NShards]int) [NShards]int {
	var dst [NShards]int
	copy(dst[:], src[:])
	return dst
}

func cloneGids(config Config) map[int]struct{} {
	src := config.Groups
	dst := make(map[int]struct{}, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}
