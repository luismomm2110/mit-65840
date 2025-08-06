package shardctrler

import (
	"6.5840/raft"
	"fmt"
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
		sc.lastRequestForClient[args.ClientId] = args.LastRequest
	}
	sc.mu.Unlock()
	DPrintf("[%d] Join command after channel with args %v", sc.me, args)
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = false
	reply.Err = OK
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
		sc.mu.Unlock()
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
		sc.lastRequestForClient[args.ClientId] = args.LastRequest
	}
	DPrintf("[%d] Leave command with args %v", sc.me, args)
	sc.mu.Unlock()
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
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

	config := sc.applyQuery(args.Num)
	reply.WrongLeader = false
	reply.Config = config
	reply.Err = OK

	return
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

func (sc *ShardCtrler) restoreSnapshot(data []byte) {
	// todo
}

func (sc *ShardCtrler) apply() {
	for {
		if !sc.killed() {
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
		DPrintf("[%d] received command %v with clientId %d and lastRequest %d", sc.me, op, clientId, lastRequest)
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
			// todo implement move
			DPrintf("[%d]  received move command %v", sc.me, op)
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
	}
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
	DPrintf("[%d] received join command with receivedServers %v", sc.me, receivedServers)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]
	groups := lastConfig.Groups
	var newGid int
	for k, v := range receivedServers {
		groups[k] = v
		newGid = k
	}
	if _, ok := sc.gids[newGid]; ok {
		msg := fmt.Sprintf("newGid %d already exists", newGid)
		panic(msg)
	}

	sc.gids[newGid] = struct{}{}
	shardByGroup := NShards/(len(lastConfig.Groups)) - 1
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
	config := Config{
		Num:    numConfig,
		Shards: newShards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) applyLeave(receivedGids []int) {
	DPrintf("[%d] received leave command with receivedServers %v", sc.me, receivedGids)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]
	groups := lastConfig.Groups
	var newGid int
	for _, v := range receivedGids {
		DPrintf("[%d] received gid %d from gids %v", sc.me, v, groups)
		delete(groups, v)
	}
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
}

func (sc *ShardCtrler) applyQuery(index int) Config {
	if index == -1 || len(sc.configs) <= index {
		config := sc.configs[len(sc.configs)-1]
		return config
	}

	config := sc.configs[index]
	return config
}
