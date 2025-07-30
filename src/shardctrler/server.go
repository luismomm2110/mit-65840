package shardctrler

import (
	"6.5840/raft"
	"fmt"
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

	configs              []Config // indexed by config num
	gids                 map[int]struct{}
	lastCompletedRequest int64 // last completed index
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if args.LastRequest <= sc.lastCompletedRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received join command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastCompletedRequest)
		return
	}

	receivedServers := args.Servers
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
	sc.lastCompletedRequest = args.LastRequest
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if args.LastRequest <= sc.lastCompletedRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received leave command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastCompletedRequest)
		return
	}

	receivedGids := args.GIDs
	DPrintf("[%d] received leave command with receivedServers %v", sc.me, receivedGids)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]
	groups := lastConfig.Groups
	var newGid int
	for _, v := range receivedGids {
		DPrintf("[%d] received gid %d from gids %v", sc.me, v, groups)
		delete(groups, v)
	}
	sc.lastCompletedRequest = args.LastRequest
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

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if args.LastRequest <= sc.lastCompletedRequest {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("[%d] received query command with lastRequest %d <= lastCompletedRequest %d, ignoring", sc.me, args.LastRequest, sc.lastCompletedRequest)
		return
	}

	index := args.Num
	if index == -1 || len(sc.configs) <= index {
		config := sc.configs[len(sc.configs)-1]
		reply.WrongLeader = false
		reply.Err = OK
		reply.Config = config
		return
	}

	config := sc.configs[index]
	sc.lastCompletedRequest = args.LastRequest
	reply.WrongLeader = false
	reply.Config = config
	reply.Err = OK
	reply.Config = config

	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
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

	// Your code here.

	return sc
}
