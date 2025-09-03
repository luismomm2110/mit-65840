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

	configs                   []Config        // indexed by config num
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
	<-c
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = false
	sc.lastRequestForClient[args.ClientId] = args.LastRequest
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
	sc.lastRequestForClient = snapshot.LastRequestForClient
	DPrintf("[%d] restored snapshot with configs %v, lastRequestForClient %v", sc.me, sc.configs, sc.lastRequestForClient)
}

func (sc *ShardCtrler) apply() {
	for {
		if sc.killed() {
			return
		}

		msg := <-sc.applyCh
		DPrintf("[%d] received command %v", sc.me, msg.Command)
		if msg.SnapshotValid {
			sc.mu.Lock()
			DPrintf("[%d] received command invalid %v", sc.me, msg)
			sc.restoreSnapshot(msg.Snapshot)
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
		LastRequestForClient: sc.lastRequestForClient,
	}
	sc.lastPersistedIndex = index
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(snapshot)
	//size := sc.rf.GetSize()
	//if sc.rf.MaxRaftState() != -1 && size > sc.rf.MaxRaftState() {
	//DPrintf("[%d] fazendo snapshot com índice %d e configs %v", sc.me, sc.lastPersistedIndex, sc.configs)
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
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]

	// 1) Clonar e adicionar novos grupos; ignorar duplicados
	groups := cloneGroups(lastConfig.Groups)
	added := false
	for gid, servers := range receivedServers {
		if _, exists := groups[gid]; !exists {
			groups[gid] = servers
			added = true
		}
	}
	// Se nada novo foi adicionado, é no-op (mantém estabilidade)
	if !added {
		config := Config{
			Num:    numConfig,
			Shards: lastConfig.Shards,
			Groups: groups,
		}
		sc.configs = append(sc.configs, config)
		DPrintf("[%d] join no-op (no new groups). new config %v", sc.me, config)
		return
	}

	// 2) Ordenar GIDs para determinismo
	indexKeys := make([]int, 0, len(groups))
	for gid := range groups {
		indexKeys = append(indexKeys, gid)
	}
	sort.Ints(indexKeys)

	// 3) Calcular alvo por grupo (floor/ceil)
	var newShards [NShards]int
	copy(newShards[:], lastConfig.Shards[:])

	G := len(indexKeys)
	if G == 0 {
		// Nenhum grupo: tudo 0
		config := Config{
			Num:    numConfig,
			Shards: [NShards]int{},
			Groups: map[int][]string{},
		}
		sc.configs = append(sc.configs, config)
		DPrintf("[%d] new config after join (no groups) %v", sc.me, config)
		return
	}
	base := NShards / G
	rem := NShards % G
	perGroup := make(map[int]int, G)
	for i, gid := range indexKeys {
		perGroup[gid] = base
		if i < rem {
			perGroup[gid]++
		}
	}

	// 4) Contagem atual por grupo + coletar shards não atribuídos (gid==0 ou gid removido)
	count := make(map[int]int, G)
	unassigned := make([]int, 0)
	for i, gid := range lastConfig.Shards {
		if _, ok := groups[gid]; ok && gid != 0 {
			count[gid]++
		} else {
			unassigned = append(unassigned, i)
		}
	}

	// 5) Preencher primeiro com shards não atribuídos -> grupos abaixo do alvo
	uIdx := 0 // índice em indexKeys do próximo grupo que ainda precisa receber
	for _, sIdx := range unassigned {
		for uIdx < G {
			g := indexKeys[uIdx]
			if count[g] < perGroup[g] {
				newShards[sIdx] = g
				count[g]++
				if count[g] == perGroup[g] {
					uIdx++
				}
				break
			}
			uIdx++
		}
		// Se todos bateram alvo antes de consumir unassigned, sobra fica como está (mas perGroup soma NShards, então tende a fechar)
	}

	// 6) Montar listas de over/under pós-preenchimento
	over := make([]int, 0)
	under := make([]int, 0)
	for _, gid := range indexKeys {
		if count[gid] > perGroup[gid] {
			over = append(over, gid)
		} else if count[gid] < perGroup[gid] {
			under = append(under, gid)
		}
	}

	// 7) Se ainda houver under, mover de over -> under minimizando movimentação
	// Determinístico: percorre shards 0..NShards-1, over/under em ordem crescente
	uPos := 0
	for _, gOver := range over {
		for i := 0; i < NShards && uPos < len(under); i++ {
			if count[gOver] == perGroup[gOver] {
				break
			}
			if newShards[i] != gOver {
				continue
			}
			// avançar até um under que ainda precise
			for uPos < len(under) && count[under[uPos]] >= perGroup[under[uPos]] {
				uPos++
			}
			if uPos >= len(under) {
				break
			}
			gUnder := under[uPos]

			// mover i: gOver -> gUnder
			newShards[i] = gUnder
			count[gOver]--
			count[gUnder]++
			if count[gUnder] == perGroup[gUnder] {
				uPos++
			}
		}
		if uPos >= len(under) {
			break
		}
	}

	// 8) Persistir nova configuração
	config := Config{
		Num:    numConfig,
		Shards: newShards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, config)
	DPrintf("[%d] new config after join %v", sc.me, config)
}

func (sc *ShardCtrler) applyLeave(receivedGids []int) {
	DPrintf("[%d] applying leave command with receivedGids %v", sc.me, receivedGids)
	numConfig := len(sc.configs)
	lastConfig := sc.configs[numConfig-1]

	// 1) Remover grupos que saem
	groups := cloneGroups(lastConfig.Groups)
	for _, gid := range receivedGids {
		delete(groups, gid)
	}

	// Caso sem grupos -> zera tudo
	if len(groups) == 0 {
		var newShards [NShards]int
		config := Config{
			Num:    numConfig,
			Shards: newShards,
			Groups: map[int][]string{},
		}
		sc.configs = append(sc.configs, config)
		DPrintf("[%d] new config after leave (no groups) %v", sc.me, config)
		return
	}

	// 2) Alvo por grupo (floor/ceil) de forma determinística
	indexKeys := make([]int, 0, len(groups))
	for gid := range groups {
		indexKeys = append(indexKeys, gid)
	}
	sort.Ints(indexKeys)

	G := len(indexKeys)
	base := NShards / G
	rem := NShards % G
	perGroup := make(map[int]int, G)
	for i, gid := range indexKeys {
		perGroup[gid] = base
		if i < rem {
			perGroup[gid]++
		}
	}

	// 3) Contar a distribuição atual (apenas dos grupos remanescentes)
	var newShards [NShards]int
	copy(newShards[:], lastConfig.Shards[:])

	count := make(map[int]int, G)
	unassigned := make([]int, 0) // índices de shards que pertenciam a grupos removidos
	for i, gid := range lastConfig.Shards {
		if _, ok := groups[gid]; ok {
			count[gid]++
		} else {
			unassigned = append(unassigned, i)
		}
	}

	// 4) Preencher primeiro usando apenas os shards "órfãos"
	//    Preenchemos grupos "magros" na ordem de indexKeys e shards por índice crescente.
	underIdx := 0 // índice em indexKeys do próximo grupo que ainda precisa receber
	for _, idx := range unassigned {
		// avançar até encontrar um grupo que ainda precise
		for underIdx < G {
			gid := indexKeys[underIdx]
			if count[gid] < perGroup[gid] {
				newShards[idx] = gid
				count[gid]++
				if count[gid] == perGroup[gid] {
					underIdx++
				}
				break
			}
			underIdx++
		}
		// se todos bateram alvo antes de consumir todos órfãos, sobras continuarão reassinadas
		// abaixo quando checarmos over/under (mas em teoria não sobra se perGroup soma NShards).
	}

	// 5) Verificar se ainda há desequilíbrio (grupos over/under)
	over := make([]int, 0)
	under := make([]int, 0)
	for _, gid := range indexKeys {
		if count[gid] > perGroup[gid] {
			over = append(over, gid)
		} else if count[gid] < perGroup[gid] {
			under = append(under, gid)
		}
	}

	// 6) Se ainda faltar, mover do(s) over para under
	//    Determinístico: iterate shards 0..NShards-1; over e under em ordem crescente.
	uPos := 0
	for _, gOver := range over {
		for i := 0; i < NShards && uPos < len(under); i++ {
			if count[gOver] == perGroup[gOver] {
				break
			}
			if newShards[i] != gOver {
				continue
			}
			// achar próximo under que ainda precise
			for uPos < len(under) && count[under[uPos]] >= perGroup[under[uPos]] {
				uPos++
			}
			if uPos >= len(under) {
				break
			}
			gUnder := under[uPos]

			// move i de gOver -> gUnder
			newShards[i] = gUnder
			count[gOver]--
			count[gUnder]++
			if count[gUnder] == perGroup[gUnder] {
				uPos++
			}
		}
		if uPos >= len(under) {
			break
		}
	}

	// 7) Montar nova configuração
	config := Config{
		Num:    numConfig,
		Shards: newShards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, config)
	DPrintf("[%d] new config after leave %v", sc.me, config)
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
		msg := fmt.Sprintf("[%d] GID %d does not exist", sc.me, GID)
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
