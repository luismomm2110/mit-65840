package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	return fmt.Sprintf(
		"ApplyMsg{CommandValid: %t, Command: %v, CommandIndex: %d, SnapshotValid: %t, Snapshot: %v, SnapshotTerm: %d, SnapshotIndex: %d}",
		a.CommandValid, a.Command, a.CommandIndex, a.SnapshotValid, a.Snapshot, a.SnapshotTerm, a.SnapshotIndex,
	)

}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State string

const (
	Leader    State = "Leader"
	Candidate       = "Candidate"
	Follower        = "Follower"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other auxiliary states
	state         State
	voteCount     int
	applyCh       chan ApplyMsg
	winElectCh    chan bool
	stepDownCh    chan bool
	grantVoteCh   chan bool
	heartbeatCh   chan bool
	firstLogIndex int // first log index after snapshot
	applyCond     *sync.Cond

	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	// kv state
	data []byte
}

type RaftState struct {
	// Define your Raft state fields here
	Term              int
	Vote              int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
	//If, when the server comes back up, it reads the updated snapshot, but the outdated log,
	//it may end up applying some log entries that are already contained within the snapshot.
	//This happens since the commitIndex and lastApplied are not persisted, and so Raft doesn’t know that those log entries have already been applied.
	//The fix for this is to introduce a piece of persistent state to Raft that records what “real” index the first entry in Raft’s persisted log corresponds to.
	//This can then be compared to the loaded snapshot’s lastIncludedIndex to determine what elements at the head of the log to discard.
	//FirstLogIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	return rf.currentTerm, rf.state == Leader
}

type RequestInfo struct {
	RequestId     int64
	CommitedIndex int
}
type Snapshot struct {
	Values               map[string]string
	LastRequestForClient map[int64]int64
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// lock must be held before calling this.
func (rf *Raft) persist() {
	// Your code here (2C).
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	var raftState RaftState
	raftState.Term = rf.currentTerm
	raftState.Log = rf.logs
	raftState.Vote = rf.votedFor
	raftState.LastIncludedIndex = rf.lastIncludedIndex
	raftState.LastIncludedTerm = rf.lastIncludedTerm

	err := encoder.Encode(&raftState)
	if err != nil {
		panic("error encoding state")
	}
	var snapshot Snapshot
	var snapshotCopy = clone(rf.data)
	decoderoutro := gob.NewDecoder(bytes.NewReader(snapshotCopy))
	rf.persister.Save(buffer.Bytes(), rf.data)
	if len(rf.data) <= 0 {
		return
	}
	if err := decoderoutro.Decode(&snapshot); err != nil {
		log.Fatalf("Erro ao decodificar snapshot: %v", err)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return // Nothing to restore
	}
	var raftState RaftState
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := labgob.NewDecoder(&buffer)
	err := decoder.Decode(&raftState)
	if err != nil {
		log.Fatalf("Failed to decode raft state: %v", err)
	}

	rf.currentTerm = raftState.Term
	rf.logs = raftState.Log
	rf.votedFor = raftState.Vote
	rf.lastIncludedIndex = raftState.LastIncludedIndex
	rf.lastIncludedTerm = raftState.LastIncludedTerm
	rf.data = rf.persister.ReadSnapshot()
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	DPrintf("Server %d restored state with term %d lastIncludedIndex %d", rf.me, rf.currentTerm, rf.lastIncludedIndex)
	rf.PrintLog()
}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"AppendEntriesArgs{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d, Entries: %v}",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries,
	)
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (a AppendEntriesReply) String() string {
	return fmt.Sprintf(
		"AppendEntriesReply{Term: %d, Success: %t, ConflictIndex: %d, ConflictTerm: %d}",
		a.Term, a.Success, a.ConflictIndex, a.ConflictTerm,
	)
}

// InstallSnapshot RPC args structure
type InstallSnapshotArgs struct {
	Term              int    // leader Term
	LeaderId          int    // to redirect to clients
	LastIncludedIndex int    //the snapshot replace all entries through and including this index
	LastIncludedTerm  int    //term of lastincludedindex
	Data              []byte // raw bytes of the state machine snapshot, already compacted
}

func (a InstallSnapshotArgs) String() string {
	return fmt.Sprintf(
		"InstallSnapshotArgs{Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedTerm: %d, Data: %v}",
		a.Term, a.LeaderId, a.LastIncludedIndex, a.LastIncludedTerm, a.Data,
	)
}

type InstallSnapshotReply struct {
	Term         int //currentTerm, to leader updateitself
	Success      bool
	ConflitIndex int
}

func (a InstallSnapshotReply) String() string {
	return fmt.Sprintf(
		"InstallSnapshotReply{Term: %d, Success: %t}",
		a.Term, a.Success,
	)

}

// get the term of the last log entry.
// lock must be held before calling this.
func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logs[len(rf.logs)-1].Term
}

// getLastIndex returns the index of the last log entry.
func (rf *Raft) getLastIndex() int {
	//DPrintf("server %d getLastIndex lastIncludedIndex %d len(rf.logs) %d", rf.me, rf.lastIncludedIndex, len(rf.logs))
	if rf.lastIncludedIndex == 0 {
		return len(rf.logs) - 1
	}
	return rf.lastIncludedIndex + len(rf.logs)
}

// get the randomized election timeout.
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

// GetSize read RaftStateSize
func (rf *Raft) GetSize() int {
	return rf.persister.RaftStateSize()
}

// send value to an un-buffered channel without blocking
func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

// step down to follower when getting higher term,
// lock must be held before calling this.
func (rf *Raft) stepDownToFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	// step down if not follower, this check is needed
	// to prevent race where state is already follower
	if state != Follower {
		rf.sendToChannel(rf.stepDownCh, true)
	}
}

// check if the candidate's log is at least as up-to-date as ours
// lock must be held before calling this.
func (rf *Raft) isLogUpToDate(cLastIndex int, cLastTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastIndex(), rf.getLastTerm()

	if cLastTerm == myLastTerm {
		return cLastIndex >= myLastIndex
	}

	return cLastTerm > myLastTerm
}

// apply the committed logs.
func (rf *Raft) applyLogs() {
	go func() {
		for {
			if rf.killed() {
				return
			}
			// Wait until there are new logs to apply.
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				//DPrintf("server %d waiting for new logs to apply", rf.me)
				rf.applyCond.Wait()
			}
			// Apply all new logs.
			start := rf.lastApplied + 1
			end := rf.commitIndex
			var msgs []ApplyMsg
			for i := start; i <= end; i++ {
				entry := rf.getLogEntry(i)
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: i,
				})
				rf.lastApplied = i
			}
			DPrintf("server %d commit index %d", rf.me, rf.commitIndex)
			DPrintf("server %d last applied %d", rf.me, rf.lastApplied)
			rf.mu.Unlock()

			// Send the messages outside the lock.
			for _, msg := range msgs {
				DPrintf("server %d sending apply msg %v", rf.me, msg)
				rf.applyCh <- msg
			}
		}
	}()
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.sendToChannel(rf.grantVoteCh, true)
	}
}

// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	defer rf.persist()

	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		// only send once when vote count just reaches majority
		if rf.voteCount == len(rf.peers)/2+1 {
			rf.sendToChannel(rf.winElectCh, true)
		}
	}
}

// broadcast RequestVote RPCs to all peers in parallel.
// lock must be held before calling this.
func (rf *Raft) broadcastRequestVote() {
	if rf.state != Candidate {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

func decodeSnapshot(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if len(data) == 0 {
		return &snap, nil // Return an empty snapshot if no data is provided

	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&snap)
	if err != nil {
		return nil, err
	}
	return &snap, nil
}

// InstallSnapshot RPC handler.
// deve mandar applych to the service in an ApplyMsg
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("server %v received install snapshot with args %v term %v and current lastIncludedIndex %v", rf.me, args, rf.currentTerm, rf.lastIncludedIndex)
	defer func() {
		rf.mu.Unlock()
	}()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		DPrintf("server %v received install snapshot with term %d less than current term %d", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}
	rf.sendToChannel(rf.heartbeatCh, true)
	reply.Success = true
	reply.Term = args.Term

	//2025/07/17 08:15:44 server 1 received install snapshot with args InstallSnapshotArgs{Term: 8, LeaderId: 0, LastIncludedIndex: 66, LastIncludedTerm: 8, Data: [60 255 147 3 1 1 8 83 110 97 112 115 104 111 116 1 255 148 0 1 2 1 6 86 97 108 117 101 115 1 255 150 0 1 20 76 97 115 116 82 101 113 117 101 115 116 70 111 114 67 108 105 101 110 116 1 255 152 0 0 0 33 255 149 4 1 1 17 109 97 112 91 115 116 114 105 110 103 93 115 116 114 105 110 103 1 255 150 0 1 12 1 12 0 0 31 255 151 4 1 1 15 109 97 112 91 105 110 116 54 52 93 105 110 116 54 52 1 255 152 0 1 4 1 4 0 0 254 1 81 255 148 1 55 2 52 55 2 52 55 1 99 1 67 1 100 1 68 1 101 1 69 1 51 1 51 1 53 1 53 1 57 1 57 2 49 52 2 49 52 2 49 57 2 49 57 2 50 48 2 50 48 2 50 49 2 50 49 2 51 55 2 51 55 2 49 49 2 49 49 2 51 54 2 51 54 2 52 50 2 52 50 2 52 51 2 52 51 2 52 52 2 52 52 2 49 48 2 49 48 2 49 56 2 49 56 2 50 53 2 50 53 2 50 54 2 50 54 2 51 51 2 51 51 2 51 53 2 51 53 2 51 56 2 51 56 2 52 56 2 52 56 2 50 56 2 50 56 2 52 49 2 52 49 1 48 1 48 1 50 1 50 1 52 1 52 1 54 1 54 1 56 1 56 2 49 50 2 49 50 2 49 51 2 49 51 2 50 50 2 50 50 1 49 1 49 2 50 52 2 50 52 2 50 57 2 50 57 2 51 48 2 51 48 2 51 50 2 51 50 2 51 52 2 51 52 2 52 48 2 52 48 2 52 53 2 52 53 2 49 55 2 49 55 2 50 51 2 50 51 2 50 55 2 50 55 2 51 49 2 51 49 2 51 57 2 51 57 2 52 54 2 52 54 2 52 57 2 52 57 1 98 1 66 1 97 1 65 1 55 1 55 2 49 53 2 49 53 2 49 54 2 49 54 1 3 248 23 124 35 85 133 38 232 132 6 248 102 98 159 233 239 230 9 80 102 248 127 21 128 183 88 136 143 68 12 0]} term 8 and current lastIncludedIndex 46
	//If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	if rf.LogExists(args.LastIncludedIndex) && rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.logs = rf.getLogEntriesFromStart(args.LastIncludedIndex + 1)
		DPrintf("server %v logs after install snapshot %v retaining logs", rf.me, rf.logs)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.data = args.Data
		if rf.commitIndex < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
		}
		if rf.lastApplied < args.LastIncludedIndex {
			rf.lastApplied = args.LastIncludedIndex
		}
		rf.applyCh <- ApplyMsg{
			CommandIndex:  -1,
			CommandValid:  false,
			Command:       nil,
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
		return
	}

	// discard the entire log
	rf.logs = []LogEntry{}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	DPrintf("server %v logs after install snapshot %v discarding logs", rf.me, rf.logs)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.data = args.Data
	//When a follower's Raft code receives an InstallSnapshot RPC, it can use the applyCh to send the snapshot to the service in an ApplyMsg.

	snap, err := decodeSnapshot(rf.data)
	if err != nil {
		log.Fatalf("server %d: error decoding snapshot: %v", rf.me, err)
	} else {
		DPrintf("server %d: Snapshot decodificado: %+v", rf.me, snap)
	}

	argsSnapshot, err := decodeSnapshot(args.Data)
	if err != nil {
		log.Fatalf("server %d: error decoding snapshot args: %v", rf.me, err)
	} else {
		DPrintf("server %d: Snapshot args decodificado: %+v", rf.me, argsSnapshot)
	}

	// apply the snapshot message
	DPrintf("server %d sending snapshot to state machine", rf.me)
	rf.applyCh <- ApplyMsg{
		CommandIndex:  -1,
		CommandValid:  false,
		Command:       nil,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	return
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer func() {
		DPrintf("server %v size logs after append entries %v", rf.me, rf.GetSize())
		rf.mu.Unlock()
	}()
	defer rf.persist()
	//DPrintf("server %v received append entries with args %v", rf.me, args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	rf.sendToChannel(rf.heartbeatCh, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// follower log is shorter than leader
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = args.PrevLogIndex + 1
		return
	}

	// log consistency check fails, i.e. different term at prevLogIndex
	DPrintf("server %v received append entries with args %v and lastIndex %d", rf.me, args, lastIndex)
	if cfTerm := rf.getLogTerm(args.PrevLogIndex); cfTerm != args.PrevLogTerm {
		reply.ConflictTerm = cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.getLogTerm(i) == cfTerm; i-- {
			if i == rf.lastIncludedIndex {
				reply.ConflictTerm = rf.lastApplied
				reply.ConflictIndex = i
				reply.Success = false
				return
			}
			DPrintf("server %v checking conflit in index %v and lastIncludedIndex %v", rf.me, i, rf.lastIncludedIndex)
			reply.ConflictIndex = i
		}
		reply.Success = false
		return
	}

	// only truncate log if an existing entry conflicts with a new one
	truncatedIndex, j := args.PrevLogIndex+1, 0
	for ; truncatedIndex < lastIndex+1 && j < len(args.Entries); truncatedIndex, j = truncatedIndex+1, j+1 {
		if rf.getLogTerm(truncatedIndex) != args.Entries[j].Term {
			break
		}
	}
	rf.logs = rf.getLogEntriesUntilEnd(truncatedIndex)
	args.Entries = args.Entries[j:]
	rf.logs = append(rf.logs, args.Entries...)

	reply.Success = true

	DPrintf("server %v received and sucessfuly appended entries with args %v", rf.me, args)
	// update commit index to min(leaderCommit, lastIndex)
	DPrintf("server %v commitIndex %d leaderCommit %d lastIndex %d", rf.me, rf.commitIndex, args.LeaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		DPrintf("server %v commitIndex updated to %d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
}

// send a AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		return
	}

	// update matchIndex and nextIndex of the follower
	DPrintf("server %v received append entries reply from server %v with reply %v", rf.me, server, reply)
	if reply.Success {
		// match index should not regress in case of stale rpc response
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		// follower's log shorter than leader's
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// try to find the conflictTerm in log
		newNextIndex := rf.getLastIndex()
		DPrintf("server %d reply %s", rf.me, reply)
		for ; newNextIndex >= 0; newNextIndex-- {
			if newNextIndex == rf.lastIncludedIndex {
				newNextIndex = -1
				break
			}
			if rf.getLogTerm(newNextIndex) == reply.ConflictTerm {
				break
			}
		}
		// if not found, set nextIndex to conflictIndex
		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = newNextIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	DPrintf("server %v next index to server %v is %d", rf.me, server, rf.nextIndex[server])
	DPrintf("server %v match index to server %v is %d", rf.me, server, rf.matchIndex[server])
	DPrintf("server %v commit index %d and last index %d from reply of %v", rf.me, rf.commitIndex, rf.getLastIndex(), server)
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		DPrintf("server %d with index %d with log term %d (matching indexes after reply from %d)", rf.me, n, rf.getLogTerm(n), server)
		if rf.getLogTerm(n) == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				DPrintf("server %d with match index %d to %v", i, rf.matchIndex[i], server)
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		//DPrintf("server %d with count %d and len %d after reply %v from %v", rf.me, count, len(rf.peers)/2, reply, server)
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			//DPrintf("server %d commitIndex updated to %d as leader", rf.me, rf.commitIndex)
			rf.applyCond.Signal()
			break
		}
	}
}
func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// broadcast AppendEntries RPCs to all peers in parallel.
// lock must be held before calling this.
func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			nextIndex := rf.nextIndex[server]
			// aqui preciso mandar se for maior ou igual mas também se lastIncludedIndex  for igual a zero
			if nextIndex > rf.lastIncludedIndex || rf.lastIncludedIndex == 0 {
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
				args.LeaderCommit = rf.commitIndex
				entries := rf.getLogEntriesFromStart(nextIndex)
				// aqui tá com erro, parece que o primeiro depois de cortar não tá indo
				args.Entries = make([]LogEntry, len(entries))
				// make a deep copy of the entries to send
				copy(args.Entries, entries)
				//DPrintf("server %v sending append entries with args %v to server %v", rf.me, args, server)
				go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
			} else {
				go rf.sendInstallSnapshot(server)
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.data,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	decodedData, err := decodeSnapshot(args.Data)
	if err != nil {
		log.Fatalf("server %d: error decoding snapshot data: %v", rf.me, err)
	}
	DPrintf("server %v sending install snapshot to server %v with args %v and data %v", rf.me, peer, args, decodedData)
	valid := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
	//DPrintf("server %v received install snapshot reply from server %v with reply %v and it valid %v", rf.me, peer, reply, valid)
	if !valid {
		return
	}
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	if reply.Term > rf.currentTerm {
		// todo aqui pode ser que tenha que ver após o return se é líderr ainda
		//DPrintf("server %v received install snapshot reply from server %v with term %d greater than current term %d", rf.me, peer, reply.Term, rf.currentTerm)
		rf.stepDownToFollower(reply.Term)
		return
	}
	rf.nextIndex[peer] = rf.lastIncludedIndex + 1
	time.Sleep(50 * time.Millisecond)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	if rf.killed() {
		return -1, rf.currentTerm, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader {
		DPrintf("Raft: server %v is not leader %v", rf.me, rf.currentTerm)
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	DPrintf("Raft: server %v received command %v", rf.me, command)
	rf.logs = append(rf.logs, LogEntry{term, command})
	// size of logs
	DPrintf("Raft: server %v size of logs after receive %v", rf.me, rf.GetSize())

	return rf.getLastIndex(), term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// convert the raft state to leader.
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	defer rf.persist()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}
	DPrintf("server %d elected leader in term %d", rf.me, rf.currentTerm)

	rf.broadcastAppendEntries()
}

// convert the raft state to candidate.
func (rf *Raft) convertToCandidate(fromState State) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	defer rf.persist()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1

	rf.broadcastRequestVote()
}

// reset the channels, needed when converting server state.
// lock must be held before calling this.
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

// main server loop.
func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-time.After(30 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Candidate)
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.data = []byte{}
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(rf.persister.ReadRaftState())

	// start the background server loop
	go rf.runServer()

	// start the apply thread
	rf.applyLogs()

	return rf
}

// need to be called with lock held
func (rf *Raft) LogExists(index int) bool {
	if rf.lastIncludedIndex != 0 {
		return index > rf.lastIncludedIndex && index < len(rf.logs)+rf.lastIncludedIndex
	}
	return index >= 1 && index < len(rf.logs)-1
}

// index accessors
// need to be called with lock held
func (rf *Raft) getLogEntry(index int) LogEntry {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("server %v panic in getLogEntry with index %d", rf.me, index)
			rf.PrintLog()
			panic(r)
		}
	}()
	if rf.lastIncludedIndex == 0 {
		return rf.logs[index]
	}
	return rf.logs[index-rf.lastIncludedIndex-1]
}

func (rf *Raft) getLogLength() int {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("server %v panic in getLogLength", rf.me)
			rf.PrintLog()
			panic(r)
		}
	}()
	if rf.lastIncludedIndex == 0 {
		return len(rf.logs) - 1
	}
	return len(rf.logs) + rf.lastIncludedIndex
}

func (rf *Raft) getLogEntriesFromStart(start int) []LogEntry {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("server %v panic in getLogEntriesFromStart with start %d", rf.me, start)
			rf.PrintLog()
			panic(r)
		}
	}()
	//DPrintf("server %v getLogEntriesFromStart start %d len %d last included index %d",
	//	rf.me, start, len(rf.logs), rf.lastIncludedIndex)
	if rf.lastIncludedIndex == 0 {
		return rf.logs[start:]
	} else if start <= rf.lastIncludedIndex {
		return []LogEntry{}
	}
	return rf.logs[start-rf.lastIncludedIndex-1:]
}

func (rf *Raft) getLogsEntriesFromStart(start int) []LogEntry {
	defer func() {
		if r := recover(); r != nil {
			//DPrintf("server %v panic in getLogEntriesFromStart with start %d", rf.me, start)
			//rf.PrintLog()
			panic(r)
		}
	}()
	//DPrintf("server %v getLogEntriesFromStart start %d len %d last included index %d",
	//	rf.me, start, len(rf.logs), rf.lastIncludedIndex)
	if rf.lastIncludedIndex == 0 {
		return rf.logs[start+1:]
	} else if start <= rf.lastIncludedIndex {
		return []LogEntry{}
	}
	return rf.logs[start-rf.lastIncludedIndex:]
}

func (rf *Raft) getLogEntriesUntilEnd(end int) []LogEntry {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("server %v panic in getLogEntriesUntilEnd with end %d", rf.me, end)
			//rf.PrintLog()
			panic(r)
		}
	}()
	//DPrintf("server %v getLogEntriesUntilEnd end %d len %d last included index %d",
	//	rf.me, end, len(rf.logs), rf.lastIncludedIndex)
	if rf.lastIncludedIndex == 0 {
		return rf.logs[:end]
	}
	if end <= rf.lastIncludedIndex {
		return []LogEntry{}
	}
	adjustedEnd := end - rf.lastIncludedIndex - 1
	return rf.logs[:adjustedEnd]
}

// term accessors

func (rf *Raft) getLogTerm(index int) int {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("server %v panic in getLogTerm with index %d", rf.me, index)
			DPrintf("Server %d last index is %d", rf.me, rf.getLastIndex())
			rf.PrintLog()
			panic(r)
		}
	}()

	//DPrintf("server %v getLogTerm index %d lastIncludedIndex %d len logs %d",
	//	rf.me, index, rf.lastIncludedIndex, len(rf.logs))
	if rf.lastIncludedIndex != 0 && index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.getLogEntry(index).Term
}

func (rf *Raft) Snapshot(index int, i []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if index > rf.commitIndex {
		DPrintf("server %v snapshot index %d greater than commit index %d", rf.me, index, rf.commitIndex)
		panic("snapshot index greater than commit index")
	}
	if index <= rf.lastIncludedIndex {
		return
	}
	DPrintf("server %v received snapshot index: %d, current last included index %d, commit index %d", rf.me, index, rf.lastIncludedIndex, rf.commitIndex)
	//DPrintf("server %v logs before snapshot", rf.me)
	rf.PrintLog()
	rf.data = i
	rf.lastIncludedTerm = rf.getLogTerm(index)
	rf.logs = rf.getLogsEntriesFromStart(index)
	DPrintf("server %v LOGS SIZE %d bytes", rf.me, rf.GetLogSizeInBytes()) //DPrintf("server %v logs after snapshot", rf.me)
	//rf.PrintLog()
	rf.lastIncludedIndex = index
}

func (rf *Raft) PrintLog() {
	DPrintf("server %v printing logs", rf.me)
	for i, entry := range rf.logs {
		if rf.lastIncludedIndex != 0 {
			i++
		}
		DPrintf("server %v Index: %d, rf.me, Term: %d, Command: %v (printing log)", rf.me, i+rf.lastIncludedIndex, entry.Term, entry.Command)
	}
	DPrintf("server %v len of logs: %d", rf.me, len(rf.logs))
}
func (rf *Raft) GetLogSizeInBytes() int {
	size := 0
	for _, entry := range rf.logs {
		size += binary.Size(entry)
	}
	return size
}
