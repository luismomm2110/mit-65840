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
	state       State
	voteCount   int
	applyCh     chan ApplyMsg
	winElectCh  chan bool
	stepDownCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool

	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
}

type RaftState struct {
	// Define your Raft state fields here
	Term int
	Vote int
	Log  []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after get state", rf.me)
	}()

	return rf.currentTerm, rf.state == Leader
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
	err := encoder.Encode(&raftState)
	if err != nil {
		panic("error encoding state")
	}
	rf.persister.Save(buffer.Bytes(), rf.persister.ReadSnapshot())
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

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// InstallSnapshot RPC args structure
type InstallSnapshotArgs struct {
	Term              int    // leader Term
	LeaderId          int    // to redirect to clients
	LastIncludedIndex int    //the snapshot replace all entries through and including this index
	LastIncludedTerm  int    //term of lastincludedindex
	Data              []byte // raw bytes of the state machine snapshot, already compacted
}

type InstallSnapshotReply struct {
	Term    int //currentTerm, to leader updateitself
	Success bool
}

//
// get the index of the last log entry.
// lock must be held before calling this.
//

// get the term of the last log entry.
// lock must be held before calling this.
func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// getLastIndex returns the index of the last log entry.
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) + rf.lastIncludedIndex - 1
}

// get the randomized election timeout.
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

// GetSize read RaftStateSize
func (rf *Raft) GetSize() int {
	return rf.persister.RaftStateSize()
}

// SnapshotAndPersist snapshot and persist the state
func (rf *Raft) SnapshotAndPersist(snapshot []byte) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: SnapshotAndPersist: lock released", rf.me)
	}()
	defer rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	DPrintf("peer %v: state machine sent command to snapshot", rf.me)
	// truncate the log
	lastIncludedIndex := rf.commitIndex
	lastIncludedTerm := rf.getLogTerm(lastIncludedIndex)
	rf.logs = rf.getLogEntriesFromStart(lastIncludedIndex)

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	DPrintf("Server %v: SnapshotAndPersist: lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, lastIncludedIndex, lastIncludedTerm)
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
	var msgs []ApplyMsg

	rf.mu.Lock()
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msgs = append(msgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(i).Command,
			CommandIndex: i,
		})
		rf.lastApplied = i
	}
	DPrintf("Raft peer %v: applyLogs: lastIndex: %v, commitIndex: %v, lastApplied: %v", rf.me, rf.getLastIndex(), rf.commitIndex, rf.lastApplied)
	rf.mu.Unlock()

	for _, msg := range msgs {
		rf.applyCh <- msg
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Server %v: RequestVote: args: %v", rf.me, args)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after RequestVote", rf.me)
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

	DPrintf("Server %v: sendRequestVote: args: %v, reply: %v", rf.me, args, reply)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after sendRequestVote", rf.me)
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

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("Server %v: InstallSnapshot: args: %v", rf.me, args)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after InstallSnapshot", rf.me)
	}()
	defer rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// save snapshot

	// if existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	reply.Success = true
	reply.Term = args.Term
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if len(rf.logs) >= args.LastIncludedIndex {
		rf.logs = rf.logs[rf.lastIncludedIndex:]
		return
	}

	// discard the entire log
	rf.logs = make([]LogEntry, 1)
	DPrintf("Server %v: InstallSnapshot processed: lastIndex: %v, lastTerm: %v, LeaderId %v", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId)

	// reset the state machine using snapshot contents
	// TODO: apply the snapshot to the state machine
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Server %v: AppendEntries: args: %v", rf.me, args)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after AppendEntries", rf.me)
	}()
	defer rf.persist()

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
	if rf.me == 2 {
		DPrintf("Server %v: AppendEntries: args.PrevLogIndex: %v, lastIndex: %v", rf.me, args.PrevLogIndex, lastIndex)
		DPrintf("Server %v: AppendEntries: args.PrevLogTerm: %v", rf.me, args.PrevLogTerm)
	}
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	// log consistency check fails, i.e. different term at prevLogIndex
	// todo não deveria ter conflito aqui
	if cfTerm := rf.getLogTerm(args.PrevLogIndex); cfTerm != args.PrevLogTerm {
		reply.ConflictTerm = cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.getLogTerm(i) == cfTerm; i-- {
			reply.ConflictIndex = i
		}
		reply.Success = false
		DPrintf("Server %v: AppendEntries: args.PrevLogTerm: %v cfterm %v", rf.me, args.PrevLogTerm, cfTerm)
		return
	}

	// only truncate log if an existing entry conflicts with a new one
	i, j := args.PrevLogIndex+1, 0
	for ; i < lastIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.getLogEntry(i).Term != args.Entries[j].Term {
			break
		}
	}
	rf.logs = rf.getLogEntriesUntilEnd(i)
	args.Entries = args.Entries[j:]
	rf.logs = append(rf.logs, args.Entries...)

	reply.Success = true
	if rf.me == 2 {
		DPrintf("Server %v: AppendEntries replied success: lastIndex: %v, term: %v", rf.me, rf.getLastIndex(), rf.currentTerm)
		DPrintf("Server %v: args.Entries: %v", rf.me, args.Entries)

	}

	// update commit index to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		go rf.applyLogs()
	}
}

// send a AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Server %v: sendAppendEntries to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after sendAppendEntries", rf.me)
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
		for ; newNextIndex >= 0; newNextIndex-- {
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
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.getLogEntry(n).Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
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
			if nextIndex >= rf.lastIncludedIndex {
				if rf.me != 1 {
					DPrintf("Server %v: sending to peer %v, nextIndex: %v, lastIndex: %v, term: %v", rf.me, server, nextIndex, rf.getLastIndex(), rf.currentTerm)
					DPrintf("Server %v: lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
				}
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
				args.LeaderCommit = rf.commitIndex
				entries := rf.getLogEntriesFromStart(nextIndex)
				args.Entries = make([]LogEntry, len(entries))
				// make a deep copy of the entries to send
				copy(args.Entries, entries)
				go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
			} else {
				go rf.sendInstallSnapshot(server)
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	DPrintf("Server %v: sendInstallSnapshot to %v", rf.me, peer)
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.getLogTerm(rf.lastIncludedIndex),
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	valid := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
	if !valid {
		return
	}
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after sendInstallSnapshot", rf.me)

	}()
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		return
	}
	DPrintf("\n\n\n")
	DPrintf("Server %v: next index to before to %v installSnapshot: %v", rf.me, peer, rf.nextIndex[peer])
	rf.nextIndex[peer] = rf.lastIncludedIndex + 1
	DPrintf("Server %v: next index to %v after installSnapshot: %v", rf.me, peer, rf.nextIndex[peer])
	DPrintf("\n\n\n")
	rf.matchIndex[peer] = args.LastIncludedIndex
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
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})

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
	DPrintf("Server %v: convertToLeader", rf.me)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after convertToLeader", rf.me)
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

	DPrintf("Server %v: convertToLeader: lastIndex: %v, term: %v", rf.me, lastIndex, rf.currentTerm)
	log.Printf("Server %v: convertToLeader: lastIndex: %v, term: %v", rf.me, lastIndex, rf.currentTerm)
	rf.broadcastAppendEntries()
}

// convert the raft state to candidate.
func (rf *Raft) convertToCandidate(fromState State) {
	DPrintf("Server %v: convertToCandidate", rf.me)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("Server %v: lock released after convertToCandidate", rf.me)
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
			case <-time.After(120 * time.Millisecond):
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

	// Your initialization code here (2A, 2B, 2C).
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
	rf.readPersist(rf.persister.ReadRaftState())

	// start the background server loop
	go rf.runServer()

	return rf
}

// index accessors
// need to be called with lock held
func (rf *Raft) getLogEntry(index int) LogEntry {
	DPrintf("Server %v: getLogEntry: index: %v, lastIncludedIndex: %v", rf.me, index, rf.lastIncludedIndex)
	DPrintf("Server %v: getLogEntry: logs: %v", rf.me, rf.logs)
	return rf.logs[index-rf.lastIncludedIndex]
}

// slice accessors
func (rf *Raft) getLogEntriesFromStart(start int) []LogEntry {
	if start < rf.lastIncludedIndex {
		return []LogEntry{}
	}
	return rf.logs[start-rf.lastIncludedIndex:]
}

func (rf *Raft) getLogEntries(start int, end int) []LogEntry {
	return rf.logs[start-rf.lastIncludedIndex : end-rf.lastIncludedIndex]
}

func (rf *Raft) getLogEntriesUntilEnd(end int) []LogEntry {
	return rf.logs[:end-rf.lastIncludedIndex]
}

// term accessors

func (rf *Raft) getLogTerm(index int) int {
	// caso ja tenha feito snapshot, o index do log é menor que o index do snapshot
	if rf.lastIncludedIndex >= index {
		return rf.lastIncludedTerm
	}
	return rf.getLogEntry(index).Term
}

func (rf *Raft) Snapshot(index int, i []byte) {

}
