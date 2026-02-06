package shardctrler

import (
	"fmt"
	"log"
)

//
// ShardId controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// um servidor tem um GID, fixo, o shard para cada gid muda  e cada gid vai ter um numero de servers (raft)

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) String() string {
	return fmt.Sprintf("Num: %d, Shards: %v, Groups: %v", c.Num, c.Shards, c.Groups)
}

const (
	OK          = "OK"
	WrongLeader = "WrongLeader"
)

type Err string

type JoinArgs struct {
	Servers     map[int][]string // new GID -> servers mappings
	LastRequest int64            // for deduplication
	ClientId    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

// toString of Join Reply

type LeaveArgs struct {
	GIDs        []int
	LastRequest int64 // for deduplication
	ClientId    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard       int
	GID         int
	LastRequest int64 // for deduplication
	ClientId    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num         int   // desired config number
	LastRequest int64 // for deduplication
	ClientId    int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	// set flag for miliseconds since epoch

	if Debug {
		log.Printf(format, a...)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}
	return
}
