package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastRequest int64
	me          int64 // unique client identifier
	leader      int   // current leader index, used for retrying
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastRequest = 0
	// Your code here.
	ck.me = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.LastRequest = ck.lastRequest + 1
	// Your code here.
	args.Num = num
	args.ClientId = ck.me
	leader := ck.leader
	servers := len(ck.servers)
	DPrintf("[%d] Query called with args %v", ck.me, args)
	for {
		// try each known server.
		for offset := 0; offset < servers; offset++ {
			server := (leader + offset) % servers
			reply := QueryReply{}
			ok := make(chan bool)
			DPrintf("[%d] Query called with args %v, reply %v to server %d", ck.me, args, reply, server)
			go func() {
				ok <- ck.servers[server].Call("ShardCtrler.Query", args, &reply)
			}()
			select {
			case <-ok:
				{
					DPrintf("[%d] Query reply %v from server %d", ck.me, reply, server)
					if reply.Err == OK {
						ck.leader = server                // update leader to the server that successfully processed the request
						ck.lastRequest = args.LastRequest // update last request
						DPrintf("[%d] Query succeeded with args %v, reply %v", ck.me, args, reply)
						return reply.Config
					}
					if reply.Err == WrongLeader {
						DPrintf("[%d] Query failed with wrong leader, retrying", ck.me)
						time.Sleep(100 * time.Millisecond)
						continue // try next server
					} else {
						DPrintf("[%d] Query failed with error %v, retrying", ck.me, reply.Err)
						continue // try next server
					}
				}
			case <-time.After(100 * time.Millisecond):
				{
					DPrintf("[%d] Query timed out for args %v, retrying", ck.me, args)
					// If we timeout, we can try the next server.
					continue // try next server
				}
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// todo retry
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.LastRequest = ck.lastRequest + 1
	args.ClientId = ck.me
	numServers := len(ck.servers)
	start := ck.leader

	for {
		for offset := 0; offset < numServers; offset++ {
			server := (start + offset) % numServers
			var reply JoinReply
			ok := make(chan bool)
			// try each known server.
			DPrintf("[%d] Join called with args %v, reply %v to server %d", ck.me, args, reply, server)
			go func() {
				ok <- ck.servers[server].Call("ShardCtrler.Join", args, &reply)
			}()
			select {
			case <-ok:
				DPrintf("Shard controller: [%d] Join reply %v from server %d", ck.me, reply, server)
				if reply.Err == OK {
					ck.leader = server // update leader to the server that successfully processed the request
					DPrintf("Shard controller: [%d] Join succeeded with args %v, reply %v", ck.me, args, reply)
					ck.lastRequest = args.LastRequest // update last request
					return
				} else if reply.Err == WrongLeader {
					DPrintf("[%d] Join failed with wrong leader, retrying", ck.me)
					time.Sleep(100 * time.Millisecond)
				} else {
					DPrintf("[%d] Join failed with error %v, retrying", ck.me, reply.Err)
				}
			case <-time.After(20 * time.Millisecond):
				{
					DPrintf("[%d] Join timed out for args %v, retrying", ck.me, args)
					continue
				}
			}

		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.LastRequest = ck.lastRequest + 1
	args.ClientId = ck.me

	// retry logic
	leader := ck.leader
	servers := len(ck.servers)

	for {
		// try each known server.
		for offset := 0; offset < servers; offset++ {
			var reply LeaveReply
			srv := ck.servers[(leader+offset)%servers]
			DPrintf("[%d] Leave called with args %v, reply %v", ck.me, args, reply)
			ok := make(chan bool)
			go func() {
				ok <- srv.Call("ShardCtrler.Leave", args, &reply)
			}()
			select {
			case <-ok:
				DPrintf("[%d] Leave reply %v from server %d", ck.me, reply, (leader+offset)%servers)
				if reply.Err == OK {
					ck.leader = (leader + offset) % servers // update leader to the server that successfully processed the request
					DPrintf("[%d] Leave succeeded with args %v, reply %v", ck.me, args, reply)
					ck.lastRequest = args.LastRequest // update last request
					return
				}
				if reply.Err == WrongLeader {
					DPrintf("[%d] Leave failed with wrong leader, retrying", ck.me)
					continue // try next server
				} else {
					DPrintf("[%d] Leave failed with error %v, retrying", ck.me, reply.Err)
					continue // try next server
				}
			case <-time.After(100 * time.Millisecond):
				DPrintf("[%d] Leave timed out for args %v, retrying", ck.me, args)
				// If we timeout, we can try the next server.
				continue // try next server
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.LastRequest = ck.lastRequest + 1
	args.ClientId = ck.me

	// retry logic
	leader := ck.leader
	servers := len(ck.servers)

	for {
		// try each known server.
		for offset := 0; offset < servers; offset++ {
			var reply MoveReply
			srv := ck.servers[(leader+offset)%servers]
			DPrintf("[%d] Move called with args %v, reply %v", ck.me, args, reply)
			// make the RPC call
			ok := make(chan bool)
			go func() {
				ok <- srv.Call("ShardCtrler.Move", args, &reply)
			}()
			// wait for the reply
			select {
			case <-ok:
				DPrintf("[%d] Move reply %v from server %d", ck.me, reply, (leader+offset)%servers)
				if reply.Err == OK {
					ck.leader = (leader + offset) % servers // update leader to the server that successfully processed the request
					DPrintf("[%d] Move succeeded with args %v, reply %v", ck.me, args, reply)
					ck.lastRequest = args.LastRequest // update last request
					return
				}
				if reply.Err == WrongLeader {
					DPrintf("[%d] Move failed with wrong leader, retrying", ck.me)
					continue // try next server
				} else {
					DPrintf("[%d] Move failed with error %v, retrying", ck.me, reply.Err)
					continue // try next server
				}
			case <-time.After(100 * time.Millisecond):
				DPrintf("[%d] Move timed out for args %v, retrying", ck.me, args)
				// If we timeout, we can try the next server.
				continue // try next server
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
