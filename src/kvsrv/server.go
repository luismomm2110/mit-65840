package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LastRequest struct {
	requestId int64
	value     string
}

type KVServer struct {
	mu     sync.Mutex
	values map[string]string
	// Your definitions here.
	lastRequestForClient map[int64]LastRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestFromClient := kv.lastRequestForClient[args.ClientId]
	if args.RequestId <= lastRequestFromClient.requestId {
		reply.Value = lastRequestFromClient.value
		return
	}
	value := kv.values[args.Key]
	reply.Value = value
	kv.lastRequestForClient[args.ClientId] = lastRequestFromClient
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.values[args.Key] = args.Value
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestFromClient := kv.lastRequestForClient[args.ClientId]
	if args.RequestId <= lastRequestFromClient.requestId {
		reply.Value = lastRequestFromClient.value
		return
	}
	reply.Value = kv.values[args.Key]
	lastRequestFromClient = LastRequest{requestId: args.RequestId, value: kv.values[args.Key]}
	kv.values[args.Key] += args.Value
	kv.lastRequestForClient[args.ClientId] = lastRequestFromClient
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.values = make(map[string]string)
	kv.lastRequestForClient = make(map[int64]LastRequest)
	kv.mu = sync.Mutex{}
	return kv
}
