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
	lastRequestForKey map[int64]LastRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestFromClient := kv.lastRequestForKey[args.ClientId]
	if args.RequestId <= lastRequestFromClient.requestId {
		reply.Value = lastRequestFromClient.value
		return
	}
	value := kv.values[args.Key]
	reply.Value = value
	kv.lastRequestForKey[args.ClientId] = lastRequestFromClient
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestFromClient := kv.lastRequestForKey[args.ClientId]
	if args.RequestId <= lastRequestFromClient.requestId {
		return
	}
	kv.values[args.Key] = args.Value
	reply.Value = ""
	lastRequestFromClient = LastRequest{requestId: args.RequestId, value: args.Value}
	kv.lastRequestForKey[args.ClientId] = lastRequestFromClient
	DPrintf("value: %v", len(kv.values[args.Key]))
	DPrintf("lastRequestForKey: %v", len(kv.lastRequestForKey))
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestFromClient := kv.lastRequestForKey[args.ClientId]
	if args.RequestId <= lastRequestFromClient.requestId {
		reply.Value = lastRequestFromClient.value
		return
	}
	reply.Value = kv.values[args.Key]
	lastRequestFromClient = LastRequest{requestId: args.RequestId, value: kv.values[args.Key]}
	kv.values[args.Key] += args.Value
	kv.lastRequestForKey[args.ClientId] = lastRequestFromClient
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.values = make(map[string]string)
	kv.lastRequestForKey = make(map[int64]LastRequest)
	kv.mu = sync.Mutex{}
	return kv
}
