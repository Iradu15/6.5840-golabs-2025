package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersionPair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu          sync.Mutex
	keyValueMap map[string]ValueVersionPair
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.keyValueMap = make(map[string]ValueVersionPair)
	return kv
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	/*
		Get returns the value and version for args.Key if args.Key
		exists; otherwise returns ErrNoKey.
	*/
	kv.mu.Lock()
	defer kv.mu.Unlock()

	tuple, ok := kv.keyValueMap[args.Key]
	if !ok {
		(*reply).Err = rpc.ErrNoKey
		return
	}

	*reply = rpc.GetReply{Value: tuple.Value, Version: tuple.Version, Err: rpc.OK}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	/*
		Update the value for a key if args.Version matches the version of the key on the server, 
		return ErrVersion otherwise.
		If the key doesn't exist, Put installs the value if the args.Version is 0, and returns ErrNoKey otherwise.
	*/
	kv.mu.Lock()
	defer kv.mu.Unlock()

	tuple, ok := kv.keyValueMap[args.Key]

	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}

	if ok && args.Version != tuple.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	reply.Err = rpc.OK
	kv.keyValueMap[args.Key] = ValueVersionPair{Value: args.Value, Version: tuple.Version + 1}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
