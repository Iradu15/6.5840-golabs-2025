package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	/*
		Fetches the current value and version for a key.
		Returns:
			- ErrNoKey if the key does not exist.
			- It keeps trying forever in the face of all other errors until reply is received.
	*/
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if !ok {
			continue
		}

		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}

		return reply.Value, reply.Version, reply.Err
	}
}

// Updates key with value only if request.version = server.version, returns ErrVersion otherwise.
// If Put receives an ErrVersion on its first RPC, Put should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a resend RPC, then Put must return ErrMaybe to
// the application, since its earlier RPC might have been processed by the server successfully but the response was
// lost, and the Clerk doesn't know if the Put was performed or not.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	retry := false

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			err := reply.Err
			if retry && reply.Err == rpc.ErrVersion {
				err = rpc.ErrMaybe
			}
			return err
		}

		retry = true
		time.Sleep(100 * time.Millisecond)
	}

}
