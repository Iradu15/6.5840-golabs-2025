package rsm

import (
	"math/rand/v2"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	Me int
	Id int64
	Req any 
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	results map[int64]any // op id -> DoOp result
	dead bool
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		results:      make(map[int64]any),
		dead: 		  false,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.Reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
// 
// If the reader applies different op at that index, the op got overwritten
// 
// When applyCh closes, the reader should wake every waiting Submit with ErrWrongLeader. TestShutdown4A checks this.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// To help the reader goroutine match applyCh messages with waiting calls to rsm.Submit(), Submit() 
	// should wrap each client operation in an Op structure along with a unique identifier. Submit() should then wait
	//  until the operation has committed and been executed, and return the result of execution (the value returned 
	// by DoOp())

	id := rand.Int64()
	op := Op{Me:rsm.me, Id: id, Req: req}

	_, initialTerm, isLeader := rsm.Raft().Start(op)
	if !isLeader{
		return rpc.ErrWrongLeader, nil
	}

	for true{
		ms := 10 + (rand.Int64() % 10)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Just tells the client: Can't confirm
		rsm.mu.Lock()
	
		res, ok := rsm.results[op.Id]
		if ok{
			delete(rsm.results, op.Id)
			rsm.mu.Unlock()

			return rpc.OK, res
		}
		
		// this should stay aftet checking the result is in
		// what if the leader just lost leadership but command was applied
		term, isLeader := rsm.Raft().GetState()
		if !isLeader || term != initialTerm{	
			// It can be the case the op was commited, but leader changed OR op was not
			// commited and leader changed, you do NOT know if it will be applied shortly. 

			// Returning an error from Submit doesn't undo anything in Raft. 
			// Once committed, the op is applied on every peer.
			// The client hears ErrWrongLeader, retries on the new leader, and the op runs a second time.
			// If it waited "because it will probably be applied soon", a partitioned ex-leader
			// whose op never committed would wait forever.
			rsm.mu.Unlock()		
			break
		}

		if rsm.dead{
			rsm.mu.Unlock()
			break
		}
		
		rsm.mu.Unlock()	
	}
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

// And as committed operations appear on the applyCh, each needs to be executed by the service, 
// and the results need to be handed to the goroutine that called raft.Start() so that it can 
// return the result to the client. 
// 
// The reader goroutine should hand each committed operation to DoOp(); DoOp()'s return
//  value should be given to the corresponding rsm.Submit() call for it to return
func (rst * RSM) Reader() {
	for msg := range rst.applyCh{
		if !msg.CommandValid{
			rst.mu.Lock()
			rst.dead=true
			rst.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)		
		// apply the request via the service, store result
		res := rst.sm.DoOp(op.Req)

		rst.mu.Lock()
		rst.results[op.Id] = res
		rst.mu.Unlock()
	}
}