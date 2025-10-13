package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	kvtest "6.5840/kvtest1"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *tester.Persister   // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	numberOfPeers   int
	electionTimeout int64
	majority        int

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm  int
	VotedFor     int // prevent a candidate voting for a, rebooting and then voting for b in the same term
	VotedForTerm int
	Log          []LogEntry // what should this contain?
	State        State

	CommitIndex   int
	CommitIndexCh chan int

	LastApplied   int
	LastAppliedCh chan int

	applyCh         chan raftapi.ApplyMsg
	requestVoteCh   chan bool
	appendEntriesCh chan bool
	heartBeatCh     chan bool

	stop chan bool // used to stop elections and convert back to follower

	// only for leader
	NextIndex  []int
	MatchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, (rf.State == Leader)
}

//

func (rf *Raft) Heartbeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("%v is receiving heartbeat", rf.me)
	rf.mu.Lock()
	rf.VotedFor = -1
	rf.mu.Unlock()
	select {
	case rf.heartBeatCh <- true:
	default:
	}

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	/*
		RequestVote RPC handler
		1. Reply false if term < currentTerm (§5.1)
		2. If votedFor is null or candidateId, and candidate’s log is at least as
		up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	*/
	log.Printf("RequestVote received by %v from %v", rf.me, args.Id)

	select {
	case rf.requestVoteCh <- true:
	default:
	}

	rf.mu.Lock()
	if rf.moreUpToDate(args.Term, args.LastLogIndex) && (rf.VotedFor == -1 || rf.VotedForTerm != rf.CurrentTerm) {
		reply.VoteGranted = true
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.Id
		rf.VotedForTerm = rf.CurrentTerm
		log.Printf("Vote granted by %v to %v", rf.me, args.Id)

	} else {
		reply.VoteGranted = false
		log.Printf("Vote denied by %v to %v", rf.me, args.Id)
	}
	reply.CurrentTerm = rf.CurrentTerm
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
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
//
//	func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//		return ok
//	}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan RequestReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	select {
	case ch <- RequestReply{ok, reply}:
	default:
	}
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	return ok
}

func (rf *Raft) lead() {
	/*
		send initial empty AppendEntries RPCs
		(heartbeat) to each server; repeat during idle
		periods to prevent election timeouts
	*/

	for !rf.killed() {
		log.Printf("%v is leading in term %v", rf.me, rf.CurrentTerm)
		rf.broadcastHeartbeats()

		ms := 15 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	/*
		Goroutine that constantly checks wether a leader election should be started
	*/

	count := 1
	for !rf.killed() {
		// log.Printf("Ticker counter for %v: %v", rf.me, count)

		/*
			if election timeout elapses without receiving AppendEntries
			RPC from current leader or granting vote to candidate:
			convert to candidate
		*/

		// !!! mechanism to make leader not start an election while waiting
		select {
		case <-rf.heartBeatCh:
			// !!!! DOES NOT REACH HERE, BUT IT ENTERS THE Heartbeat endpoint
			// TICKER COUNTER GETS ONLY TO 1
			log.Printf("%v received heartbeat", rf.me)
		case <-rf.appendEntriesCh:
			log.Printf("%v received appendEntry", rf.me)
			select {
			case rf.stop <- true:
			default:
			}
		case <-rf.requestVoteCh:
			// NOTHING GETS HERE
			log.Printf("%v received requestVote", rf.me)
		case <-time.After(kvtest.ElectionTimeout + time.Duration(rf.electionTimeout)*time.Millisecond):
			go rf.startElection()
		}
		log.Printf("%v PULA PULA past select", rf.me)
		// Your code here (3A)
		// Check if a leader election should be started.
		/*
			Basically check if it took longer that electionTimeout(from kvtest.go) since the last heartbeat
			Election is started this way:
				- increment current term
				- convert to candidate
				- vote for itself and issue RequestVote RPC to all other peers(servers)
			3 things can happen:
				- wins election
					receives majority votes of all servers of the cluster for the same given term
					if wins, then sends heartbeat messages to all other servers to establish its authority
				- another server establishes as server
				- a period of time goes by without any winner
					- all servers time out then start an election (above steps).
					Each candidate restarts its randomized election timeout at the start of an
					election, and it waits for that timeout to elapse before
					starting the next election; this reduces the likelihood of
					another split vote in the new election

			!!! While waiting for votes, it can receive AppendEntries RPC from a server that claims to be the leader,
			if the leader's term >= current term, then it converts back to follower, otherwise rejects the request RPC
			and continues in candidate state

			!!! if one server’s current term is smaller than the other’s, then it updates its current term to the larger value (5.1, page 5)

			!!! If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state (5.1, page 5)

		*/

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		count += 1
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.electionTimeout = rand.Int63() % 200

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.numberOfPeers = len(peers)
	rf.CurrentTerm = 1
	rf.VotedFor = -1
	rf.requestVoteCh = make(chan bool)
	rf.appendEntriesCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.majority = rf.numberOfPeers/2 + 1
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}
