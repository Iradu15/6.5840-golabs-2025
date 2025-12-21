package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	majority        int
	electionTimeout int // amount of time it waits until starting new election

	currentTerm int
	votedFor    int
	log         []LogEntry
	state       State

	lastAppend        time.Time // keeps track of last log append or heartbeat received or request vote granted
	lastHeartBeatSent time.Time
	heartBeatInterval int

	lastApplied int // Pic2, State
	commitIndex int // Pic2, State

	nextIndex  []int
	matchIndex []int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// if already granted vote or the candidate is not more up to date, deny vote
	if rf.votedFor != -1 || !moreUpToDate(args.Term, args.LastLogIndex, rf.currentTerm, rf.getLastLogIndex()) {
		return
	}

	// reset timer
	rf.lastAppend = time.Now()
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term //Fig2: Rules for Servers: All Servers
	rf.changeState(Follower)   //Fig2: Rules for Servers: All Servers
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	/*
		Handler for receiving AppendEntryRequest (HeartBeat included) from leader.
		More documentation below at [2]
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// reject appendEntry request (Fig2): requester is behind term wise
	if rf.currentTerm > args.Term {
		return
	}

	/*
		update term + reset votedFor. Also assure node is in Follower state
		= is needed in condition check to change state to Follower if node is candidate but
		another node wins in the same term.
	*/
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(Follower)
	}

	/*
		even though node might not append now (ex: enters next if),
		it heard from leader, so timer should be reset
	*/
	rf.lastAppend = time.Now()

	if args.PrevLogIndex >= len(rf.log) {
		return
	}

	// mismatch between logs
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	reply.Success = true

	// update log entries
	startIndex := args.PrevLogIndex + 1

	remainingLen := len(rf.log) - startIndex
	lenArgsEntries := len(args.Entries)

	for index := range min(remainingLen, lenArgsEntries) {

		entry := rf.log[startIndex+index]
		argEntry := args.Entries[index]

		// discard everything and append rest of logs from args
		if entry != argEntry {
			rf.log = append(rf.log[:startIndex+index], args.Entries[index:]...)
			lenArgsEntries = 0			
			break
		}
	}

	// append possible remaining elements from args
	if lenArgsEntries > remainingLen {
		remainingElements := args.Entries[remainingLen:]
		rf.log = append(rf.log, remainingElements...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) handleHeartBeat(peer int, term int, leaderId int, leaderCommit int) {
	/*
		Dual purpose of this method:
		- Maintain Authority: Tell followers "I am still alive" (prevent election timeouts).
		- Replicate Data: Check if the follower is behind and send missing entries.
		More documentation below at [1]
	*/
	rf.mu.Lock()

	if rf.state != Leader {
		/*
			What if when issuing heartbeats you receive higher term and step down,
			you need to check for the following ones that you are still leader
		*/
		rf.mu.Unlock()
		return
	}

	nextIndex := rf.getPrevLogIndex(peer)
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.getLogTermForIndex(prevLogIndex)
	entries := rf.log[prevLogIndex:]

	rf.mu.Unlock()

	args := AppendEntryArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := AppendEntryReply{}
	ok := rf.sendHeartBeat(peer, args, &reply)

	if !ok {
		/*
			Do not retry in loop because it will block this thread, then after some time
			another goroutine will be scheduled to retry for the same peer, and so on
			=> lots of goroutines trying to reach same peer
		*/
		return
	}

	replySuccess := reply.Success
	replyTerm := reply.Term

	if replyTerm > term {
		// step down and convert back to follower
		rf.mu.Lock()

		rf.changeState(Follower)
		rf.currentTerm = replyTerm
		rf.votedFor = -1 // always when update term, votedFor gets -1

		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()

	if replySuccess {
		// Update matchIndex to what we just sent
		// Update nextIndex to matchIndex + 1
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[peer] = newMatchIndex
		rf.nextIndex[peer] = newMatchIndex + 1

	} else {
		// Decrements nextIndex for peer and retry the AppendEntries RPC next time the ticker fires until logs match
		rf.nextIndex[peer] = max(1, prevLogIndex-1)
	}

	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		ms := 50 //50 + (rand.Int63() % 10)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()

		if rf.state == Leader {
			currentTerm := rf.currentTerm

			// needed not to spam with a lot of goroutines that send heartbeat
			if rf.timePassedSince(rf.lastHeartBeatSent) > time.Duration(rf.heartBeatInterval) {
				rf.lastHeartBeatSent = time.Now()
				commitIndex := rf.commitIndex
				leaderId := rf.me

				rf.mu.Unlock()

				for peerId, _ := range rf.peers {
					go rf.handleHeartBeat(peerId, currentTerm, leaderId, commitIndex)
				}

				continue
			}

			rf.mu.Unlock()
			fmt.Printf("%v is Leader ...", rf.me)
			continue
		}

		if rf.timePassedSince(rf.lastAppend) > time.Duration(rf.electionTimeout) {
			/*
				update timer here (not in startElection) because go goroutine() schedules it,
				not starts it, what if queue is big, the goroutine might not start and the election
				timeout passes again and the timer is not updated in the goroutine
			*/
			rf.lastAppend = time.Now()
			currentTerm := rf.currentTerm

			rf.mu.Unlock()

			go rf.startElection(currentTerm)
			continue
		}
		rf.mu.Unlock()

		/*

			electionTimer should be reset just when:
			1. Append entry from a leader which has >= term.
			2. Vote is actually granted to another candidate.
			3. Starting a new election as a candidate.

			select{

				case <- valid appendEntry(heartbeat or data)

				case <- granting (NOT just receiving a request vote)!!! IMPORTANT
						because outdated candidates (with different term, outdated log) can still
						call and timer is going to be reset even though they will never win the election.
						This will increase leader election process and raft is unavailable for longer period of time.
						https://github.com/nats-io/nats-server/discussions/5023

						also Fig2: Rules for Servers: Followers

				case <-time.After(rf.ElectionTimeout):
					start New election
			}
		*/

	}
}

func (rf *Raft) startElection(term int) {
	/*

	 */
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
	rf.majority = len(peers)/2 + 1

	rf.currentTerm = 1
	rf.electionTimeout = 150 + (rand.Int() % 150) // 5.2: between [150, 300]
	rf.votedFor = -1
	rf.state = Follower
	rf.lastAppend = time.Now()
	rf.lastHeartBeatSent = time.Now()
	rf.heartBeatInterval = 100
	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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

/*
	[1]
	- want different object for each RequestVoteReply
	- want AppendEntryArgs passed by value:
		Each peer has a different nextIndex. Therefore, you cannot just create one args struct outside
		the loop. You would have to modify it inside the loop anyway.
	- 	args.latLogIndex will be different for each peer, so we need args to be placed in loop


	The leader keeps track of the highest index it knows
	to be committed, and it includes that index in future
	AppendEntries RPCs (including heartbeats) so that the
	other servers eventually find out
	Once a follower learns
	that a log entry is committed, it applies the entry to its
	local state machine (in log order).


	! Need to process the reply of heartbeats to know when to step down

	When sending an AppendEntries RPC, the leader includes the index
	and term of the entry in its log that immediately precedes
	the new entries. If the follower does not find an entry in
	its log with the same index and term, then it refuses the
	new entries

	When a leader first comes to power,
	it initializes all nextIndex values to the index just after the
	last one in its log. If a follower’s log is
	inconsistent with the leader’s, the AppendEntries consistency check will fail in the next AppendEntries RPC.
	After a rejection, the leader decrements nextIndex and retries
	the AppendEntries RPC. Eventually nextIndex will reach
	a point where the leader and follower logs match.
	When this happens, AppendEntries will succeed, which removes
	any conflicting entries in the follower’s log and appends
	entries from the leader’s log (if any). Once AppendEntries
	succeeds, the follower’s log is consistent with the leader’s,
	and it will remain that way for the rest of the term.

	[2]
	The leader decides when it is safe to apply a log entry to the state machines;
	such an entry is called committed.
	A log entry is committed once the leader that created the entry has replicated it on a majority of the servers.
	Once a follower learns that a log entry is committed, it applies the entry to its local
	state machine (in log order).

	When sending an AppendEntries RPC, the leader includes the index
	and term of the entry in its log that immediately precedes
	the new entries. If the follower does not find an entry in
	its log with the same index and term, then it refuses the
	new entries.

	In Raft, the leader handles inconsistencies by forcing
	the followers’ logs to duplicate its own:
		To bring a follower’s log into consistency with its own,
		the leader must find the latest log entry where the two
		logs agree, delete any entries in the follower’s log after
		that point, and send the follower all of the leader’s entries
		after that point.


*/
