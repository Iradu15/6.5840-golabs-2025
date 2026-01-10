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

	votesReceived int

	applyCh chan raftapi.ApplyMsg
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Fig2: RequestVote RPC: rule 1
	if rf.currentTerm > args.Term {
		fmt.Printf("%v rejected request vote from %v due to lower term \n", rf.me, args.CandidateId)
		return
	}

	// Update term Fig2: Rules for Servers
	if args.Term > rf.currentTerm {
		rf.lastAppend = time.Now()
		rf.stepDown(args.Term)
	}

	// Fig2: RequestVote RPC: rule 2
	if (rf.votedFor == args.CandidateId || rf.votedFor == -1) &&
		rf.atLeastUpToDate(args.LastLogIndex, args.LastLogTerm) {

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		fmt.Printf("%v accepted request vote from %v \n", rf.me, args.CandidateId)
		return
	}

	// if already granted vote (terms are equal, if higher would have granted, if lower would have rejected), deny vote
	if rf.votedFor != -1 {
		fmt.Printf("[VoteReject] S%d T%d: Rejected S%d (Already voted for S%d)\n",
			rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)

		return
	}

	if !rf.moreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("[VoteReject] S%d T%d: Rejected S%d (Cand=[%d/T%d] vs Me=[%d/T%d])\n",
			rf.me, rf.currentTerm, args.CandidateId,
			args.LastLogIndex, args.LastLogTerm,
			rf.getLastLogIndex(), rf.getLastLogTerm())

		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	rf.changeState(Follower)
	rf.lastAppend = time.Now()

	rf.currentTerm = args.Term
}

// AppendEntry handles the AppendEntries RPC sent by a leader (More documentation below at [2])
func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.AppendNeeded = false

	// reject appendEntry request (Fig2): requester is behind term wise
	if rf.currentTerm > args.Term {
		fmt.Printf("[AppendEntryReject] S%d T%d: Rejected S%d (Leader Term %d < My Term %d)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.currentTerm)

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

		if rf.state != Follower {

			rf.changeState(Follower)

			fmt.Printf(
				"[ConvertToFollower (heartbeat)] S%vT%v steps down from %v due to S%vT%v\n",
				rf.me,
				rf.currentTerm,
				rf.state,
				args.LeaderId,
				args.Term,
			)
		}
	}

	/*
		even though node might not append now (ex: enters next if),
		it heard from leader, so timer should be reset
	*/
	rf.lastAppend = time.Now()

	if args.PrevLogIndex >= len(rf.log) {
		fmt.Printf("[AppendEntryReject] S%d T%d: Rejected S%d (PrevLogIndex %d out of bounds, my len=%d)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, len(rf.log))

		return
	}

	// mismatch between logs
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		fmt.Printf("[AppendEntryReject] S%d T%d: Rejected S%d at Index %d (Have Term %d, Leader Expects %d)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

		return
	}

	reply.Success = true

	// update log entries
	startIndex := args.PrevLogIndex + 1
	reply.AppendNeeded = rf.reconcileLog(startIndex, args.Entries)

	// no entries to commit, already up to date
	if rf.commitIndex >= args.LeaderCommit {
		return
	}

	// commit remaining entries
	for index := rf.commitIndex + 1; index <= args.LeaderCommit; index++ {

		applyMsg := raftapi.ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[index].Command,
			CommandIndex:  index,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  -1,
			SnapshotIndex: -1,
		}

		rf.sendApplyMsg(applyMsg, rf.me, rf.currentTerm)
	}

	oldCommitIndex := rf.commitIndex
	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	fmt.Printf(
		"[CommitIndexUpdate] S%vT%v updated from %v to %v \n",
		rf.me,
		rf.currentTerm,
		oldCommitIndex,
		rf.commitIndex,
	)
}

// reconcileLog reconciles the local log with a batch of new entries starting at the given index.
//
// It implements the conflict resolution logic required by Raft:
//  1. Conflict Detection: It compares existing entries with the new entries.
//     If a mismatch is found, it truncates the local
//     log at the point of conflict and appends the rest of the new entries.
//  2. Log Extension: If the new entries extend beyond the current log length
//     (and no conflicts were found), the remaining entries are appended.
//  3. Commit Safety: In the event of a conflict, it rolls back the commitIndex
//     to the last matching entry to ensure safety before the new uncommitted
//     entries are added.
//
// It returns true if the log was modified (entries appended or replaced).
func (rf *Raft) reconcileLog(startIndex int, argsEntries []LogEntry) bool {
	appendNeeded := false

	remainingLen := len(rf.log) - startIndex
	lenArgsEntries := len(argsEntries)

	for index := range min(remainingLen, lenArgsEntries) {

		entry := rf.log[startIndex+index]
		argEntry := argsEntries[index]

		if entry == argEntry {
			continue
		}

		// discard everything and append rest of logs from args
		appendNeeded = true

		rf.log = append(rf.log[:startIndex+index], argsEntries[index:]...)

		lenArgsEntries = 0

		fmt.Printf(
			"[LogAppend] S%vT%v: updated from %v with %v. Now %v \n",
			rf.me,
			rf.currentTerm,
			startIndex+index,
			argsEntries[index:],
			rf.log,
		)

		/*
			decrement commitIndex until last entry that matches, those that will be replaced / added
			are not yet committed
		*/
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = startIndex + index - 1
		fmt.Printf(
			"[CommitIndexUpdate] S%vT%v updated from %v to %v \n",
			rf.me,
			rf.currentTerm,
			oldCommitIndex,
			rf.commitIndex,
		)

		break
	}

	// append possible remaining elements from args
	if lenArgsEntries > remainingLen {

		appendNeeded = true

		remainingElements := argsEntries[remainingLen:]
		rf.log = append(rf.log, remainingElements...)

		fmt.Printf("[LogAppend] S%vT%v: added %v. Now: %v \n", rf.me, rf.currentTerm, remainingElements, rf.log)
	}

	return appendNeeded
}

// More documentation below at [0]
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// handleAppendEntry manages the transmission of an AppendEntry RPC to a single peer
//
// Dual purpose of this method:
//   - Maintain Authority: Tell followers "I am still alive" (prevent election timeouts).
//   - Replicate Data: Check if the follower is behind and send missing entries.
//
// More documentation below at [1]
func (rf *Raft) handleAppendEntry(peer int, term int, leaderId int, leaderCommit int) {
	rf.mu.Lock()

	if rf.state != Leader {
		/*
			What if when issuing heartbeats you receive higher term and step down,
			you need to check for the following ones that you are still leader
		*/
		rf.mu.Unlock()
		return
	}

	nextIndex := rf.getNextLogIndex(peer)
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.getLogTermForIndex(prevLogIndex)

	entries := rf.log[nextIndex:]

	rf.mu.Unlock()

	args := AppendEntryArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := AppendEntryReply{}
	ok := rf.sendAppendEntry(peer, args, &reply)

	if !ok {
		/*
			Do not retry in loop because it will block this thread, then after some time
			another goroutine will be scheduled to retry for the same peer, and so on
			=> lots of goroutines trying to reach same peer
		*/
		fmt.Printf("[HeartBeatError] %v did not respond to heartbeat from %v \n", peer, leaderId)

		return
	}

	replySuccess := reply.Success
	replyTerm := reply.Term
	replyAppendNeeded := reply.AppendNeeded

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if replyTerm > term {
		// step down and convert back to follower
		fmt.Printf("[StepDown] S%d T%d: (Peer S%d replied with higher Term %d)\n", leaderId, term, peer, replyTerm)

		rf.stepDown(replyTerm)

		return
	}

	if replySuccess {

		lenEntries := len(entries)
		if !replyAppendNeeded || lenEntries == 0 {
			return
		}

		// Update matchIndex to what we just sent
		// Update nextIndex to matchIndex + 1
		newMatchIndex := prevLogIndex + len(entries)
		rf.matchIndex[peer] = newMatchIndex

		oldNextIndex := rf.nextIndex[peer]
		rf.nextIndex[peer] = newMatchIndex + 1

		fmt.Printf(
			"[NextIndexUpdate]: S%v updated nextIndex from %v to %v because entries %v \n",
			peer,
			oldNextIndex,
			rf.nextIndex[peer],
			entries,
		)

		maxCommitIndex := rf.getMaxCommittedIndex()
		// update commitIndex value and apply uncommitted values
		if maxCommitIndex > rf.commitIndex {

			for index := rf.commitIndex; index <= maxCommitIndex; index++ {

				command := rf.log[index].Command
				commandIndex := index
				applyMsg := raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       command,
					CommandIndex:  commandIndex,
					SnapshotValid: false,
					Snapshot:      []byte{},
					SnapshotTerm:  -1,
					SnapshotIndex: -1,
				}

				rf.sendApplyMsg(applyMsg, rf.me, rf.currentTerm)
			}

			oldCommitIndex := rf.commitIndex
			rf.commitIndex = maxCommitIndex

			fmt.Printf(
				"[CommitIndexUpdate] S%vT%v updated from %v to %v \n",
				rf.me,
				rf.currentTerm,
				oldCommitIndex,
				rf.commitIndex,
			)
		}

		fmt.Printf(
			"[ReplicateSuccess] S%vT%v replicated %v on S%v \n",
			leaderId,
			term,
			rf.log[len(rf.log)-lenEntries:],
			peer,
		)

	} else {
		// Decrements nextIndex for peer and retry the AppendEntries RPC next time the ticker fires until logs match

		oldNextIndex := rf.nextIndex[peer]
		rf.nextIndex[peer] = max(1, oldNextIndex-1)

		fmt.Printf(
			"[LogBackoff] S%d T%d: S%d rejected AppendEntries at PrevLogIndex %d. Decrement NextIndex from %d to %d\n",
			leaderId, term, peer, prevLogIndex, oldNextIndex, rf.nextIndex[peer])
	}
}

func (rf *Raft) becomeLeader() {
	fmt.Printf("[Leader] S%d T%d: Won election and became Leader \n", rf.me, rf.currentTerm)

	rf.changeState(Leader)

	// reinitialize arrays Fig2: State
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	lastLogIndex := rf.getLastLogIndex()

	for i := range rf.peers {
		rf.nextIndex[i] = max(1, lastLogIndex)
		rf.matchIndex[i] = 0
	}
}

// handleRequestVote manages the transmission of a RequestVote RPC to a single peer during an election
func (rf *Raft) handleRequestVote(peer int, term int, lastLogIndex int, lastLogTerm int, candidateId int) {
	rf.mu.Lock()

	if rf.state != Candidate {
		/*
			What if when issuing request votes you receive higher term and step down,
			you need to check for the following ones that you are still candidate
		*/
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
	reply := RequestVoteReply{}

	ok := rf.sendRequestVote(peer, &args, &reply)

	if !ok {
		// peer did not respond
		fmt.Printf("[RequestVoteError] %v did not respond to request vote from %v \n", peer, candidateId)

		return
	}

	voteGranted := reply.VoteGranted
	replyTerm := reply.Term

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if replyTerm > term {
		// step down and convert back to follower
		rf.stepDown(replyTerm)

		fmt.Printf("[StepDown] S%d T%d: (Peer S%d replied with higher Term %d)\n", candidateId, term, peer, replyTerm)

		return
	}

	if !voteGranted {
		fmt.Printf(
			"[VoteDenied] S%d T%d: Peer S%d denied (ReplyTerm: %d)\n",
			candidateId,
			rf.currentTerm,
			peer,
			reply.Term,
		)

		return
	}

	fmt.Printf(
		"[VoteReceived] S%d T%d: Peer S%d granted vote (Total: %d)\n",
		candidateId,
		rf.currentTerm,
		peer,
		rf.votesReceived,
	)

	rf.votesReceived += 1

	if rf.votesReceived >= rf.majority && rf.state != Leader {
		rf.becomeLeader()
	}
}

func (rf *Raft) startElection(term int) {
	rf.mu.Lock()

	fmt.Printf("[ElectionStarted] S%v T%v \n", rf.me, rf.currentTerm)

	rf.changeState(Candidate)

	rf.votedFor = rf.me
	rf.votesReceived = 1

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	candidateId := rf.me

	rf.mu.Unlock()

	fmt.Printf("[VoteSend] S%d T%d: started issuing request Votes\n", candidateId, rf.currentTerm)

	for peerId := range rf.peers {

		if peerId == candidateId {
			continue
		}

		go rf.handleRequestVote(peerId, term, lastLogIndex, lastLogTerm, candidateId)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		ms := 50 //50 + (rand.Int63() % 10)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()

		rf.electionTimeout = 250 + (rand.Int() % 250) // 5.2: example

		if rf.state == Leader {
			currentTerm := rf.currentTerm

			// needed not to spam with a lot of goroutines that send heartbeat
			if rf.timePassedSince(rf.lastHeartBeatSent) > time.Duration(rf.heartBeatInterval) {

				rf.lastHeartBeatSent = time.Now()
				commitIndex := rf.commitIndex
				leaderId := rf.me

				rf.mu.Unlock()

				for peerId := range rf.peers {

					if peerId == leaderId {
						continue
					}

					go rf.handleAppendEntry(peerId, currentTerm, leaderId, commitIndex)
				}

				continue
			}

			rf.mu.Unlock()
			continue
		}

		// More documentation below at [3]
		if rf.timePassedSince(rf.lastAppend) > time.Duration(rf.electionTimeout*int(time.Millisecond)) {
			/*
				update timer here (not in startElection) because go goroutine() schedules it,
				not starts it, what if queue is big, the goroutine might not start and the election
				timeout passes again and the timer is not updated in the goroutine
			*/
			rf.lastAppend = time.Now()
			rf.currentTerm += 1
			currentTerm := rf.currentTerm

			rf.mu.Unlock()

			go rf.startElection(currentTerm)
			continue
		}

		rf.mu.Unlock()
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
	rf.majority = len(peers)/2 + 1

	rf.currentTerm = 1
	rf.votedFor = -1
	rf.state = Follower

	rf.lastAppend = time.Now()
	rf.lastHeartBeatSent = time.Now()
	rf.heartBeatInterval = 100

	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) sendApplyMsg(applyMsg raftapi.ApplyMsg, peer int, term int) {
	rf.applyCh <- applyMsg

	fmt.Printf("[ApplyCh] S%vT%v Sent %v via ApplyMsg \n", peer, term, applyMsg)
}

// replicateCommand manages the consensus flow for a new client operation.
//
// It follows the lifecycle of a log entry:
//  1. Append command to own log (done in Start()).
//  2. Issue AppendEntries RPCs in parallel to replicate the entry.
//  3. (Might not happen) When the entry has been safely replicated (as described
//     below), the leader applies the entry to its state machine and returns the
//     result of that execution to the client.
func (rf *Raft) replicateCommand(command any) {
	rf.mu.Lock()

	term := rf.currentTerm
	commitIndex := rf.commitIndex
	leaderId := rf.me

	lenEntries := len(rf.log)

	rf.nextIndex[rf.me] = lenEntries
	rf.matchIndex[rf.me] = lenEntries - 1

	rf.mu.Unlock()

	for peerId := range rf.peers {

		if peerId == leaderId {
			continue
		}

		go rf.handleAppendEntry(peerId, term, leaderId, commitIndex)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	maxCommitIndex := rf.getMaxCommittedIndex()

	if rf.commitIndex >= maxCommitIndex {
		return
	}

	// update commitIndex value and apply uncommitted values

	for index := rf.commitIndex; index <= maxCommitIndex; index++ {

		command := rf.log[index].Command
		commandIndex := index
		applyMsg := raftapi.ApplyMsg{
			CommandValid:  true,
			Command:       command,
			CommandIndex:  commandIndex,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  -1,
			SnapshotIndex: -1,
		}

		rf.sendApplyMsg(applyMsg, rf.me, rf.currentTerm)
	}

	rf.commitIndex = maxCommitIndex
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
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	// add entry to log. It needs to be quick, can't wait for the goroutine to execute
	lenEntries := len(rf.log)

	entry := LogEntry{command, rf.currentTerm, lenEntries}
	rf.log = append(rf.log, entry)
	lenEntries += 1

	fmt.Printf("[LogAppend] S%vT%v: added %v. Now: %v \n", rf.me, rf.currentTerm, entry, rf.log)

	go rf.replicateCommand(command)

	term = rf.currentTerm
	index = lenEntries - 1

	return index, term, isLeader
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
	[0]
		example code to send a RequestVote RPC to a server.
		server is the index of the target server in rf.peers[].
		expects RPC arguments in args.
		fills in *reply with RPC reply, so caller should
		pass &reply.
		the types of the args and reply passed to Call() must be
		the same as the types of the arguments declared in the
		handler function (including whether they are pointers).

		The labrpc package simulates a lossy network, in which servers
		may be unreachable, and in which requests and replies may be lost.
		Call() sends a request and waits for a reply. If a reply arrives
		within a timeout interval, Call() returns true; otherwise
		Call() returns false. Thus Call() may not return for a while.
		A false return can be caused by a dead server, a live server that
		can't be reached, a lost request, or a lost reply.

		Call() is guaranteed to return (perhaps after a delay) *except* if the
		handler function on the server side does not return.  Thus there
		is no need to implement your own timeouts around Call().

		look at the comments in ../labrpc/labrpc.go for more details.

		if you're having trouble getting RPC to work, check that you've
		capitalized all field names in structs passed over RPC, and
		that the caller passes the address of the reply struct with &, not
		the struct itself.


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

	[3]
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
