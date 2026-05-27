package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

	majority        int
	electionTimeout int // amount of time it waits until starting new election

	currentTerm int
	votedFor    int
	log         []LogEntry
	state       State

	lastHeartBeat     time.Time // keeps track of last log append or heartbeat received or request vote granted
	lastHeartBeatSent time.Time
	heartBeatInterval int

	lastApplied int // Pic2, State
	commitIndex int // Pic2, State

	nextIndex  []int
	matchIndex []int

	votesReceived int

	/*
		per-peer flag to prevent concurrent AppendEntries
		only used for TestRPCBytes3B test that counts how many bytes are sent
		Removed due to network failure cases blocking other goroutines
	*/
	// replicating []bool

	/*
		used such that a partitioned node step down by itself.
		Caveat:
			if at least one node responds to leader in same term,
			considers it's still leader, but would need a majority
			in fact, not only one: [oldLeader, s1], [s2....sn, n >= 3]
	*/
	lastQuorumAck time.Time

	/*
		Used for exclusive applying while not holding the primary lock.
		Primary lock should be freed while applying because there might not be
		someone at the other edge of the channel to read
	*/
	applyMu sync.Mutex

	wg sync.WaitGroup

	applyCh chan raftapi.ApplyMsg

	/*
		offset from snapshotting. If log starts with index 6 and you want to access element with index 7, then
		7-6 = 1, second slot
	*/
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Fig2: RequestVote RPC: rule 1
	if rf.currentTerm > args.Term {
		DPrintf("[VoteDeclined]%v rejected %v due to lower term \n", rf.me, args.CandidateId)
		return
	}

	// Update term Fig2: Rules for Servers
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
		// persist to "disk" (disk = persister object)
		rf.persist()

		reply.Term = rf.currentTerm
	}

	// Fig2: RequestVote RPC: rule 2
	if (rf.votedFor == args.CandidateId || rf.votedFor == -1) &&
		rf.atLeastUpToDate(args.LastLogIndex, args.LastLogTerm) {

		// [Documentation Below (-1) ] valid RPC received
		rf.lastHeartBeat = time.Now()

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("%v accepted request vote from %v \n", rf.me, args.CandidateId)

		rf.persist()

		return
	}
}

// AppendEntry handles the AppendEntries RPC sent by a leader (More documentation below at [2])
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.AppendNeeded = false
	reply.OutOfBounds = false
	reply.NeedsInstallSnapshot = false

	// reject appendEntry request (Fig2): requester is behind term wise
	if rf.currentTerm > args.Term {
		DPrintf("[AppendEntryReject] S%d T%d: Rejected S%d (Leader Term %d < My Term %d)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.currentTerm)

		return
	}

	//convert to follower. // Section 5.2
	if args.Term >= rf.currentTerm {

		// new term => reset votedFor // section 5.2
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term

			// update reply
			reply.Term = rf.currentTerm
			rf.votedFor = -1

			// persist to "disk" (disk = persister object)
			rf.persist()
		}

		if rf.state != Follower {
			rf.changeState(Follower)
		}
	}

	// it heard from leader, so timer should be reset
	rf.lastHeartBeat = time.Now()

	lastIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > lastIndex {

		// used for log reconciliation optimization
		lastTerm := rf.getLastLogTerm()
		reply.TermAtLeaderIndex = lastTerm
		reply.IndexOfFirstTermAtLeaderIndex = rf.getFirstIndexOfGivenTerm(
			len(rf.log)-1,
			lastTerm,
		)

		reply.FollowerLastIndex = rf.getLastLogIndex()
		reply.OutOfBounds = true

		DPrintf("[AppendEntryReject] S%d T%d: Rejected S%d (PrevLogIndex %d out of bounds, firstEntry:%v, my len=%d)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, rf.log[0].Index, len(rf.log))

		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// prevLogIndex is too old, need to send InstallSnapshot RPC instead of AppendEntries
		DPrintf(
			"[StaleAppendEntryRPC] S%vT%v rejected AppendEntry from S%v because PrevLogIndex %v <= lastIncludedIndex %v \n",
			rf.me,
			rf.currentTerm,
			args.LeaderId,
			args.PrevLogIndex,
			rf.lastIncludedIndex,
		)

		reply.NeedsInstallSnapshot = true

		return
	}

	termAtLeaderIndex := rf.log[rf.logAt(args.PrevLogIndex)].Term

	// mismatch between logs
	if termAtLeaderIndex != args.PrevLogTerm {
		// used for log reconciliation optimization
		reply.TermAtLeaderIndex = termAtLeaderIndex
		reply.IndexOfFirstTermAtLeaderIndex = rf.getFirstIndexOfGivenTerm(
			rf.logAt(args.PrevLogIndex),
			termAtLeaderIndex,
		)

		DPrintf("[AppendEntryReject] S%d T%d: Rejected S%d at Index %d (Have Term %d, Leader Expects %d)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, termAtLeaderIndex, args.PrevLogTerm)

		return
	}

	reply.Success = true

	// update log entries and persist changes
	startIndex := args.PrevLogIndex + 1
	reply.AppendNeeded = rf.reconcileLog(startIndex, args.Entries)
	if reply.AppendNeeded {
		rf.persist()
	}

	/*
		Update commitIndex if needed.
		Cap it to lastIndex because later: rf.lastApplied = rf.commitIndex and entries that are not in
		peer's log would be skipped when applying

		lastEntrySentByLeader is useful only when leader sends batches of entries,
		not [nextIndex[peer], its last entry].
	*/
	lastIndex = rf.getLastLogIndex()
	lastEntrySentByLeader := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	rf.commitIndex = min(max(rf.commitIndex, lastEntrySentByLeader), lastIndex)

	// no entries to apply, already up to date
	if rf.lastApplied == rf.commitIndex {
		return
	}

	/*
		Snapshot is only applied on already applied Entries(FACT, check where Snapshot() is called)
		Applying entries happens in a goroutine, so lastApplies might be not updated yet
		but the entries are applied, so Snapshot was called on an entry older than lastApplied.
		Trying to access entries that are not in the log anymore, they are in the
		snapshot will get outOfBounds
	*/
	entries := rf.prepareEntriesForApply(max(rf.lastApplied, rf.lastIncludedIndex)+1, rf.commitIndex)
	// save commitIndex such that is protected while releasing lock
	commitIndex := rf.commitIndex

	currentTerm := rf.currentTerm
	peerId := rf.me

	// apply remaining entries
	
	if !rf.killed() {
		rf.wg.Add(1)
		go rf.applyEntries(entries, peerId, currentTerm, commitIndex)
	}
}

// reconcileLog reconciles the local log with a batch of new entries starting at the given index.
//
// It implements the conflict resolution logic required by Raft:
//
//  1. Conflict Detection: It compares existing entries with the new entries.
//     If a mismatch is found, it truncates the local
//     log at the point of conflict and appends the rest of the new entries.
//
//  2. Log Extension: If the new entries extend beyond the current log length
//     (and no conflicts were found), the remaining entries are appended.
//
//  3. Commit Safety: In the event of a conflict, it rolls back the commitIndex
//     to the last matching entry to ensure safety before the new uncommitted
//     entries are added.
//
// It returns true if the log was modified (entries appended or replaced).
func (rf *Raft) reconcileLog(startIndex int, argsEntries []LogEntry) bool {
	appendNeeded := false

	remainingLen := rf.getLastLogIndex() - startIndex + 1
	lenArgsEntries := len(argsEntries)

	/*
		compare existing log to the ones sent by the leader.
		Due to unreliable network it might send RPC from indexes where
		the follower already appended entries, so it might delete valid entries:
		RPC 0 at indexes: [10, 11, 12]
		RPC 1 ar  indexes: [10], so it might delete valid 11 and 12
	*/
	for index := range min(remainingLen, lenArgsEntries) {

		entry := rf.log[rf.logAt(startIndex+index)]
		argEntry := argsEntries[index]

		if entry.Term == argEntry.Term {
			continue
		}

		// discard everything and append rest of logs from args
		appendNeeded = true
		rf.log = append(rf.log[:rf.logAt(startIndex+index)], argsEntries[index:]...)

		DPrintf(
			"[LogAppend] S%vT%v: updated via reconcile from %v with %v. Now %v \n",
			rf.me,
			rf.currentTerm,
			startIndex+index,
			argsEntries[index:],
			rf.log,
		)

		//NOTE: I dont think is correct [PULA PULA]
		/*
			decrement commitIndex until last entry that matches, those that will be replaced / added
			are not yet committed
		*/
		// oldCommitIndex := rf.commitIndex
		// rf.commitIndex = startIndex + index - 1
		// fmt.Printf(
		// 	"[CommitIndexUpdate] S%vT%v updated from %v to %v \n",
		// 	rf.me,
		// 	rf.currentTerm,
		// 	oldCommitIndex,
		// 	rf.commitIndex,
		// )

		break
	}

	// append possible remaining elements from args
	if !appendNeeded && lenArgsEntries > remainingLen {
		appendNeeded = true

		remainingElements := argsEntries[remainingLen:]
		rf.log = append(rf.log, remainingElements...)

		DPrintf(
			"[LogAppend] S%vT%v: added via reconcile %v. Now: %v \n",
			rf.me,
			rf.currentTerm,
			remainingElements,
			rf.log,
		)
	}

	return appendNeeded
}

func (rf *Raft) applyEntries(
	entries []raftapi.ApplyMsg,
	peerId int,
	currentTerm int,
	commitIndex int,
) {
	defer rf.wg.Done()

	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()

	rf.mu.Lock()
	// ignore if already applied
	lastApplied := rf.lastApplied
	if lastApplied >= commitIndex {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	/*
		Edge case:
		- RPC 1 arrives, updates commitIndex to 5 + applies [1, 2, 3, 4, 5] in Goroutine 1.
		- RPC 2 arrives, updates commitIndex to 6. lastApplied is still 0 because Goroutine 1 hasn't finished.
			It prepares a new entries slice holding [1, 2, 3, 4, 5, 6]. It spawns Goroutine 2.
			Goroutine 2 gets applyMu. It checks lastApplied >= commitIndex (5 >= 6, false). It then loops over its
			pre-prepared slice and sends [1, 2, 3, 4, 5, 6] to the channel.Your state machine just received duplicates
			of entries 1 through 5.
	*/
	entriesToBeApplied := len(entries)
	for _, applyMsg := range entries {
		/*
			Do not keep sending if the server is NOT alive anymore.
			new rfsrv is created, but goroutine from old server(this one) keeps
			sending entries to the old applyCh.
			applier of the old rfsrv only stops when the channel is closed.
			Nobody closes the old applyCh — Kill() just sets rf.dead = 1 and rs.raft = nil.
			So the old applier keeps reading from the old channel as long as old Raft goroutines
			keep sending to it.
		*/
		if rf.killed() {
			return
		}
		if applyMsg.CommandIndex <= lastApplied {
			entriesToBeApplied -= 1
			continue
		}
		rf.sendApplyMsg(applyMsg, peerId, currentTerm)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(
		"[LastAppliedUpdate] S%vT%v applied %v entries from %v \n",
		peerId,
		currentTerm,
		entriesToBeApplied,
		lastApplied+1,
	)

	rf.lastApplied = commitIndex

}

// More documentation below at [0]
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
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

	// if rf.replicating[peer] {
	// 	rf.mu.Unlock()
	// 	return
	// }
	// rf.replicating[peer] = true

	nextIndex := rf.nextIndex[peer]
	if nextIndex <= rf.lastIncludedIndex {
		// nextIndex is too old(prevLogIndex would also be too old), need to send InstallSnapshot RPC instead of AppendEntries
		args := InstallSnapshotArgs{
			Term:              term,
			LeaderId:          leaderId,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		reply := InstallSnapshotReply{}

		rf.mu.Unlock()

		rf.sendInstallSnapshot(peer, &args, &reply)
		return
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.getLogTermForIndex(rf.logAt(prevLogIndex))

	// NOTE: copy, DO NOT pass a pointer, so later it can be modified even if inside lock
	// SOLUTION: deep copy: https://gemini.google.com/share/957cda6db728
	entriesToBeSent := rf.log[rf.logAt(nextIndex):]
	entries := make([]LogEntry, len(entriesToBeSent))
	copy(entries, entriesToBeSent)

	rf.mu.Unlock()

	args := AppendEntryArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := AppendEntryReply{}
	ok := rf.sendAppendEntry(peer, &args, &reply)

	rf.mu.Lock()

	// reset replicating flag
	// rf.replicating[peer] = false

	/*
		Check if peer is still leader.
		Possible causes to lose leadership:
			1. Higher term received in reply, step down and convert to follower
			2. Partitioned from majority, step down by itself after timeout
			3. etc
		Check current term to prevent processing stale replies that were sent before step down and new re-election
			1. Leader sends AppendEntries in term 5
			2. While RPC is in flight, leader steps down (receives higher term 6)
			3. Leader gets re-elected in term 7
	*/
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	if !ok {
		/*
			Do not retry in loop because it will block this thread, then after some time
			another goroutine will be scheduled to retry for the same peer, and so on
			=> lots of goroutines trying to reach same peer
		*/

		rf.mu.Unlock()
		return
	}

	replySuccess := reply.Success
	replyTerm := reply.Term
	replyAppendNeeded := reply.AppendNeeded

	if replyTerm > term {
		// fmt.Printf("[StepDown] S%d T%d: (Peer S%d replied with higher Term %d)\n", leaderId, term, peer, replyTerm)
		rf.stepDown(replyTerm)

		rf.persist()

		rf.mu.Unlock()
		return
	}

	/*
		Reset quorum timer due to response from peer.
		No point in updating if step down (after replyTerm > term)
	*/
	rf.lastQuorumAck = time.Now()

	if !replySuccess {

		if reply.NeedsInstallSnapshot {

			args := InstallSnapshotArgs{
				Term:              term,
				LeaderId:          leaderId,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.snapshot,
			}
			reply := InstallSnapshotReply{}

			rf.mu.Unlock()

			rf.sendInstallSnapshot(peer, &args, &reply)

			return
		}

		// Optimize nextIndex search and retry the AppendEntries RPC next time the ticker fires until logs match
		oldNextIndex := nextIndex
		termAtFollowerFirstIndex := rf.log[rf.logAt(reply.IndexOfFirstTermAtLeaderIndex)].Term
		if termAtFollowerFirstIndex == reply.TermAtLeaderIndex {
			rf.nextIndex[peer] = min(
				rf.getLastIndexOfGivenTerm(
					rf.logAt(reply.IndexOfFirstTermAtLeaderIndex),
					termAtFollowerFirstIndex,
				)+1,
				nextIndex-1,
				rf.nextIndex[peer], // never go UP from current value
			)
		} else {
			rf.nextIndex[peer] = min(reply.IndexOfFirstTermAtLeaderIndex, nextIndex-1, rf.nextIndex[peer])
		}

		// do not go further than the follower last index
		if reply.OutOfBounds {
			rf.nextIndex[peer] = min(rf.nextIndex[peer], reply.FollowerLastIndex)
		}

		// safety guard
		rf.nextIndex[peer] = max(rf.nextIndex[peer], 1)

		DPrintf(
			"[LogBackoff] S%d T%d: S%d rejected AppendEntries at PrevLogIndex %d. Decrement NextIndex from %d to %d\n",
			leaderId, term, peer, prevLogIndex, oldNextIndex, rf.nextIndex[peer])

		rf.mu.Unlock()
		return
	}

	lenEntries := len(entries)

	/*
		On a successful reply, the follower's log matches the leader's up to
		prevLogIndex + len(entries) — regardless of whether the follower had
		to mutate its log (replyAppendNeeded). We must still update matchIndex
		and nextIndex; otherwise the leader's view of the follower stays
		frozen and commitIndex can never advance via this peer.
	*/

	// Update matchIndex to what we just sent
	// Update nextIndex to matchIndex + 1
	newMatchIndex := prevLogIndex + lenEntries
	rf.matchIndex[peer] = max(rf.matchIndex[peer], newMatchIndex)

	oldNextIndex := nextIndex
	rf.nextIndex[peer] = max(rf.nextIndex[peer], newMatchIndex+1)

	if !replyAppendNeeded || lenEntries == 0 {
		DPrintf(
			"[NoEntries] S%v->Peer:%v matchIndex=%v nextIndex=%v->%v (replyAppendNeeded:%v)\n",
			rf.me,
			peer,
			rf.matchIndex[peer],
			oldNextIndex,
			rf.nextIndex[peer],
			replyAppendNeeded,
		)
	} else {
		DPrintf(
			"[NextIndexUpdate]: updated nextIndex for S%v from %v to %v because entries %v \n",
			peer,
			oldNextIndex,
			rf.nextIndex[peer],
			entries,
		)
	}

	maxCommitIndex := rf.getMaxCommittedIndex()

	// update commitIndex if needed
	// Section 5.4.2, only consider entries committed by count from current term
	entryIndex := rf.logAt(maxCommitIndex)
	if entryIndex >= 0 && rf.log[entryIndex].Term == rf.currentTerm {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = max(rf.commitIndex, maxCommitIndex)
		if oldCommitIndex != rf.commitIndex {
			DPrintf(
				"[CommitIndexUpdate]: S%vT%v updated commitIndex from %v to %v \n",
				leaderId,
				term,
				oldCommitIndex,
				rf.commitIndex,
			)
		}
	}

	if rf.lastApplied == rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	/*
		Snapshot is only applied on already applied Entries(FACT, check where Snapshot() is called)
		Applying entries happens in a goroutine, so lastApplies might be not updated yet
		but the entries are applied, so Snapshot was called on an entry older than lastApplied.
		Trying to access entries that are not in the log anymore, they are in the
		snapshot will get outOfBounds
	*/
	applyEntries := rf.prepareEntriesForApply(max(rf.lastApplied, rf.lastIncludedIndex)+1, rf.commitIndex)
	// save commitIndex such that is protected while releasing lock
	commitIndex := rf.commitIndex

	currentTerm := rf.currentTerm
	peerId := rf.me

	// apply remaining entries
	if !rf.killed() {
		rf.wg.Add(1)
		go rf.applyEntries(applyEntries, peerId, currentTerm, commitIndex)
	}

	rf.mu.Unlock()
}

func (rf *Raft) becomeLeader() {
	DPrintf("[Leader] S%d T%d: Won election and became Leader \n", rf.me, rf.currentTerm)

	// reset quorum timer
	rf.lastQuorumAck = time.Now()

	rf.changeState(Leader)

	// reinitialize arrays Fig2: State
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	lastLogIndex := rf.getLastLogIndex()

	for i := range rf.peers {
		// figure 2: volatile state on leaders
		rf.nextIndex[i] = max(1, lastLogIndex+1)
		rf.matchIndex[i] = 0
		// reset replicating
		// rf.replicating[i] = false
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
		// fmt.Printf("[RequestVoteError] %v did not respond to request vote from %v \n", peer, candidateId)

		return
	}

	voteGranted := reply.VoteGranted
	replyTerm := reply.Term

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Discard stale replies from a previous term's election
	if rf.state != Candidate || rf.currentTerm != term {
		return
	}

	if replyTerm > term {
		// step down and convert back to follower
		rf.stepDown(replyTerm)

		// fmt.Printf("[StepDown] S%d T%d: (Peer S%d replied with higher Term %d)\n", candidateId, term, peer, replyTerm)

		rf.persist()

		return
	}

	if !voteGranted {
		// fmt.Printf(
		// 	"[VoteDenied] S%d T%d: Peer S%d denied (ReplyTerm: %d)\n",
		// 	candidateId,
		// 	rf.currentTerm,
		// 	peer,
		// 	reply.Term,
		// )

		return
	}

	// fmt.Printf(
	// 	"[VoteReceived] S%d T%d: Peer S%d granted vote (Total: %d)\n",
	// 	candidateId,
	// 	rf.currentTerm,
	// 	peer,
	// 	rf.votesReceived,
	// )

	rf.votesReceived += 1

	if rf.votesReceived >= rf.majority && rf.state != Leader {
		rf.becomeLeader()
	}
}

func (rf *Raft) startElection(term int, lastLogIndex int, lastLogTerm int, candidateId int) {

	// log.Printf("[ElectionStarted] S%v T%v \n", rf.me, rf.currentTerm)

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

			/*
				Optimization such that partitioned leader steps down by itself.
				It is safe to step down to same term because meanwhile the other servers
				started an election and incremented the term
			*/
			if rf.timePassedSince(rf.lastQuorumAck) > time.Duration(2*rf.electionTimeout*int(time.Millisecond)) {
				rf.stepDown(rf.currentTerm)
				DPrintf("[StepDown] S%d T%d: (Partitioned leader stepping down)\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				continue
			}

			// needed not to spam with a lot of goroutines that send heartbeat
			if rf.timePassedSince(rf.lastHeartBeatSent) > time.Duration(rf.heartBeatInterval*int(time.Millisecond)) {

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
		if rf.timePassedSince(rf.lastHeartBeat) > time.Duration(rf.electionTimeout*int(time.Millisecond)) {
			/*
				update timer here (not in startElection) because go goroutine() schedules it,
				not starts it, what if queue is big, the goroutine might not start and the election
				timeout passes again and the timer is not updated in the goroutine
			*/
			rf.lastHeartBeat = time.Now()
			rf.currentTerm += 1
			currentTerm := rf.currentTerm

			// critical: between incrementing term and starting election, it might receive RPC with
			// higher term and step down, so it needs to check for that in the goroutine
			rf.changeState(Candidate)
			rf.votedFor = rf.me

			rf.persist()

			lastLogIndex := rf.getLastLogIndex()
			lastLogTerm := rf.getLastLogTerm()
			candidateId := rf.me

			rf.votesReceived = 1 // vote for self

			rf.mu.Unlock()

			go rf.startElection(currentTerm, lastLogIndex, lastLogTerm, candidateId)

			continue
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendApplyMsg(applyMsg raftapi.ApplyMsg, peer int, term int) {
	rf.applyCh <- applyMsg

	DPrintf("[ApplyCh] S%vT%v Sent %v via ApplyMsg \n", peer, term, applyMsg)
}

// handleReplicateCommand manages the consensus flow for a new client operation.
//
// It follows the lifecycle of a log entry:
//  1. Append command to own log (done in Start()).
//  2. Issue AppendEntries RPCs in parallel to replicate the entry.
//  3. (Might not happen) When the entry has been safely replicated (as described
//     below), the leader applies the entry to its state machine and returns the
//     result of that execution to the client. (done in handleAppendEntry())
func (rf *Raft) handleReplicateCommand(term int, commitIndex int, leaderId int) {
	for peerId := range rf.peers {

		if peerId == leaderId {
			continue
		}

		go rf.handleAppendEntry(peerId, term, leaderId, commitIndex)
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
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, isLeader
	}

	isLeader = true

	// add entry to log. It needs to be quick, can't wait for the goroutine to execute
	lastIndex := rf.getLastLogIndex()

	entry := LogEntry{command, rf.currentTerm, lastIndex + 1}
	rf.log = append(rf.log, entry)
	lastIndex += 1

	DPrintf("[LogAppend] S%vT%v: added via Start %v. Now: %v \n", rf.me, rf.currentTerm, entry, rf.log)
	// DPrintf("[LogAppend] S%vT%v: added %v.\n", rf.me, rf.currentTerm, entry)

	// persist to "disk" (disk = persister object)
	rf.persist()

	// Update nextIndex and matchIndex for itself, so when it sends AppendEntries to followers,
	// it will know that it is already replicated on itself
	rf.nextIndex[rf.me] = lastIndex + 1
	rf.matchIndex[rf.me] = lastIndex

	term = rf.currentTerm

	go rf.handleReplicateCommand(term, rf.commitIndex, rf.me)

	return lastIndex, term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// It is critical that is sync: if async-persist and crash, you could double vote
// in the same term twice
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)

	persistentRaftState := PersistentRaftState{
		Logs:              rf.log,
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}

	err := enc.Encode(&persistentRaftState)
	if err != nil {
		log.Printf("[EncodeError] S%vT%v: err for %v: %v\n", rf.me, rf.currentTerm, persistentRaftState, err)
	}

	raftStateBytes := w.Bytes()

	rf.persister.Save(raftStateBytes, rf.snapshot)

	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshotData []byte) {
	// on fresh start, no data to read
	if data == nil || len(data) < 1 {

		// dummy entry (in case log is not persisted, otherwise it is already included)
		rf.log = append(rf.log, LogEntry{Term: rf.lastIncludedTerm, Index: rf.lastIncludedIndex})
		// rf.replicating = make([]bool, len(peers))

		DPrintf("S%vT%v has no data from persister\n", rf.me, rf.currentTerm)
		return
	}

	// Your code here (3C).
	buff := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buff)

	var raftStateStruct PersistentRaftState

	err := dec.Decode(&raftStateStruct)
	if err != nil {
		log.Printf("[DecodeError] S%vT%v err decoding %v into persister: %v \n", rf.me, rf.currentTerm, data, err)
		return
	}

	// log.Printf("S%v:Read data from persister %v \n", rf.me, raftStateStruct)
	// restore data from the received bytes
	rf.log = raftStateStruct.Logs
	rf.currentTerm = raftStateStruct.CurrentTerm
	rf.votedFor = raftStateStruct.VotedFor

	// snapshotting
	rf.lastIncludedIndex = raftStateStruct.LastIncludedIndex
	rf.lastIncludedTerm = raftStateStruct.LastIncludedTerm
	rf.snapshot = snapshotData
	// commitIndex >= lastIncludedIndex because snapshot is created on already applied entries,
	// so it is safe to update commitIndex to lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
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

	// will be updated in readPersist if there is persisted state, otherwise = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (3A, 3B, 3C).
	rf.majority = len(peers)/2 + 1

	rf.currentTerm = 1
	rf.votedFor = -1
	rf.state = Follower

	rf.lastHeartBeat = time.Now()
	rf.lastHeartBeatSent = time.Now()
	rf.heartBeatInterval = 100

	// offset from snapshotting
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.applyCh = applyCh
	rf.applyMu = sync.Mutex{}

	rf.wg = sync.WaitGroup{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, resp *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	resp.Term = currentTerm

	if args.Term > currentTerm {
		// step down and convert back to follower
		rf.stepDown(args.Term)
		rf.persist()
	} else if args.Term < currentTerm {
		// outdated snapshot, ignore
		DPrintf("[InstallSnapshot Reject] S%vT%v args.Term:%v currentTerm:%v\n", rf.me, currentTerm, args.Term, currentTerm)
		return
	}

	if rf.commitIndex > args.LastIncludedIndex {
		// reject snapshot, already have committed entries that are not included in the snapshot
		// since restart
		DPrintf(
			"[InstallSnapshot Reject] S%vT%v commitIndex:%v > args.LastIncludedIndex:%v\n",
			rf.me,
			currentTerm,
			rf.commitIndex,
			args.LastIncludedIndex,
		)
		return
	}

	if rf.snapshot != nil && args.LastIncludedIndex <= rf.lastIncludedIndex {
		// reject entry, current snapshot is more/as up to date than the one sent by the leader
		DPrintf(
			"[InstallSnapshot Reject] S%vT%v args.LastIncludedIndex:%v <= rf.lastIncludedIndex:%v\n",
			rf.me,
			currentTerm,
			args.LastIncludedIndex,
			rf.lastIncludedIndex,
		)
		return
	}

	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	ok, index := rf.isEntryPresent(args.LastIncludedIndex, args.LastIncludedTerm)
	if !ok {
		// otherwise, discard the entire log
		rf.log = []LogEntry{{Command: nil, Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	} else {
		rf.log = rf.log[index:]
	}

	DPrintf(
		"[InstallSnapshot] S%vT%v discarded until %v NOW until: %v\n",
		rf.me,
		currentTerm,
		args.LastIncludedIndex,
		rf.getLastLogIndex(),
	)

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	rf.persist()

	applyEntries := make([]raftapi.ApplyMsg, 1)
	applyEntries[0] = raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  args.LastIncludedIndex,
	}

	peerId := rf.me

	if !rf.killed() {
		rf.wg.Add(1)
		go rf.applyEntries(applyEntries, peerId, currentTerm, args.LastIncludedIndex)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/*
		Remove logs up to and including index.
		Keep first index the dummy entry(needs to have the correct term)
		when looking at prevLogTerm
	*/
	rf.log = rf.log[rf.logAt(index):]

	// reset rf.lastIncluded
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[0].Term

	// remove other older snapshots
	rf.snapshot = snapshot

	rf.persist()

	DPrintf("[Snapshot] S%vT%v: index:%v log:%v \n", rf.me, rf.currentTerm, index, rf.log)
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
	
	// used for building a happens-before relationship between WaitGroup Wait() and Add() 
	rf.mu.Lock()
	rf.mu.Unlock()

	rf.wg.Wait()

	rf.applyCh <- raftapi.ApplyMsg{CommandValid: false}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
	[-1]
		section 5.2 "valid RPC from leader/candidate"

		If you reset your timer every time a "lagging" candidate
		asked for a vote, a broken or slow node could
		indefinitely prevent a leader from being elected by
		repeatedly sending "valid" but "unvotable" requests.

		Also a partitioned candidate will skyrocket his term and
		when coming back it will send requestVote that will be rejected

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
