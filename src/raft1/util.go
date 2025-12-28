package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) getLastLogIndex() int {
	entries := rf.log
	lastLogIndex := len(entries) - 1

	return lastLogIndex
}

func (rf *Raft) getLastLogTerm() int {
	entries := rf.log
	lastLog := entries[len(entries)-1]
	lastLogTerm := lastLog.Term

	return lastLogTerm
}

func (rf *Raft) getLogTermForIndex(index int) int {
	entries := rf.log

	if index < 0 || index > len(entries)-1 {
		return 0
	}

	indexEntry := entries[index]
	term := indexEntry.Term

	return term
}

func (rf *Raft) getNextLogIndex(peer int) int {
	return rf.nextIndex[peer]
}

func (rf *Raft) moreUpToDate(requesterLastLogIndex int, requesterLastLogTerm int) bool {
	/*
		Check if candidate is more up to date than current server.
		Up to date = comparing the index and term of the last entries.
		The log with later term is up to date. If equal, the longer log is more up to date

		Raft determines which of two logs is more up-to-date
		by comparing the index and term of the last entries in the
		logs. If the logs have last entries with different terms, then
		the log with the later term is more up-to-date. If the logs
		end with the same term, then whichever log is longer is
		more up-to-date
	*/

	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	if lastLogTerm == requesterLastLogTerm {
		return requesterLastLogIndex > lastLogIndex
	}

	return requesterLastLogTerm > lastLogTerm

}

func (rf *Raft) atLeastUpToDate(requesterLastLogIndex int, requesterLastLogTerm int) bool {
	/*
		Extension of moreUpToDate
	*/
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	if lastLogTerm == requesterLastLogTerm {
		return requesterLastLogIndex >= lastLogIndex
	}

	return requesterLastLogTerm >= lastLogTerm
}


func (rf *Raft) changeState(newState State) {
	rf.state = newState
}

func (rf *Raft) GetState() (int, bool) {
	/*
		Return currentTerm and whether this server believes it is the leader.
	*/
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) timePassedSince(lastEventTime time.Time) time.Duration {
	return time.Since(lastEventTime)
}

func (rf *Raft) stepDown(replyTerm int) {
	/*
		Convert to follower, update currentTerm and reset voted for
	*/
	rf.changeState(Follower)
	rf.currentTerm = replyTerm
	rf.votedFor = -1 // always when update term, votedFor gets -1
}
