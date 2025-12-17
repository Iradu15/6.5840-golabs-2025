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
	/*
		To be implemented
	*/
	return 0
}

func (rf *Raft) getLastLogTerm() int {
	/*
		To be implemented
	*/
	return 0
}

func (rf *Raft) getLogTermForIndex(index int) int {
	/*
		To be implemented
	*/
	return 0
}

func (rf *Raft) getPrevLogIndex(peer int) int {
	return rf.nextIndex[peer]
}


func moreUpToDate(requesterTerm int, requesterIndex int, currentTerm int, lastLogIndex int) bool {
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

	if requesterTerm > currentTerm {
		return true
	} else if requesterTerm == currentTerm {
		return requesterIndex >= lastLogIndex
	} else {
		return false
	}
}

func (rf *Raft) changeState(newState State) {
	rf.state = newState
}

func (rf *Raft) GetState() (int, bool) {
	/*
		Return currentTerm and whether this server believes it is the leader.
	*/
	// Your code here (3A).
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) timePassedSince(lastEventTime time.Time) time.Duration {
	return time.Since(lastEventTime)
}
