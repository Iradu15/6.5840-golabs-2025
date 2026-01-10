package raft

import (
	"fmt"
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

func (rf *Raft) getLastLogCommand() any {
	entries := rf.log
	lastLog := entries[len(entries)-1]
	lastLogCommand := lastLog.Command

	return lastLogCommand
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

// moreUpToDate checks if the candidate is more up to date than the current server.
//
// Up to date is defined by comparing the index and term of the last entries.
// The log with the later term is up to date. If terms are equal, the longer
// log is more up to date.
//
// As described in the Raft paper: "Raft determines which of two logs is more
// up-to-date by comparing the index and term of the last entries in the logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log is longer is more up-to-date."
func (rf *Raft) moreUpToDate(requesterLastLogIndex int, requesterLastLogTerm int) bool {
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	if lastLogTerm == requesterLastLogTerm {
		return requesterLastLogIndex > lastLogIndex
	}

	return requesterLastLogTerm > lastLogTerm
}

// atLeastUpToDate is a more permissive extension of moreUpToDate
func (rf *Raft) atLeastUpToDate(requesterLastLogIndex int, requesterLastLogTerm int) bool {
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

// GetState returns currentTerm and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) timePassedSince(lastEventTime time.Time) time.Duration {
	return time.Since(lastEventTime)
}

// stepDown converts to follower, updates currentTerm and resets voted for
func (rf *Raft) stepDown(replyTerm int) {
	rf.changeState(Follower)
	rf.currentTerm = replyTerm
	rf.votedFor = -1 // always when update term, votedFor gets -1
}

// getMaxCommittedIndex calculates the highest log index that is known to be
// replicated on a majority of servers.
//
// It iterates through all unique matchIndex values currently tracked for peers
// and checks if each index is present on at least rf.majority servers.
// The highest index satisfying this condition is returned.
//
// This value is used by the leader to advance its own commitIndex.
func (rf *Raft) getMaxCommittedIndex() int {
	res := 0
	matchIndexMap := map[int]int{}

	// extract all distinct values for matchIndex
	for peer := range rf.peers {
		matchIndex := rf.matchIndex[peer]
		matchIndexMap[matchIndex] = 1
	}

	for matchIndex := range matchIndexMap {

		replicatedCount := 0

		for peer := range rf.peers {

			peerMatchIndex := rf.matchIndex[peer]

			if peerMatchIndex >= matchIndex {
				replicatedCount += 1

				if replicatedCount >= rf.majority {
					res = max(res, matchIndex)
					break
				}
			}
		}
	}

	fmt.Printf("[MaxCommitIndexValue] res: %v (out of %v) \n", res, rf.matchIndex)

	return res
}
