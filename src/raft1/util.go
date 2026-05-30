package raft

import (
	"fmt"
	"log"
	"slices"
	"time"

	"6.5840/raftapi"
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
	lastLog := entries[len(entries)-1]
	lastLogIndex := lastLog.Index

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
		log.Fatalf("[Error getLogTermForIndex]: invalid index %v for log: %v \n", index, entries)
		panic("[Error getLogTermForIndex] invalid index")
	}

	indexEntry := entries[index]
	term := indexEntry.Term

	return term
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
// Sort matchIndex and pick the median.
//
// This value is used by the leader to advance its own commitIndex.
func (rf *Raft) getMaxCommittedIndex() int {
	copySlice := make([]int, len(rf.matchIndex))
	copy(copySlice, rf.matchIndex)

	slices.Sort(copySlice)

	res := copySlice[rf.majority-1]

	DPrintf("[MaxCommitIndexValue] res: %v (out of %v) \n", res, rf.matchIndex)

	return res
}

// getFirstIndexOfGivenTerm returns the index of the first occurrence of a
// given term in own log.
//
// Used for log reconciliation optimization
func (rf *Raft) getFirstIndexOfGivenTerm(startPosition int, term int) int {
	// pay attention to startPosition, otherwise it might never find sought value
	if rf.log[startPosition].Term != term {
		fmt.Printf("[Error]: invalid startPosition:%v for T:%v\n", startPosition, term)
	}

	if rf.log[0].Term == term {
		return rf.log[0].Index
	}

	for index := startPosition; index >= 0; index-- {
		if rf.log[index].Term != term {
			return rf.log[index].Index + 1
		}
	}

	log.Fatalf(
		"[getFirstIndexOfGivenTerm] err for startPosition %v, term %v and log: %v \n",
		startPosition,
		term,
		rf.log,
	)

	panic("Error: getFirstIndexOfGivenTerm unreachable")
}

// getLastIndexOfGivenTerm returns the index of the last occurrence of a
// given term in own log.
//
// Used for log reconciliation optimization
func (rf *Raft) getLastIndexOfGivenTerm(startPosition int, term int) int {
	// pay attention to startPosition, otherwise it might never find sought value
	if rf.log[startPosition].Term != term {
		log.Printf("[StartPosition Error]: invalid startPosition:%v for T:%v\n", startPosition, term)
	}

	lenLog := len(rf.log)

	if rf.log[lenLog-1].Term == term {
		return rf.log[lenLog-1].Index
	}

	for index := startPosition; index < lenLog; index++ {
		if rf.log[index].Term != term {
			return rf.log[index].Index - 1
		}
	}

	log.Fatalf("[Error]: invalid startPosition:%v for T:%v\n", startPosition, term)

	panic("Error: getLastIndexOfGivenTerm unreachable")
}

// prepareEntriesForApply returns copy of entries that will be applied
func (rf *Raft) prepareEntriesForApply(startIndex int, endIndex int) []raftapi.ApplyMsg {
	var entries []raftapi.ApplyMsg

	for index := startIndex; index <= endIndex; index++ {

		entries = append(entries, raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.logAt(index)].Command,
			CommandIndex: index,
		})
	}

	return entries
}

// logAt returns the index into rf.log for the given Raft log index.
//
// Invariant: after snapshotting, rf.log[0] corresponds to the entry at
// rf.lastIncludedIndex. That means a Raft log index is converted to an
// offset in rf.log by subtracting rf.lastIncludedIndex.
//
// Example: if rf.log[0] is the entry for index 6, then the entry for Raft
// index 7 is at rf.log[1], because 7-6 == 1
func (rf *Raft) logAt(raftIndex int) int {
	return raftIndex - rf.lastIncludedIndex
}

// isEntryPresent checks if the entry at the given Raft log index is present in rf.log.
//
// Returns a boolean indicating presence and the index in rf.log if present.
func (rf *Raft) isEntryPresent(raftIndex int, term int) (bool, int) {
	if raftIndex < rf.lastIncludedIndex || raftIndex > rf.getLastLogIndex() {
		return false, -1
	}

	if rf.log[rf.logAt(raftIndex)].Term != term {
		return false, -1
	}

	return true, rf.logAt(raftIndex)
}