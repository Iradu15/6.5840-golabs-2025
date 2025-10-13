package raft

import (
	"log"
	"math/rand"
	"time"

	kvtest "6.5840/kvtest1"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) getLastLogIndex() int {
	return 0
}

func (rf *Raft) getLastLogTerm() int {
	return 0
}

func (rf *Raft) moreUpToDate(term int, index int) bool {
	/*
		Check if candidate is more up to date than current server.
		Up to date = comparing the index and term of the last entries.
		The log with later term is up to date. If equal, the longer log is more up to date
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.CurrentTerm
	if term > currentTerm {
		return true
	} else if term == currentTerm {
		return index >= rf.getLastLogIndex()
	} else {
		return false
	}
}

func (rf *Raft) changeState(newState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.State = newState
}

func (rf *Raft) startElection() {

	log.Printf("Election started by %v, new term %v", rf.me, rf.CurrentTerm+1)

	electionTimer := time.NewTimer(kvtest.ElectionTimeout)
	defer electionTimer.Stop()

	rf.mu.Lock()
	rf.VotedFor = -1
	rf.CurrentTerm += 1
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	rf.mu.Unlock()
	rf.changeState(Candidate) // already uses lock

	// from self
	votes := 1
	responses := 1
	ch := make(chan RequestReply)
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastLogIndex, lastLogTerm}

	rf.broadcastRequestVotes(ch, args)

	for {

		select {
		case <-electionTimer.C:
			log.Printf("Election failed. Candidate %v timed out for term %v. Abandoning election", rf.me, rf.CurrentTerm)
			rf.changeState(Follower)
			return

		case <-rf.stop:
			log.Printf("Election canceled by %v, returns back to follower", rf.me)
			rf.changeState(Follower)
			return

		case rsp := <-ch:
			ok := rsp.ok
			reply, _ := rsp.reply.(RequestVoteReply)

			// should it retry?
			if !ok {
				log.Printf("Responses 222222222222 for %v in term %v: %v", rf.me, rf.CurrentTerm, responses)
				continue
			}

			responses += 1
			log.Printf("Responses 33333333333333 for %v in term %v: %v", rf.me, rf.CurrentTerm, responses)
			log.Printf("Votes for %v in term %v: %v", rf.me, rf.CurrentTerm, votes)

			voteGranted := reply.VoteGranted
			if !voteGranted {
				if reply.CurrentTerm > rf.CurrentTerm {
					log.Printf("%v stops election in term %v, received reply with bigger term", rf.me, rf.CurrentTerm)
					rf.changeState(Follower)
					return
				}

				continue
			}

			votes += 1
			if votes >= rf.majority {
				log.Printf("%v got majority of votes, is becoming leader in term %v", rf.me, rf.CurrentTerm)
				rf.establishAsLeader()
				return
			}
		}

	}
}

func (rf *Raft) broadcastRequestVotes(ch chan RequestReply, args RequestVoteArgs) {

	for peerId := range rf.peers {
		if peerId == rf.me {
			select {
			case rf.requestVoteCh <- true:
			default:
			}
			continue
		}

		reply := RequestVoteReply{}
		go rf.sendRequestVote(peerId, &args, &reply, ch)
	}

}

func (rf *Raft) establishAsLeader() {
	rf.changeState(Leader)
	log.Printf("%v became leader in term %v", rf.me, rf.CurrentTerm)

	rf.lead()
}

func (rf *Raft) broadcastHeartbeats() {
	for peerId := range rf.peers {
		if peerId == rf.me {
			select {
			case rf.heartBeatCh <- true:
			default:
			}
			continue
		}
		log.Printf("Leader %v is sending hearthbeat to %v in term %v", rf.me, peerId, rf.CurrentTerm)
		go rf.sendHeartbeat(peerId, &AppendEntriesArgs{}, &AppendEntriesReply{})
	}
}

func (rf *Raft) getElectionTimeOut() time.Duration {
	duration := rand.Intn(int(REELECTION_PERIOD_MAX)-int(REELECTION_PERIOD_MIN)) +
		int(REELECTION_PERIOD_MIN)
	return time.Duration(duration)
}
