package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const REELECTION_PERIOD_MAX = 350
const REELECTION_PERIOD_MIN = 300

type RequestReply struct {
	ok    bool
	reply interface{}
}

type RequestVoteArgs struct {
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}
