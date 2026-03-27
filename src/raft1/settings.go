package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	// check Figure 2
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	/*
		specifies if append of entries was necessary.
		Ex of usage:
			Append entries was not refused, but no entries were appended because already up to date
	*/
	AppendNeeded bool

	// used for log reconciliation optimization
	TermAtLeaderIndex             int
	IndexOfFirstTermAtLeaderIndex int
	OutOfBounds                   bool
	Len int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

/*
5.3:

	Each log entry stores a state machine command along with the term number
	when the entry was received by the leader.
	Each log entry also has an integer index identifying its position in the log.
*/
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int // relevant? I dont think so, can be taken from the entries
}

type PersistentRaftState struct {
	// PersistData holds data that needs to be persistent (Fig2 paper)
	Logs        []LogEntry
	CurrentTerm int
	VotedFor    int
}
