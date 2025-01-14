package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term  int
	Index int
}

type Status string

const (
	HeartbeatInterval   = 50 * time.Millisecond // Should be well under election timeout
	BaseElectionTimeout = 700 * time.Millisecond
	ElectionRandomRange = 150 * time.Millisecond // Results in 300-450ms timeout
)

const (
	FOLLOWER  Status = "FOLLOWER"
	CANDIDATE Status = "CANDIDATE"
	LEADER    Status = "LEADER"
	DEAD      Status = "DEAD"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//persistent
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile
	commitedIndex int
	lastApplied   int

	lastReceived time.Time
	state        Status
	//volatile leader
	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == LEADER
	term = rf.currentTerm

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC handler.

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

	// Your code here (2B).

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
	rf.mu.Lock()
	rf.state = DEAD
	rf.mu.Unlock()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// handler for append entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("Raft Term %d Raft ID %d last received %v", rf.currentTerm, rf.me, rf.lastReceived)

	if args.Term >= rf.currentTerm {
		rf.lastReceived = time.Now()
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		// DPrintf("Raft Term %d Raft ID %d last received %v", rf.currentTerm, rf.me, rf.lastReceived)
		// reply.Term = rf.currentTerm
		// reply.Success = true
	}

}

// rpc call for append entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderPulse() {
	// Keep sending heartbeats while we're leader
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.getLastLogEntry().Index,
			PreLogTerm:   rf.getLastLogEntry().Term,
			Entries:      rf.log,
			LeaderCommit: rf.commitedIndex,
		}
		rf.mu.Unlock()

		// Send AppendEntries to all peers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(peer, &args, &reply); ok {
					//DPrintf("%v", time.Now())
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						return
					}
				}
			}(i)
		}
		// Wait for the heartbeat interval before next round
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	// DPrintf("Current candidate terms %d Current voter terms %d candidateid %d voter id %d last received %v", args.Term, rf.currentTerm, args.CandidateId, rf.me, rf.lastReceived)
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = args.CandidateId
		return
	}

	//TO-DO implement logic for handling a candidates log so that it is at least as up to date with the server
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	reply.VoteGranted = true
	rf.state = FOLLOWER
	rf.lastReceived = time.Now()
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

func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastReceived = time.Now()

}

func (rf *Raft) kickoffelection() {
	rf.mu.Lock()
	// Capture all needed state while holding the lock
	currentTerm := rf.currentTerm + 1
	lastLogEntry := rf.getLastLogEntry()
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	// Update state atomically
	rf.convertToCandidate()
	rf.mu.Unlock()

	// Track votes with its own mutex to avoid holding the main lock
	var votesMu sync.Mutex
	votes := 1

	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(peerIndex int) {
			defer wg.Done()
			reply := RequestVoteReply{}

			if ok := rf.sendRequestVote(peerIndex, &args, &reply); !ok {
				return
			}

			if !reply.VoteGranted {
				return
			}

			votesMu.Lock()
			defer votesMu.Unlock()

			votes++
			if votes > len(rf.peers)/2 {
				// DPrintf("voting finished")
				rf.mu.Lock()
				if rf.currentTerm == currentTerm && rf.state == CANDIDATE {
					rf.state = LEADER
					// Initialize leader state
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {

						rf.nextIndex[i] = rf.getLastLogEntry().Index + 1
						rf.matchIndex[i] = 0
					}
				}
				rf.mu.Unlock()
				// DPrintf("leader pulse sent")
				go rf.leaderPulse()
				return
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == DEAD {
			return
		}
		if state == LEADER {
			// Start the leader pulse if we aren't already running it
			//go rf.leaderPulse()
			// Sleep for a while before checking again
			time.Sleep(HeartbeatInterval)
		} else {
			// Election timeout logic remains the same
			electionTimeout := BaseElectionTimeout +
				time.Duration(rand.Int63())%ElectionRandomRange

			startTime := time.Now()
			time.Sleep(electionTimeout)

			rf.mu.Lock()
			if rf.lastReceived.Before(startTime) && rf.state != LEADER {
				go rf.kickoffelection()
			}
			rf.mu.Unlock()
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.lastReceived = time.Now()
	rf.log = append(rf.log, LogEntry{0, rf.currentTerm})
	go rf.ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	return rf
}
