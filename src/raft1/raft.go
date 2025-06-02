package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// PeerState represents the role of a Raft peer.
type PeerState int

const (
	Follower PeerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *tester.Persister   // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	currentTerm        int                 // the latest term seen by this peer
	votedFor           int                 // the index of the peer that this peer voted for
	latestRpcTimestamp time.Time           // the latest timestamp that this peer was contacted
	state              PeerState           // state of this peer: Follower, Candidate, or Leader

	// Your data here (3B, 3C).

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
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

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	// Your data here (3B).
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Never vote for a candidate who is less up to date than you.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	// Update the terms.
	reply.Term = args.Term
	rf.currentTerm = args.Term

	// Vote if you've voted before, or if this is your first time voting.
	isCandidateVoter := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	// TODO: Your code here (3B).
	isCandidateLogLatest := true
	if isCandidateVoter && isCandidateLogLatest {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	// Don't request a vote from yourself, this should already be handled on election start.
	if rf.me == server {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		// If you're the current leader, then don't run an election timer.
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Unlock()

			return
		}

		// If the election timeout elapses, start an election.
		electionTimeoutMs := 300 + (rand.Int63() % 200)
		if elapsed := time.Since(rf.latestRpcTimestamp); elapsed > time.Duration(electionTimeoutMs)*time.Millisecond {
			rf.transitionToCandidate()
			rf.mu.Unlock()

			return
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) transitionToCandidate() {
	DPrintf("Transitioning server %d to Candidate", rf.me)

	// Update the state of the server to reflect that it's a candidate.
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.latestRpcTimestamp = time.Now()

	// Keep track of how many votes we recieve.
	votes := 1

	// Keep track of the current term, because that might change under concurrency.
	currentTerm := rf.currentTerm

	// For each server, request its vote.
	for server := range rf.peers {
		go func(server int) {
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: rf.me,
			}
			var reply RequestVoteReply

			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// If the server is no longer a candidate, then stop the election.
			if rf.state != Candidate {
				return
			}

			// If the server requested a vote from a more up-to-date server, stop the election
			// and become a follower.
			if reply.Term > currentTerm {
				rf.transitionToFollower(reply.Term)
				return
			}

			// Tally up the vote!
			if reply.Term == currentTerm && reply.VoteGranted {
				votes += 1

				DPrintf("Candidate server %d has %d votes", rf.me, votes)

				// If you have a majority, become the leader
				if 2*votes > len(rf.peers) {
					rf.transitionToLeader()
					return
				}
			}
		}(server)
	}

	// If the election is not successful, then run another election timer.
	go rf.ticker()
}

func (rf *Raft) transitionToFollower(term int) {
	DPrintf("Transitioning server %d to Follower", rf.me)

	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.latestRpcTimestamp = time.Now()

	go rf.ticker()
}

func (rf *Raft) transitionToLeader() {
	DPrintf("Transitioning server %d to Leader", rf.me)

	rf.state = Leader

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			_, isLeader := rf.GetState()
			rf.mu.Unlock()

			// If you're no longer the leader, stop sending heartbeats.
			if !isLeader {
				return
			}

			rf.sendHeartbeats()

			// Cap heartbeat RPCs at 10 QPS
			ms := 100
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}()
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	// In parallel, send heartbeat requests through append entries
	for server, client := range rf.peers {
		args := AppendEntriesArgs{
			Term:     currentTerm,
			LeaderId: rf.me,
		}

		go func(server int, client *labrpc.ClientEnd) {
			if server == rf.me {
				return
			}

			var reply AppendEntriesReply
			if !client.Call("Raft.AppendEntries", &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > currentTerm {
				rf.transitionToFollower(reply.Term)
				return
			}
		}(server, client)
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

	// Your initialization code here (3B, 3C).
	rf.transitionToFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
