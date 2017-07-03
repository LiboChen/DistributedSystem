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
	"sync"
  "labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type State int

const (
	follower State = iota
  candidate
  leader
)

type Log struct {
	term int
	// command
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state			State
	currentTerm int
	votedFor  int				// 	should be null at the beginning
	logs			[]Log

	commitIndex int
	lastApplier int

	// use channel !!
	heartbeatCH chan bool
	voteCH chan bool
	voteCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	// for 2A only
	if len(rf.logs) == 0 {
		dummyLog := new(Log) // create a dummy log for convenience
		rf.logs = []Log{*dummyLog}
	}
	fmt.Println(rf.logs)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here (2A, 2B).
	fmt.Printf("task %v (term: %v, votedFor: %v, lastLogTerm: %v, lastLogIndex: %v), received vote request from task %v (term: %v, lastLogTerm: %v, lastLogIndex: %v)\n",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.logs) - 1, rf.logs[len(rf.logs)-1].term,
		args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Printf("task %v rejects task %v because of smaller term\n", rf.me, args.CandidateId)
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm {
		curLastLogIndex := len(rf.logs) - 1
		curLogTerm := rf.logs[curLastLogIndex].term
		if args.LastLogTerm > curLogTerm ||
			(args.LastLogTerm == curLogTerm && args.LastLogIndex >= curLastLogIndex) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				// remember to change state to follower if it's leader or candidate.
				rf.state = follower
				rf.currentTerm = args.Term
				fmt.Printf("task %v approves task %v\n", rf.me, args.CandidateId)
				return
		}
	}
	fmt.Printf("task %v rejects task %v because of other reasons\n", rf.me, args.CandidateId)
	reply.VoteGranted = false
	return
}


type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// for 2A first.
	// fmt.Printf("task %v received heartbeat with term %v\n", rf.me, args.Term)
	rf.heartbeatCH <- true
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	}
	// Remember to update own term.
	if rf.currentTerm < args.Term {
		rf.votedFor = -1 // remmeber to update this bit whenever term increases.
		rf.currentTerm = args.Term
	}
	if rf.state == candidate {
		rf.state = follower
	}
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.voteCount++
			rf.mu.Unlock()
			if rf.state == candidate && rf.voteCount + 1 > len(rf.peers) / 2 {
				rf.voteCH <- true
			}
		} else if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.state = follower
			rf.mu.Unlock()
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = follower
		}
		rf.mu.Unlock()
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func generateTimeOut() time.Duration {
	return time.Duration(rand.Intn(300) + 200) * time.Millisecond
}

func(rf *Raft) leaderAction() {
	// send heartbeat every 100ms.
	time.Sleep(10 * time.Millisecond)
	go rf.BroadcastAppendEntries()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	fmt.Printf("task %v: start another round of election\n", rf.me)
	// reset voteCount before each startElection
	rf.voteCount = 0
	// this is important. start a new round of request vote, to resolve multiple
	// candidates problem.
	rf.currentTerm++
	// remember to reset votedFor to rf.me. There is a case when old leader fails,
	// two other replicas both become candidate, but refuse to approve the other
	// task due to unreset votedFor bit (equal to the candidate ID of the failed one).
	rf.votedFor = rf.me
	req := &RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm: rf.logs[len(rf.logs) - 1].term,
	}
	rf.mu.Unlock()

	gochan := make(chan int, len(rf.peers) - 1)
	for i := 0; i < len(rf.peers) && i != rf.me; i++ {
		gochan <- i
		go func() {
			resp := &RequestVoteReply{}
			// set resp.Term??
			temp := <- gochan
			rf.sendRequestVote(temp, req, resp)
		}()
	}
}

func (rf *Raft) BroadcastAppendEntries() {
	for i := 0; i < len(rf.peers) && i != rf.me; i++ {
		req := &AppendEntriesArgs {rf.currentTerm}
		resp := &AppendEntriesReply{}
		rf.sendAppendEntries(i, req, resp)
	}
}

func(rf *Raft) candidateAction() {
	for {
		rf.startElection()
		timeout := generateTimeOut()
		select {
		case <- rf.heartbeatCH:
			// Convert to follower
			rf.mu.Lock()
			rf.state = follower
			rf.mu.Unlock()
			return
		case isLeader := <- rf.voteCH:
			if isLeader {
				rf.mu.Lock()
				rf.state = leader
				rf.mu.Unlock()
				go rf.BroadcastAppendEntries()
				return
			}
		case <- time.After(timeout):
			continue
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// currently only initializes for 2A.
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.heartbeatCH = make(chan bool)
	rf.voteCH = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// create background workers.
	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			if state == leader {
				fmt.Printf("start leader: %v, term: %v\n", rf.me, rf.currentTerm)
				rf.leaderAction()
				fmt.Printf("end leader: %v, term: %v\n", rf.me, rf.currentTerm)
			}

			if state == follower {
				fmt.Printf("start follower: %v, term: %v\n", rf.me, rf.currentTerm)
				timeout := generateTimeOut()
				select {
				case <- rf.heartbeatCH:
				case <- time.After(timeout):
					rf.mu.Lock()
					if rf.state != leader {
						rf.state = candidate
					}
					rf.mu.Unlock()
				}
				fmt.Printf("end follower: %v, term: %v\n", rf.me, rf.currentTerm)
			}

			if state == candidate {
				fmt.Printf("start candidate: %v, term: %v\n", rf.me, rf.currentTerm)
				rf.candidateAction()
				fmt.Printf("end candidate: %v, term: %v\n", rf.me, rf.currentTerm)
			}
		}
	}()

	return rf
}
