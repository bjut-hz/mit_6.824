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
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
import "mit_6.824/src/labrpc"

// import "bytes"
// import "labgob"

type State int
const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)


// global term counter
var Mu sync.Mutex
var Term = 0
func getTerm() (term int) {
	Mu.Lock()

	Term = Term + 1
	term = Term

	Mu.Unlock()
	return
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term int
	Index int
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

	// channel to sync go routine
	heartBeatChan chan bool
	leaderChan chan bool
	voteChan chan bool

	currentTerm int
	votedFor int
	logs []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	state State
	voteCounter int // vote from other raft server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).


	return rf.currentTerm, rf.IsLeader()
}

func (rf *Raft) IsLeader() bool {
	return rf.state == LEADER
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	logFlag := false
	if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex == lastLogIndex) {
		logFlag = true
	}

	if (-1 == rf.votedFor || rf.me == rf.votedFor) && logFlag {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.voteChan <- true
		rf.state = FOLLOWER
	}
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) RequestAppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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

	if ok && reply.VoteGranted {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.voteCounter++
		if rf.voteCounter > len(rf.peers) / 2 && rf.state == CANDIDATE {
			rf.leaderChan <- true
		}

		rf.mu.Unlock()
	} else {
		fmt.Printf("vote failed. rpc result: %v, term: %v, VoteGranted: %v \n", ok, reply.Term, reply.VoteGranted)
	}

	return ok
}


func (rf *Raft) broadcastVoteReq() {
	voteReq := RequestVoteArgs{Term:rf.currentTerm, CandidateId:rf.me, LastLogIndex:rf.getLastLogIndex(), LastLogTerm:rf.getLastLogTerm()}
	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			voteReply := RequestVoteReply{}
			go rf.sendRequestVote(i, &voteReq, &voteReply)
		}
	}
}


func (rf *Raft) sendRequestAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)

	if ok && reply.Success {
	} else {
		fmt.Printf("append entry rpc result: %v, term: %v, success: %v \n", ok, reply.Term, reply.Success)
	}

	return ok
}

func (rf *Raft) broadcastAppendEntryReq() {
	voteReq := RequestVoteArgs{Term:rf.currentTerm, CandidateId:rf.me, LastLogIndex:rf.getLastLogIndex(), LastLogTerm:rf.getLastLogTerm()}
	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			voteReply := RequestVoteReply{}
			go rf.sendRequestVote(i, &voteReq, &voteReply)
		}
	}
}
//
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

func (rf *Raft) getLastLogTerm() int{
	return rf.logs[len(rf.logs)].Term
}

func (rf *Raft) getLastLogIndex() int{
	return rf.logs[len(rf.logs)].Index
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


	rf.heartBeatChan = make(chan bool, 100)
	rf.leaderChan = make(chan bool)
	rf.voteChan = make(chan bool)



	rf.votedFor = -1
	rf.voteCounter = 0
	rf.state = FOLLOWER

	// Your initialization code here (2A, 2B, 2C).

	// background go routine to track raft state
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <- rf.heartBeatChan:
				case <- rf.voteChan:
				case <- time.After(time.Duration(rand.Int63() % 400 + 700) * time.Millisecond):
					rf.mu.Lock()
					rf.state = CANDIDATE
					rf.mu.Unlock()
				}
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = me
				rf.mu.Unlock()

				go rf.broadcastVoteReq()

				select {
				case <- rf.heartBeatChan:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case <- rf.leaderChan:
					rf.mu.Lock()
					rf.state = LEADER
					rf.mu.Unlock()
				case <- time.After(time.Duration(rand.Int63() % 400 + 700) * time.Millisecond):
				}

			case LEADER:
				go rf.broadcastAppendEntryReq()
				time.Sleep(100 * time.Millisecond)
			}
		}


	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
