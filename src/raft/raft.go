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
	//"io"
	//"log"
	"math/rand"
	//"strconv"
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
	CommandValid			bool
	Command      			interface{}
	CommandIndex 					int
}

type LogEntry struct {
	Command 				interface{}
	Term 					int
	Index					int
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
	heartBeatChan 			chan bool		// 收到心跳包rpc时
	leaderChan 				chan bool		// 变成leader时
	voteChan 				chan bool		// 投完票后
	commitChan				chan bool		// 提交日志

	currentTerm 			int				// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor 				int				// candidateId that received vote in current term (or null if none)
	logs 					[]LogEntry		// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex 			int				// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied 			int				// index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// (Reinitialized after election)
	nextIndex 				[]int			// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex 				[]int			// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	state 					State			// current server's state
	voteCounter 			int 			// vote from other raft server
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
	Term 			int		// candidate’s term
	CandidateId 	int		// candidate requesting vote
	LastLogIndex 	int		// index of candidate’s last log entry (§5.4)
	LastLogTerm 	int		// term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int			// currentTerm, for candidate to update itself
	VoteGranted bool		// true means candidate received vote
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
	Term 						int						// leader’s term
	LeaderId 					int						// so follower can redirect clients
	PrevLogIndex 				int						// index of log entry immediately preceding new ones
	PrevLogTerm 				int						// term of prevLogIndex entry
	Entries 					[]LogEntry				// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 				int						// leader’s commitIndex
}

type AppendEntriesReply struct {
	Term 	int			// currentTerm, for leader to update itself
	Success bool		// true if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int		// receiver's next log index
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.heartBeatChan <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1 // follower 期待的log index,
		return
	}

	// 当刚启动的时候, PrevLogIndex与getLastLogIndex()相等,都是0
	// 同步follower与leader的日志信息
	baseIndex := rf.logs[0].Index // 0
	if args.PrevLogIndex > baseIndex {
		term := rf.logs[args.PrevLogIndex - baseIndex].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.logs[i-baseIndex].Term != term {
					// 日志刚开始的时候都是空的
					reply.NextIndex = i + 1
					break
				}
			}

			return
		}
	}

	if args.PrevLogIndex < baseIndex {

	} else {
		rf.logs = rf.logs[:args.PrevLogIndex + 1 - baseIndex]
		rf.logs = append(rf.logs, args.Entries...)

		reply.Success = true
		reply.NextIndex = rf.getLastLogIndex() + 1
	}

	// 提交日志
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastLogIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//DPrintf("server %v, commit log", rf.me)
		rf.commitChan <- true
	}

	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		term := rf.currentTerm
		if rf.state != CANDIDATE {
			return ok
		}

		if args.Term != term {
			return ok
		}

		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}

		if reply.VoteGranted {
			rf.voteCounter++
			if rf.voteCounter > len(rf.peers) / 2 && rf.state == CANDIDATE {
				//
				// rf.state = FOLLOWER
				rf.leaderChan <- true
			}
		}

		//rf.mu.Unlock()
	} else {
		//DPrintf("vote failed. rpc result: %v, term: %v, VoteGranted: %v \n", ok, reply.Term, reply.VoteGranted)
	}

	return ok
}


func (rf *Raft) broadcastVoteReq() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteReq := RequestVoteArgs{Term:rf.currentTerm, CandidateId:rf.me, LastLogIndex:rf.getLastLogIndex(), LastLogTerm:rf.getLastLogTerm()}

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				voteReply := RequestVoteReply{}
				rf.sendRequestVote(i, &voteReq, &voteReply)
			}(i)
		}
	}
}


func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		// raft整个协议中节点的状态以及term是实时发生变化的,对于每个操作,尽可能的去拿最新的值进行再判断
		if rf.state != LEADER {
			return ok
		}

		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	} else {
		DPrintf("append entry rpc result: %v, term: %v, success: %v \n", ok, reply.Term, reply.Success)
	}

	return ok
}

func (rf *Raft) broadcastAppendEntriesReq() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("broadcastAppendEntriesReq: %v\n", rf)
	N := rf.commitIndex
	lastIndex := rf.getLastLogIndex()
	baseIndex := rf.logs[0].Index

	for i := rf.commitIndex; i <= lastIndex; i++ {
		// 利用matchIndex判断该条目是否已经被复制到了大部分server上
		// 进行日志提交判断
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.logs[i-baseIndex].Term == rf.currentTerm {
				num++
			}
		}

		if 2 * num > len(rf.peers) {
			N = i
		}
	}

	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitChan <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex - baseIndex].Term
				// 复制最新的日志
				args.Entries = make([]LogEntry, len(rf.logs[args.PrevLogIndex + 1 - baseIndex:]))
				copy(args.Entries, rf.logs[args.PrevLogIndex + 1 - baseIndex:])
				args.LeaderCommit = rf.commitIndex

				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendRequestAppendEntries(i, &args, &reply)
				}(i, args)
			} else {
				// install snapshot
			}
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
	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()


	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command, Index: index})
		rf.persist()
	}
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
	return rf.logs[len(rf.logs) - 1].Term
}

func (rf *Raft) getLastLogIndex() int{
	return len(rf.logs) - 1
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

	rf.votedFor = -1
	rf.state = FOLLOWER

	rf.logs = append(rf.logs, LogEntry{Term:0})  // add dummy
	rf.currentTerm = 0
	rf.heartBeatChan = make(chan bool, 100)
	rf.leaderChan = make(chan bool, 100)
	rf.voteChan = make(chan bool, 100)
	rf.commitChan = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
				rf.voteCounter = 1
				rf.mu.Unlock()

				DPrintf("server %v become a CANDIDATE. term: %v", rf.me, rf.currentTerm)

				go rf.broadcastVoteReq()

				select {
				case <- rf.heartBeatChan:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()

					DPrintf("server %v become a FOLLOWER. term: %v", rf.me, rf.currentTerm)
				case <- rf.leaderChan:
					rf.mu.Lock()
					rf.state = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					// 初始化每个follower的log匹配信息
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()

					DPrintf("server %v become a LEADER. term: %v", rf.me, rf.currentTerm)
				case <- time.After(time.Duration(rand.Int63() % 400 + 700) * time.Millisecond):
				}

			case LEADER:
				go rf.broadcastAppendEntriesReq()
				time.Sleep(100 * time.Millisecond)  // 每秒10次
			}
		}


	}()

	// commit log
	go func() {
		for {
			select {
			case <- rf.commitChan:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.logs[0].Index

				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandIndex:i, Command: rf.logs[i - baseIndex].Command, CommandValid: true}
					//DPrintf("server %v, commit log entry, command: %v", rf.me, msg.Command)
					applyCh <- msg
					rf.lastApplied = i
				}

				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
