package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labrpc"
	"6.5840/labgob"
	"log"
	"bytes"
)

const (
	Leader = iota
	Candidate
	Follower
)

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
	Term    int
	Command interface{}
}

type Raft struct {
	mu        	sync.Mutex          
	peers     	[]*labrpc.ClientEnd 
	persister 	*Persister          
	me        	int                 
	dead      	int32               
	applyCh   	chan ApplyMsg	  

	log         []LogEntry        
	currentTerm int
	votedFor   	int

	commitIndex int
	lastApplied int

	nextIndex  	[]int
	matchIndex 	[]int

	state 		int      

	applyMsgCond 		*sync.Cond		
	electionTimeoutDur 	int				
	lastTimestamp time.Time
}

// The GetState function returns the current term and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()
	return term, isLeader
}

func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	// Serialize the state fields
	if err := encoder.Encode(rf.currentTerm); err != nil {
		log.Fatalf("Error encoding currentTerm: %v", err)
	}
	if err := encoder.Encode(rf.votedFor); err != nil {
		log.Fatalf("Error encoding votedFor: %v", err)
	}
	if err := encoder.Encode(rf.log); err != nil {
		log.Fatalf("Error encoding log: %v", err)
	}

	rf.persister.Save(buffer.Bytes(), nil)
}

func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()

	if data == nil || len(data) == 0 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var currentTerm int
	var votedFor int
	var logEntries []LogEntry

	if err := decoder.Decode(&currentTerm); err != nil {
		log.Fatalf("Error decoding currentTerm: %v", err)
	}
	if err := decoder.Decode(&votedFor); err != nil {
		log.Fatalf("Error decoding votedFor: %v", err)
	}
	if err := decoder.Decode(&logEntries); err != nil {
		log.Fatalf("Error decoding logEntries: %v", err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logEntries
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool		
	ConflictTerm int 	
	ConflictIndex int	
}

// The AppendEntryHandler function is the RPC handler for AppendEntries RPC.
func (rf *Raft) AppendEntryHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed(){
		return
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.currentTerm < args.Term || rf.state == Candidate{
		rf.currentTerm = args.Term
		rf.convert2Follower()
		rf.persist()
	}
	rf.lastTimestamp = time.Now()
	reply.Term = rf.currentTerm
	if args.PrevLogIndex>=len(rf.log){
		reply.ConflictIndex = len(rf.log)
		reply.Success = false
		return
	}else if (args.PrevLogIndex>=0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm){
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i, val := range rf.log{
			if val.Term == reply.ConflictTerm{
				reply.ConflictIndex = i 
				break
			}
		}
		reply.Success = false
		return
	}
	for i := range args.Entries{
		if args.PrevLogIndex+1+i >= len(rf.log) || args.Entries[i].Term != rf.log[args.PrevLogIndex+1+i].Term{
			rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex{
		lastEntryIndex := len(rf.log)-1
		rf.commitIndex = min(args.LeaderCommit, lastEntryIndex)
		rf.applyMsgCond.Broadcast()
		rf.persist()
	}
	reply.Term = rf.currentTerm
}

// The sendAppendEntries function sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", args, reply)
	return ok
}

// The CondInstallSnapshot function is a service wants to switch to snapshot.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// The RequestVote function is the RPC handler for RequestVote RPC.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed(){
		return 
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convert2Follower()
		rf.persist()
	}
	reply.Term = rf.currentTerm
	higherTermCheck := true
	higherIndexCheck := true
	if len(rf.log)>0{
		lastLogEntry := rf.log[len(rf.log) - 1]
		higherTermCheck = args.LastLogTerm > lastLogEntry.Term
		higherIndexCheck = (args.LastLogTerm == lastLogEntry.Term) && (args.LastLogIndex >= len(rf.log)-1)
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (higherTermCheck || higherIndexCheck){
		rf.votedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
		rf.lastTimestamp = time.Now()
	}
	reply.Term = rf.currentTerm
}

// The sendRequestVote function sends a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The Start function starts agreement on a new log entry.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if rf.killed(){
		return index, term, isLeader
	}
	if isLeader{
		newEntry := LogEntry{
			Term: rf.currentTerm,
			Command: command,
		}
		index = len(rf.log)
		rf.log = append(rf.log, newEntry)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
	}

	return index, term, isLeader
}

// The Kill function kills the Raft server.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// The killed function checks if the Raft server has been killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The Make function creates a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.votedFor = -1
	rf.state = Follower
	rf.log = make([] LogEntry, 1)
	rf.applyCh = applyCh
	rf.nextIndex = make([] int, len(peers))
	rf.matchIndex = make([] int, len(peers))
	rf.electionTimeoutDur = rand.Intn(250) + 400
	rf.lastTimestamp = time.Now()
	rf.applyMsgCond = sync.NewCond(&rf.mu)
	rf.readPersist()
	//rf.readPersist(persister.ReadRaftState())
	go rf.startMainRoutine()
	go rf.applyCommittedEntries()
	return rf
}

// The updateCommitIndex function updates the commit index based on the match index.
func (rf *Raft) updateCommitIndex(){
	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	for i:=len(rf.log)-1 ; i>rf.commitIndex ; i-- {	
			count := 0
			for _, val := range matches{
				if val >= i{
					count++
					if (count > len(matches)/2) && (rf.log[i].Term == rf.currentTerm){
							rf.commitIndex = i
							rf.applyMsgCond.Broadcast()
							rf.persist()
							return
					}
				}
			}
	}
}

// The applyCommittedEntries function applies committed entries to the state machine. 
func (rf *Raft) applyCommittedEntries(){
	for !rf.killed(){
		rf.mu.Lock()
		rf.applyMsgCond.Wait()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		log := make([]LogEntry, len(rf.log))
		copy(log, rf.log)
		rf.mu.Unlock()
		if lastApplied < commitIndex{
			for i:=lastApplied + 1;i<= commitIndex ; i++ {
				newApplyMsg := ApplyMsg{
					CommandValid: true,
					Command: log[i].Command,
					CommandIndex: i,
				}
				rf.mu.Lock()
				rf.applyCh <- newApplyMsg
				rf.lastApplied = i
				rf.mu.Unlock()
			}
		}
	}
}

// The getElectionTimeoutDuration function returns the election timeout duration.
func (rf *Raft) getElectionTimeoutDuration() time.Duration {
	return time.Duration(rf.electionTimeoutDur) * time.Millisecond
}

// The randomizeTimerDuration function randomizes the election timeout duration.
func (rf *Raft) randomizeTimerDuration(){
	rf.electionTimeoutDur = rand.Intn(250) + 400
}

// The startMainRoutine function starts the main routine of the Raft server.
func (rf *Raft) startMainRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			rf.startLeaderHeartBeats()
			time.Sleep(time.Millisecond * 100)
		case Candidate:
			rf.startCandidateElection()
		case Follower:
			rf.startFollower()
		default:
			panic("No such state !")
		}
	}
}

// The startLeaderHeartBeats function starts sending heartbeats to followers.
func (rf *Raft) startLeaderHeartBeats() {
	for i := range rf.peers {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed(){
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}
}

// The sendHeartBeat function sends a heartbeat to a follower.
func (rf *Raft) sendHeartBeat(server int){
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}	
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LeaderCommit = rf.commitIndex   
	args.PrevLogIndex = rf.nextIndex[server]-1
	if args.PrevLogIndex>=0{
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	if len(rf.log) > rf.nextIndex[server]{
		newEntries := make([] LogEntry, len(rf.log)-rf.nextIndex[server])
		copy(newEntries, rf.log[rf.nextIndex[server]:])
		args.Entries = newEntries
	}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok{
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm{ 
			return
		}
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.convert2Follower()
			return
		}
		if reply.Success && reply.Term == rf.currentTerm{
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.updateCommitIndex()
			return
		}
		if reply.ConflictTerm != -1{
			found := false
			for i := len(rf.log)-1 ; i>0 ; i--{
				if rf.log[i].Term == reply.ConflictTerm{
					rf.nextIndex[server] = min(i, reply.ConflictIndex) 
					found = true
					break
				}
			}
			if !found{
				rf.nextIndex[server] = reply.ConflictIndex;
			}
		}else{
			rf.nextIndex[server] = reply.ConflictIndex;
		}	
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
}

// The startCandidateElection function starts an election as a candidate.
func (rf *Raft) startCandidateElection() {
	rf.mu.Lock()
	rf.randomizeTimerDuration()
	rf.currentTerm++
	rf.persist()
	totalVotes := 1
	processedVotes := 1
	rf.votedFor = rf.me
	majority := len(rf.peers)/2 + 1
	electionStartTime := time.Now()
	rf.mu.Unlock()
	cond := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				getVote := rf.setUpSendRequestVote(server)
				rf.mu.Lock()
				if getVote!=-1{
					processedVotes++
					if getVote==1{
						totalVotes++
					}
				}
				cond.Broadcast()
				rf.mu.Unlock()
			}(i)
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for processedVotes!=len(rf.peers) && totalVotes < majority && rf.state == Candidate && time.Now().Sub(electionStartTime)<rf.getElectionTimeoutDuration(){
		cond.Wait()
	}
	if rf.state == Candidate && totalVotes >= majority{
		rf.convert2Leader()
	}
}

// The setUpSendRequestVote function sets up and sends a RequestVote RPC.
func (rf *Raft) setUpSendRequestVote(server int) int {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	rf.mu.Lock()
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.log)-1
	if len(rf.log)>0{
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm{
			return -1
		}
		
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.convert2Follower()
			return 0
		}

		if reply.VoteGranted{
			return 1
		}else{
			return 0
		}
	}
	return 0
}

// The startFollower function starts the follower behavior.
func (rf *Raft) startFollower() {
	for !rf.killed() {
		rf.mu.Lock()
		if time.Since(rf.lastTimestamp) >= rf.getElectionTimeoutDuration(){
			rf.state = Candidate
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// The convert2Follower function converts the server to a follower.
func (rf *Raft) convert2Follower(){
	rf.state = Follower
	rf.votedFor = -1
	rf.randomizeTimerDuration()
}

// The convert2Leader function converts the server to a leader.
func (rf *Raft) convert2Leader(){
	rf.state = Leader 
	rf.initializeLeaderState()
}

// The initializeLeaderState function initializes the state of a new leader.
func (rf *Raft) initializeLeaderState(){
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log)-1
	for i := range rf.peers{
		if i!=rf.me{
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
	}
}

// The min function returns the minimum of two integers.
func min(a, b int) int{
	if a < b{
		return a
	}
	return b 
}