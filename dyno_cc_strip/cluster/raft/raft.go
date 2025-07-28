package raft

import (
	"context"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"
)

type nodeStatus int

const (
	FOLLOWER nodeStatus = iota
	CANDIDATE
	LEADER
)

const (
	ELEC_TIMER_MIN  = 5 * time.Second
	ELEC_TIMER_MAX  = 15 * time.Second
	HEARTBEAT_DELAY = 1 * time.Second
)

var SRV_ID string

type LogEntry struct {
	Idx  int
	Term int
	Cmd  Command
}

type (
	Command       = []string
	Configuration = []string
	NodeID        = string
	ElectionReply = struct {
		voteGranted bool
		id          NodeID
	}
	ReplicationAck = struct {
		id  NodeID
		idx int
	}
	Conn = struct {
		client     *rpc.Client
		canConnect chan struct{}
	}
)

type ConsensusModule struct {
	mu                 sync.Mutex // FIX: delete mutex and sub with narrow scoped ones
	multiElectionMutex sync.Mutex
	connMutex          sync.RWMutex

	// Filesystem related stuff
	opsInProgress map[path]opType
	files         map[path]string

	// Raft state fields
	nodeStatus  nodeStatus
	currentTerm int
	currentIdx  int
	commitIdx   int
	log         []LogEntry

	// Cluster config related fields
	clusterConfiguration map[NodeID]*Conn
	quorum               int
	clusterSize          int

	// Election related fields
	electionTimer *time.Timer
	votedFor      NodeID

	// Communication with action handler goroutine
	signalNewEntryToReplicate chan struct{}
	commitSignalingChans      map[int]chan error

	// Communication with commit handler goroutine
	replicationAckChan chan ReplicationAck

	// Signaling
	ctx       context.Context    // used by threads to receive cancel signals
	ctxCancel context.CancelFunc // the function sending those signals
}

/*
 * Public Interface
 */

// Public interface for users of the Raft Module
type CMOuterInterface interface {
	Start()
	ApplyCommand(cmd Command) (string, error)
	IsLeader() bool
}

func NewRaftInstance(cfg Configuration, srvId string) CMOuterInterface {
	SRV_ID = srvId
	return &ConsensusModule{
		mu:                 sync.Mutex{},
		multiElectionMutex: sync.Mutex{},
		connMutex:          sync.RWMutex{},

		opsInProgress: map[path]opType{},
		files:         map[path]string{},

		nodeStatus:  FOLLOWER,
		currentTerm: 0,
		currentIdx:  0,
		commitIdx:   0,
		log:         make([]LogEntry, 0),

		quorum:      len(cfg)/2 + 1,
		clusterSize: len(cfg),
		clusterConfiguration: func(cfg Configuration) map[NodeID]*Conn {
			m := map[NodeID]*Conn{}
			for _, k := range cfg {
				if k == SRV_ID {
					continue
				}
				m[k] = &Conn{
					client:     nil,
					canConnect: make(chan struct{}),
				}
			}
			return m
		}(cfg),

		electionTimer: &time.Timer{},
		votedFor:      "",

		signalNewEntryToReplicate: make(chan struct{}),
		commitSignalingChans:      make(map[int]chan error),

		replicationAckChan: make(chan ReplicationAck),

		ctx:       context.TODO(),
		ctxCancel: func() {},
	}
}

func (cm *ConsensusModule) IsLeader() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.nodeStatus == LEADER
}

// Starts all the control threads, timed events and connections
func (cm *ConsensusModule) Start() {
	// Start connection manager and RPC server
	cm.appendNewLogEntry(Command{"NOOP"})

	done := make(chan struct{})
	go cm.startRpcServer(done)
	<-done

	// PERF: clients should not be conencted mandatorily to start operations
	for node, conn := range cm.clusterConfiguration {
		if node == SRV_ID {
			continue
		}

		go cm.tryConnection(node)
		<-conn.canConnect
	}

	// Start election timer
	d := getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	cm.electionTimer = time.AfterFunc(
		d,
		cm.startElection,
	)
	log.Printf("Start: Reset election timeout to %d ns\n", d)
}

// Applies the given command to the distributed cluster
// The call blocks until the command is committed to the cluster returning a nil
// If the current node is not the leader, an error redirectng to the correct leader will be returned
// In exceptional circumstances when committing fails, an explainative error will be returned instead

func (cm *ConsensusModule) ApplyCommand(cmd Command) (string, error) {
	// INFO: only triggered when LEADER

	if !isCmdValid(cmd) {
		return "", fmt.Errorf("ApplyCommand: Command %s is not valid", cmd[0])
	}

	path := getPath(cmd)
	opType := getOpType(cmd)

	cm.mu.Lock()

	if _, ok := cm.opsInProgress[path]; ok {
		if doOpTypesConflict(opType, cm.opsInProgress[path]) {
			cm.mu.Unlock()
			return "", fmt.Errorf("ApplyCommand: A conflicting operation on %s is already in progress", cmd[1])
		}
	}

	cm.opsInProgress[path] = getOpType(cmd)

	_, ok := cm.files[cmd[1]]

	cm.mu.Unlock()

	if cmd[0] == "CREATE" && ok {
		cm.opsInProgress[cmd[1]] = NONE
		return "", fmt.Errorf("ApplyCommand: File %s already exists", cmd[1])
	} else if !ok {
		cm.opsInProgress[cmd[1]] = NONE
		return "", fmt.Errorf("ApplyCommand: File %s doesn't exist", cmd[1])
	}

	// TODO: Send heartbeat for read-only requests (Chapter 8 of Raft)

	// create and append lreset called on uninitialized timerin golog entry, return related idx
	cm.mu.Lock()
	idx := cm.appendNewLogEntry(cmd)
	cm.mu.Unlock()

	// create and store a commit signaling channel
	ch := make(chan error)
	cm.commitSignalingChans[idx] = ch

	// signal 'new entry available' to replicationManager
	cm.signalNewEntryToReplicate <- struct{}{}

	// wait for commit signal on dedicated channel, then release resources
	<-ch
	delete(cm.commitSignalingChans, idx)

	cm.mu.Lock()

	cm.applyToState(cmd)

	cm.opsInProgress[cmd[1]] = NONE // INFO: Concurrent reads are not supported

	if opType == READ {
		cm.mu.Unlock()
		return cm.files[cmd[1]], nil
	} else {
		cm.mu.Unlock()
		return "", nil
	}
}

func (cm *ConsensusModule) applyToState(cmd Command) {
	opType := getOpType(cmd)

	if opType == READ || cmd[0] == "NOOP" {
		return
	}

	switch cmd[0] {
	case "CREATE":
		cm.files[cmd[1]] = ""
	case "UPDATE":
		cm.files[cmd[1]] = cmd[2]
	case "DELETE":
		delete(cm.files, cmd[1])
	}
}

// create and append new entry to the log
// - increments the global log index and appends the new entry to the log
func (cm *ConsensusModule) appendNewLogEntry(cmd Command) int {
	cm.currentIdx++

	entry := LogEntry{
		Idx:  cm.currentIdx,
		Term: cm.currentTerm,
		Cmd:  cmd,
	}

	cm.appendToLog(entry)

	return cm.currentIdx
}

/*
 * Node state
 */

func (cm *ConsensusModule) leader2follower() {
	cm.ctxCancel()
	// FIX: ResetElectionTimer()
}

/*
 * Shared APIs INFO:their caller invokes Lock() before calling them
 */

// Checks handles eventual term increase and returns true if request is valid and shouldn't be discarded
func (cm *ConsensusModule) HandleTerm(reqTerm int, leaderID NodeID) bool {

	if reqTerm < cm.currentTerm {
		// requests generated in older terms get discarded
		log.Printf("HandleTerm: received term %d lower than own term %d\n", reqTerm, cm.currentTerm)
		return false
	}

	if reqTerm > cm.currentTerm {
		cm.votedFor = leaderID
		log.Printf("HandleTerm: set `votedFor` to '%s'\n", leaderID)

		if cm.nodeStatus == LEADER {
			log.Printf("HandleTerm: signaling leader-related threads to stop\n")
			cm.leader2follower()
		}
		cm.nodeStatus = FOLLOWER
		cm.currentTerm = reqTerm

		log.Printf("HandleTerm: setting status to follower and term to %d\n", reqTerm)
	}

	return true
}

// Resets the election timer, which starts the election process
func (cm *ConsensusModule) ResetElectionTimer() {
	d := getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	cm.electionTimer.Reset(d)
	log.Printf("ResetElectionTimer: Reset election timeout to %d ns\n", d)
}

/*
 * AppendEntries APIs
 */

// Perform consistency check to safely append logs and return the boolean result
func (cm *ConsensusModule) ConsistencyCheck(ccIdx, ccTerm int) bool {
	// Search for a matching entry
	if len(cm.log)-1 < ccIdx {
		return false
	}

	currEntry := cm.log[ccIdx]
	if currEntry.Term != ccTerm {
		return false
	}

	cm.log = cm.log[0:ccIdx]

	return true
}

func (cm *ConsensusModule) appendToLog(entry LogEntry) {
	cm.log = append(cm.log, entry)
}

// Append already consistent entry to the local log and apply configuration change if one is detected
func (cm *ConsensusModule) AppendEntry(entry LogEntry) {
	cm.appendToLog(entry)
}

func (cm *ConsensusModule) SyncCommitIdx(leaderCommitIdx int) {
	cm.commitIdx = leaderCommitIdx
}

/*
 * RequestVote APIs
 */

// Check if the candidate has appropriate term and committed index to be eligible for vote
func (cm *ConsensusModule) CanVoteFor(checkIdx, checkTerm int, id NodeID) bool {
	// INFO: Election restriction for Leader Completeness Property' (raft paper $ 5.4.1)
	// FIX: check if leader completeness check is up to spec

	last_entry := cm.log[len(cm.log)-1]
	hasHigherTerm := checkTerm > last_entry.Term
	hasHigherIdx := checkTerm == last_entry.Term && checkIdx >= last_entry.Idx
	canVote := hasHigherTerm || hasHigherIdx

	if !canVote {
		log.Printf(
			"CanVoteFor: can't vote '%s'cand['%d'@'%d'] self['%d'@'%d'] ", 
			id, 
			checkIdx, 
			checkTerm, 
			last_entry.Idx, 
			last_entry.Term,
		)
	}

	return canVote
}

// Vote for the provided and suitable candidate if didn't vote already and wether that's the case
func (cm *ConsensusModule) VoteFor(candidateID NodeID) bool {
	if cm.votedFor == "" {
		cm.votedFor = candidateID
		return true
	}
	return false
}

/*
 * Mutual exclusion related stuff
 */

func (cm *ConsensusModule) Lock() {
	cm.mu.Lock()
}

func (cm *ConsensusModule) Unlock() {
	cm.mu.Unlock()
}

/*
 * Election Logic
 */

func (cm *ConsensusModule) startElection() {

	log.Printf("========= START ELECTION for term %d =========", cm.currentTerm)
	// stop previous election if timeout occurred
	cm.mu.Lock()
	if cm.nodeStatus == CANDIDATE {
		cm.ctxCancel()
	}
	cm.mu.Unlock()

	// INFO: wait until the previous election is stopped

	cm.multiElectionMutex.Lock()
	cm.multiElectionMutex.Unlock() // FIX: Shouldn't be a mutex, but instead a channel

	ctx, cancel := context.WithCancel(context.Background())
	cm.ctx = ctx
	cm.ctxCancel = cancel

	cm.mu.Lock()

	cm.ResetElectionTimer()

	cm.nodeStatus = CANDIDATE
	cm.currentTerm++

	cm.votedFor = SRV_ID

	ch := make(chan ElectionReply, cm.clusterSize-1)

	reqArgs := RequestVoteArgs{
		CandidateID: SRV_ID,
		Term:        cm.currentTerm,
		CheckIdx:    cm.log[len(cm.log)-1].Idx,
		CheckTerm:   cm.log[len(cm.log)-1].Term,
	}

	for id := range cm.clusterConfiguration {
		if SRV_ID == id {
			continue
		}

		go func(ch chan ElectionReply) {
			select {
			case <-cm.ctx.Done():
			case ch <- ElectionReply{voteGranted: cm.sendRequestVoteRPC(id, reqArgs).VoteGranted, id: id}:
			}
		}(ch)
	}

	log.Println("startElection: sent RequestVoteRPCs asynchronously")

	cm.mu.Unlock()

	totVotes := 1
	votesGranted := 1

	// BUG: If the new election process P_2 takes the lock before the old one P_1 reaches
	// this point, some problems may arise as two election processes would run simultaneously.
	// However this won't likely happen, as it's "impossible" for P_1 to take more
	// than the least election timeout for P_2 to spawn to reach this point

	electionWon := false
	cm.multiElectionMutex.Lock()
	for (votesGranted < cm.quorum) != (totVotes-votesGranted >= cm.clusterSize-cm.quorum+1) {
		select {
		case <-cm.ctx.Done():
			cm.mu.Lock()
			log.Println("startElection: lost due to cancelation signal")
			log.Println("###################### FOLLOWER #######################")
			cm.nodeStatus = FOLLOWER
			cm.ResetElectionTimer()
			cm.mu.Unlock()
			cm.multiElectionMutex.Unlock()
			return
		case response := <-ch:
			totVotes++
			if response.voteGranted {
				log.Printf("startElection: received vote from '%s'\n", response.id)
				votesGranted++
			} else {
				log.Printf("startElection: received rejection from '%s'\n", response.id)
			}

			electionWon = votesGranted >= cm.quorum
		}
	}

	cm.ctxCancel()

	cm.multiElectionMutex.Unlock()
	cm.mu.Lock()

	if electionWon {
		log.Println("startElection: election won")
		log.Println("####################### LEADER ########################")
		cm.nodeStatus = LEADER
		cm.electionTimer.Stop()
		go cm.replicationManager()
	} else {
		log.Println("startElection: election lost")
		log.Println("###################### FOLLOWER #######################")
		cm.nodeStatus = FOLLOWER
		cm.ResetElectionTimer()
	}
	cm.mu.Unlock()
}
