package raft

import (
	"context"
	"fmt"
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
	ELEC_TIMER_MIN  = 500 * time.Millisecond
	ELEC_TIMER_MAX  = 800 * time.Millisecond
	HEARTBEAT_DELAY = 100 * time.Millisecond
)

type logEntry struct {
	idx  int
	term int
	cmd  Command
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
		mu         *sync.Mutex
	}
)

type ConsensusModule struct {
	mu                 sync.Mutex // FIX: delete mutex and sub with narrow scoped ones
	multiElectionMutex sync.Mutex

	// Filesystem related stuff
	opsInProgress map[path]opType
	files map[path]string

	// Raft state fields
	nodeStatus  nodeStatus
	currentTerm int
	currentIdx  int
	commitIdx   int
	log         []logEntry

	// Cluster config related fields
	clusterConfiguration map[NodeID]*Conn
	quorum               int
	clusterSize          int

	// Election related fields
	electionTimer time.Timer
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
	NewRaftInstance(cfg Configuration) CMOuterInterface
	Start()
	ApplyCommand(cmd Command) (string, error)
}

func (cm *ConsensusModule) NewRaftInstance(cfg Configuration) CMOuterInterface {
	return &ConsensusModule{
		mu:                 sync.Mutex{},
		multiElectionMutex: sync.Mutex{},

		opsInProgress: map[path]opType{},
		files: map[path]string{},

		nodeStatus:  FOLLOWER,
		currentTerm: 0,
		currentIdx:  0,
		commitIdx:   0,
		log:         make([]logEntry, 0),

		quorum:      len(cfg)/2 + 1,
		clusterSize: len(cfg),
		clusterConfiguration: func(cfg Configuration) map[NodeID]*Conn {
			m := map[NodeID]*Conn{}
			for _, k := range cfg {
				m[k] = &Conn{
					client:     nil,
					mu:         &sync.Mutex{},
					canConnect: make(chan struct{}),
				}
			}
			return m
		}(cfg),

		electionTimer: time.Timer{},
		votedFor:      "",

		signalNewEntryToReplicate: make(chan struct{}),
		commitSignalingChans:      make(map[int]chan error),

		replicationAckChan: make(chan ReplicationAck),

		ctx:       context.TODO(),
		ctxCancel: func() {},
	}
}

// Starts all the control threads, timed events and connections
func (cm *ConsensusModule) Start() {
	// Start connection manager and RPC server
	go cm.startRpcServer()

	for node := range cm.clusterConfiguration {
		go cm.tryConnection(node)
	}

	// Start election timer
	cm.electionTimer = *time.AfterFunc(
		getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX),
		cm.startElection,
	)
}

// Applies the given command to the distributed cluster
// The call blocks until the command is committed to the cluster returning a nil
// If the current node is not the leader, an error redirectng to the correct leader will be returned
// In exceptional circumstances when committing fails, an explainative error will be returned instead

func (cm *ConsensusModule) ApplyCommand(cmd Command) (string, error) {
	// INFO: only triggered when LEADER

	if !isCmdValid(cmd) {
		return "", fmt.Errorf("Command %s is not valid", cmd[0])
	}

	path := getPath(cmd)
	opType := getOpType(cmd)

	if _, ok := cm.opsInProgress[path]; ok {
		if doOpTypesConflict(opType, cm.opsInProgress[path]) {
			return "", fmt.Errorf("A conflicting operation on %s is already in progress", cmd[1])
		}
	}

	cm.opsInProgress[path] = getOpType(cmd)

	_, ok := cm.files[cmd[1]]

	if cmd[0] == "CREATE" && ok {
		cm.opsInProgress[cmd[1]] = NONE
		return "", fmt.Errorf("File %s already exists", cmd[1])
	} else if !ok {
		cm.opsInProgress[cmd[1]] = NONE
		return "", fmt.Errorf("File %s doesn't exist", cmd[1])
	}

	// TODO: Send heartbeat for read-only requests (Chapter 8 of Raft)

	// create and append log entry, return related idx
	idx := cm.appendNewLogEntry(cmd)

	// create and store a commit signaling channel
	ch := make(chan error)
	cm.commitSignalingChans[idx] = ch

	// signal 'new entry available' to replicationManager
	cm.signalNewEntryToReplicate <- struct{}{}

	// wait for commit signal on dedicated channel, then release resources
	<-ch
	delete(cm.commitSignalingChans, idx)

	cm.applyToState(cmd)

	cm.opsInProgress[cmd[1]] = NONE // INFO: Concurrent reads are not supported

	if opType == READ {
		return cm.files[cmd[1]], nil
	} else {
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

	entry := logEntry{
		idx:  cm.currentIdx,
		term: cm.currentTerm,
		cmd:  cmd,
	}

	cm.appendToLog(entry)

	return cm.currentIdx
}

/*
 * Node state
 */

func (cm *ConsensusModule) leader2follower() {
	cm.ctxCancel()
}

/*
 * Shared APIs
 */

// Checks handles eventual term increase and returns true if request is valid and shouldn't be discarded
func (cm *ConsensusModule) HandleTerm(reqTerm int, leaderID NodeID) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if reqTerm < cm.currentTerm {
		// requests generated in older terms get discarded
		return false
	}

	if reqTerm > cm.currentTerm {
		cm.votedFor = leaderID
		if cm.nodeStatus == LEADER {
			cm.leader2follower()
		}
		cm.nodeStatus = FOLLOWER
		cm.currentTerm = reqTerm
	}

	return true
}

// Resets the election timer, which starts the election process
func (cm *ConsensusModule) ResetElectionTimer() {
	d := getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	cm.mu.Lock()
	cm.electionTimer.Reset(d)
	cm.mu.Unlock()
}

/*
 * AppendEntries APIs
 */

// Perform consistency check to safely append logs and return the boolean result
func (cm *ConsensusModule) ConsistencyCheck(ccIdx, ccTerm int) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	matchingEntryIndex := -1
	// Search for a matching entry
	for i := range cm.log {
		currEntry := cm.log[len(cm.log)-i-1]
		if currEntry.idx == ccIdx && currEntry.term == ccTerm {
			matchingEntryIndex = i
			break
		}
	}

	if matchingEntryIndex != -1 {
		cm.log = cm.log[0:matchingEntryIndex]
	}

	return matchingEntryIndex != -1
}

func (cm *ConsensusModule) appendToLog(entry logEntry) {
	cm.log = append(cm.log, entry)
}

// Append already consistent entry to the local log and apply configuration change if one is detected
func (cm *ConsensusModule) AppendEntry(entry logEntry) {
	cm.appendToLog(entry)
}

func (cm *ConsensusModule) SyncCommitIdx(leaderCommitIdx int) {
	cm.commitIdx = leaderCommitIdx
}

/*
 * RequestVote APIs
 */

// Check if the candidate has appropriate term and committed index to be eligible for vote
func (cm *ConsensusModule) CanVoteFor(checkIdx, checkTerm int) bool {
	// INFO: Election restriction for Leader Completeness Property' (raft paper $ 5.4.1)
	// FIX: check if leader completeness check is up to spec
	last_entry := cm.log[len(cm.log)-1]
	hasHigherTerm := checkTerm > last_entry.term
	hasHigherIdx := checkTerm == last_entry.term && checkIdx > last_entry.idx
	return hasHigherTerm || hasHigherIdx
}

// Vote for the provided and suitable candidate if didn't vote already and wether that's the case
func (cm *ConsensusModule) VoteFor(candidateID NodeID) bool {
	if cm.votedFor != "" {
		cm.votedFor = candidateID
		return true
	}
	return false
}

/*
 * Election Logic
 */

func (cm *ConsensusModule) startElection() {

	// stop previous election if timeout occurred
	if cm.nodeStatus == CANDIDATE {
		cm.ctxCancel()
	}

	// INFO: wait until the previous election is stopped

	cm.multiElectionMutex.Lock()
	cm.multiElectionMutex.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cm.ctx = ctx
	cm.ctxCancel = cancel

	cm.ResetElectionTimer()

	cm.nodeStatus = CANDIDATE
	cm.currentTerm++

	cm.votedFor = SRV_ID

	ch := make(chan ElectionReply, cm.clusterSize - 1)

	reqArgs := RequestVoteArgs{
		candidateID: SRV_ID,
		term:        cm.currentTerm,
		checkIdx:    cm.log[len(cm.log)-1].idx,
		checkTerm:   cm.log[len(cm.log)-1].term,
	}

	for id := range cm.clusterConfiguration {
		if SRV_ID == id {
			continue
		}

		go func(ch chan ElectionReply) {
			select {
			case <-cm.ctx.Done():
			case ch <- ElectionReply{voteGranted: cm.sendRequestVoteRPC(id, reqArgs).voteGranted, id: id}:
			}
		}(ch)
	}

	totVotes := 1
	votesGranted := 1

	// BUG: If the new election process P_2 takes the lock before the old one P_1 reaches
	// this point, some problems may arise as two election processes would run simultaneously.
	// However this won't likely happen, as it's "impossible" for P_1 to take more
	// than 150 ms (the least election timeout for P_2 to spawn) to reach this point

	electionWon := false
	cm.multiElectionMutex.Lock()
	for votesGranted < cm.quorum || totVotes-votesGranted >= cm.clusterSize-cm.quorum+1 {
		select {
		case <-cm.ctx.Done():
			cm.nodeStatus = FOLLOWER
			cm.ResetElectionTimer()
			cm.multiElectionMutex.Unlock()
			return
		case response := <-ch:
			totVotes++
			if response.voteGranted {
				votesGranted++
			}

			electionWon = votesGranted >= cm.quorum
		}
	}

	cm.ctxCancel()

	cm.multiElectionMutex.Unlock()

	if electionWon {
		cm.nodeStatus = LEADER
		cm.electionTimer.Stop()
		go cm.replicationManager()
	} else {
		cm.nodeStatus = FOLLOWER
		cm.ResetElectionTimer()
	}
}
