package raft

import (
	"context"
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
)

type ConsensusModule struct {
	mu                 sync.Mutex
	multiElectionMutex sync.Mutex

	// Raft state fields
	nodeStatus  nodeStatus
	currentTerm int
	currentIdx  int
	commitIdx   int
	log         []logEntry

	// Cluster config related fields
	clusterConfiguration map[NodeID]*rpc.Client
	quorum               int
	clusterSize          int

	// Election related fields
	electionTimer    time.Timer
	lastRpcTimestamp time.Time
	votedFor         NodeID

	// Communication with action handler goroutine
	signalNewEntryToReplicate chan struct{}
	commitSignalingChans      map[int]chan error

	// Communication with the connection manager
	newConnChan chan NodeID // add new connections
	delConnChan chan NodeID // delete old connections

	// Communication with commit handler goroutine
	replicationAckChan chan ReplicationAck
	commitChan         chan int // send index to commit

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
	ApplyCommand(cmd Command)
}

// TODO: write Setup function

// Starts all the control threads, timed events and connections
func (cm *ConsensusModule) Start() {
	// Start connection manager and RPC server
	go cm.startRpcServer()
	go cm.connectionManager()

	// TODO: Initialise new connections

	// Start election timer
	cm.electionTimer = *time.AfterFunc(getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX), cm.startElection)

	// TODO: start all handlers
}

// Applies the given command to the distributed cluster
// The call blocks until the command is committed to the cluster returning a nil
// If the current node is not the leader, an error redirectng to the correct leader will be returned
// In exceptional circumstances when committing fails, an explainative error will be returned instead

func (cm *ConsensusModule) ApplyCommand(cmd Command) error {
	// INFO: only triggered when LEADER

	// TODO: check command consistency
	// - if there's not a conflicting request in progress
	// - if the operation is consistent with existance of the target resource

	// TODO: Send heartbeat for read-only requests

	// create and append log entry, return related idx
	idx := cm.appendNewLogEntry(cmd)

	// create and store a commit signaling channel
	ch := make(chan error)
	cm.commitSignalingChans[idx] = ch

	// signal 'new entry available' to replicationManager
	cm.signalNewEntryToReplicate <- struct{}{}

	// wait for commit signal on dedicated channel, then release resources
	response := <-ch
	close(ch)
	delete(cm.commitSignalingChans, idx)

	// TODO: #712.1 Apply to state (trigger callback)

	return response
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
		// TODO: Save leader's id if needed
		// register term change and fallback to Follower
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
	cm.lastRpcTimestamp = time.Now()
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
	for i, _ := range cm.log {
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
	oldCommitIdx := cm.commitIdx
	cm.commitIdx = leaderCommitIdx

	for idx := range leaderCommitIdx - oldCommitIdx {
		cm.commitChan <- oldCommitIdx + idx + 1
	}
}

/*
 * RequestVote APIs
 */

// Tell if the request came in the appropriate election window
func (cm *ConsensusModule) ValidVoteRequest() bool {
	// tell if the minimum election timer has elapsed from the last RPC was received
	return time.Now().After(cm.lastRpcTimestamp.Add(ELEC_TIMER_MIN))
}

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

	cm.multiElectionMutex.Lock()
	cm.multiElectionMutex.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cm.ctx = ctx
	cm.ctxCancel = cancel

	cm.ResetElectionTimer()

	cm.nodeStatus = CANDIDATE
	cm.currentTerm++

	ch := make(chan ElectionReply)

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
			res := cm.sendRequestVoteRPC(id, reqArgs)
			select {
			case ch <- ElectionReply{voteGranted: res.voteGranted, id: id}:
			default:
			}
		}(ch)
	}

	totVotes := 1
	votesGranted := 1

	electionWon := false
	for votesGranted < cm.quorum || totVotes-votesGranted >= cm.clusterSize-cm.quorum+1 {
		cm.multiElectionMutex.Lock()
		select {
		case <-cm.ctx.Done():
			cm.nodeStatus = FOLLOWER
			cm.ResetElectionTimer()
			cm.multiElectionMutex.Unlock()
			return
		case response := <-ch:
			cm.multiElectionMutex.Unlock()
			totVotes++
			if response.voteGranted {
				votesGranted++
			}

			electionWon = votesGranted >= cm.quorum
		}

	}

	if electionWon {
		cm.nodeStatus = LEADER
		cm.electionTimer.Stop()
		go cm.replicationManager()
	} else {
		cm.nodeStatus = FOLLOWER
		cm.ResetElectionTimer()
	}
}
