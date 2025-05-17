package raft

import (
	"errors"
	"math/rand"
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
	ELEC_TIMER_MIN = 500 * time.Millisecond
	ELEC_TIMER_MAX = 800 * time.Millisecond
)

type logEntry struct {
	idx  int
	term int
	cmd  Command
}

type (
	Command       []string
	Configuration []string
	NodeID        string
)

type ConsensusModule struct {
	mu sync.Mutex

	// Raft state fields
	nodeStatus  nodeStatus
	currentTerm int
	currentIdx  int
	commitIdx   int
	log         []logEntry

	// Cluster config related fields
	clusterConfiguration map[NodeID]*rpc.Client
	nonVotingNodes       []NodeID
	oldConfig            []NodeID
	newConfig            []NodeID
	isIntermediateConfig bool

	// Election related fields
	electionTimer    time.Timer
	lastRpcTimestamp time.Time
	votedFor         NodeID

	// Communication with action handler goroutine
	cliCmdRequests  chan Command
	cliCmdResponses chan error

	// Signaling for leader configuration change
	configChanges chan Command

	// Communication with the connection manager
	newConnChan chan NodeID // add new connections
	delConnChan chan NodeID // delete old connections

	// Communication with commit handler goroutine
	commitChan chan int // send index to commit
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
	// Start connection manager
	go cm.connectionManager()

	// Initialise new connections
	for _, node := range cm.newConfig {
		cm.newConnChan <- node
	}
	cm.newConfig = []NodeID{}

	// Start election timer
	cm.electionTimer = *time.AfterFunc(getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX), cm.startElection)

	// TODO: start rpc server
	// TODO: start client cmd handler

	// TODO: start all handlers

	// TODO: implement duty cycle
}

// Aplies the given command to the distributed cluster
// The call blocks until the command is committed to the cluster returning a nil
// If the current node is not the leader, an error redirecting to the correct leader will be returned
// In exceptional circumstances when committing fails, an explainative error will be returned instead
func (cm *ConsensusModule) ApplyCommand(cmd Command) error {
	cm.mu.Lock()
	if cm.nodeStatus != LEADER {
		// If the node is not the leader, send an error specifying the correct leader
		cm.mu.Unlock()
		// FIX: should be discrete error type
		return errors.New("Not the leader, contact " + string(cm.votedFor))
	}
	cm.mu.Unlock()

	cm.cliCmdRequests <- cmd
	response := <-cm.cliCmdResponses
	// TODO: add leader logic

	return response
}

// receives commands from clients and starts the replication process
func (cm *ConsensusModule) replicationHandler() {
	for {
		cmd := <-cm.cliCmdRequests
		cm.mu.Lock()
		cm.currentIdx++
		entry := logEntry{
			idx:  cm.currentIdx,
			term: cm.currentTerm,
			cmd:  Command(cmd),
		}

		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) commitHandler() {
}

/*
 * Shared APIs
 */

// Checks handles eventual term increase and returns true if request is valid and shouldn't be discarded
func (cm *ConsensusModule) HandleTerm(reqTerm int, leaderID NodeID) bool {
	if reqTerm < cm.currentTerm {
		// requests generated in older terms get discarded
		return false
	}

	if reqTerm > cm.currentTerm {
		// register term change and fallback to Follower
		cm.mu.Lock()
		cm.currentTerm = reqTerm
		cm.nodeStatus = FOLLOWER
		cm.mu.Unlock()
	}

	return true
}

// Resets the election timer, which starts the election process
func (cm *ConsensusModule) ResetElectionTimer() {
	d := getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	cm.electionTimer.Reset(d)
	cm.lastRpcTimestamp = time.Now()
}

/*
 * AppendEntries APIs
 */

// Perform consistency ccheck for safely appending logs and return the boolean result
func (cm *ConsensusModule) ConsistencyCheck(ccIdx, ccTerm int) bool {
	// WARN: if executing appendEntries concurrently, another log may be appended between consistency check and actual appending
	lastEntry := cm.log[len(cm.log)-1]

	// FIX: delete entries if no match ??

	return lastEntry.idx == ccIdx && lastEntry.term == ccTerm
}

// Append already consistent entry to the local log and apply configuration change if one is detected
func (cm *ConsensusModule) AppendEntry(entry logEntry) {
	if entry.cmd[0] == "CC" {
		// if config change detected, start the application process
		cm.applyFollowerConfigChange(Configuration(entry.cmd[1:]))
	}

	cm.log = append(cm.log, entry)
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
 * Configuration Change Logic
 */

// Apply the configuratino change to a receiving follower
func (cm *ConsensusModule) applyFollowerConfigChange(cfg Configuration) {
	defer cm.mu.Unlock()
	cm.mu.Lock()

	if cfg[0] == "IC" {
		// if 'IF' flag set, set intermediate config
		cm.isIntermediateConfig = true

		// make the old config from the actual cluste config
		cm.oldConfig = make([]NodeID, 0, len(cm.clusterConfiguration))
		for k := range cm.clusterConfiguration {
			cm.oldConfig = append(cm.oldConfig, k)
		}

		// make the new config from the received config
		cm.newConfig = make([]NodeID, 0, len(cfg[1:]))
		for _, v := range cfg[1:] {
			cm.newConfig = append(cm.newConfig, NodeID(v))
		}

		// add connections for the new nodes
		for _, newNode := range cfg[1:] {
			found := false
			for _, v := range cm.oldConfig {
				found = found || NodeID(newNode) == v
			}

			if !found {
				// send the NodeID to the connManager to add the connection
				cm.newConnChan <- NodeID(newNode)
			}
		}
	}

	if cfg[0] == "NC" {
		// delete connections for the old nodes
		for _, oldNode := range cm.oldConfig {
			found := false
			for _, v := range cm.newConfig {
				found = found || NodeID(oldNode) == v
			}

			if !found {
				// send the NodeID to the connManager to delete the connection
				cm.delConnChan <- NodeID(oldNode)
			}
		}

		// reset intermediate config fields
		cm.oldConfig = []NodeID{}
		cm.newConfig = []NodeID{}
		cm.isIntermediateConfig = false
	}
}

/*
 * Election Logic
 */

func (cm *ConsensusModule) startElection() {
	// TODO: implement eletion logic
	panic("IMPLEMENT ConsensusModule.startElection()")
}

/*
 * Utility functions
 */

// Get a random positive duration with Millisecond granularity in a given range of durations
//   - when min > max, the vaules are inverted so the result will always be positive
func getRandomDuration(dMin, dMax time.Duration) time.Duration {
	tMin := dMin.Milliseconds()
	tMax := dMax.Milliseconds()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := rnd.Int63n(tMax-tMin+1) + tMin

	return time.Duration(n).Abs() * time.Microsecond
}
