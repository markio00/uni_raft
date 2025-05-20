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
	Command        []string
	Configuration  []string
	NodeID         string
	ReplicationAck struct {
		id  NodeID
		idx int
	}
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
	cliCmdRequests     chan Command
	cliCmdResponses    chan error
	replicatorChannels map[NodeID]chan struct{}

	// Signaling for leader configuration change
	configChanges chan Command

	// Communication with the connection manager
	newConnChan chan NodeID // add new connections
	delConnChan chan NodeID // delete old connections

	// Communication with commit handler goroutine
	replicationAckChan chan ReplicationAck
	commitChan         chan int // send index to commit
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
	cm.mu.Unlock() // Lock is released before channel utilization because those already use an internal mutex to handle concurrency

	cm.cliCmdRequests <- cmd
	response := <-cm.cliCmdResponses
	// TODO: add leader logic

	return response
}

// receives commands from clients and starts the replication process
// - starts the replicator workers
// - when new logs available to replicate, signals workers to wake
func (cm *ConsensusModule) replicationManager() {
	for id, ch := range cm.replicatorChannels {
		go replicatorWorker(id, ch)
		// TODO: init channels !!
	}

	for {
		cmd := <-cm.cliCmdRequests
		cm.mu.Lock()
		cm.currentIdx++
		entry := logEntry{
			idx:  cm.currentIdx,
			term: cm.currentTerm,
			cmd:  Command(cmd),
		}

		if cmd[0] != "CC" {
			cm.log = append(cm.log, entry)

			for _, ch := range cm.replicatorChannels {
				select {
				case ch <- struct{}{}:
				default:
				}
			}

		} else {
			go cm.applyLeaderConfigCange(cmd)
		}

		cm.mu.Unlock()
	}
}

// replicates all the available log entries to the target node
// - when no further logs available for replication, goes to sleep
// - when woken probes for new logs and eventually starts the process
func (cm *ConsensusModule) replicatorWorker(node NodeID, newLogsAvailable chan struct{}, votingMember bool) {
	hostIdx := cm.currentIdx
	heartbeatTimer := time.NewTimer(HEARTBEAT_DELAY)

	for {
		heartbeatTimer.Reset(HEARTBEAT_DELAY)
		replicationIdx := hostCommitIdx + 1
		result := true // make request
		if result {
			hostIdx = replicationIdx
			cm.replicationAckChan <- RepicationAck{
				id:  node,
				idx: replicationIdx,
			}
		} else {
			hostIdx - 1
		}

		if hostIdx == cm.currentIdx {
			if !votingMember {
				votingMember = true
				cm.nonVotingMmbersChan <- node
			}
			select {
			case <-newLogsAvailable:
			case <-heartbeatTimer.C:
				// send heartbeat

			}

		}
	}
}

func (cm *ConsensusModule) consensusTrackerLoop() {
	replicationLedger := map[int][]int{} // map[log idx] ack nr

	for {
		// Unlocks when an entry is replicated successfuly to one node
		ack := <-cm.replicationAckChan

		if _, ok := replicationLedger[ack.idx]; !ok {
			// if the entry has never been replicated, initialize
			replicationLedger[ack.idx] = []int{0, 0}
			if !cm.isIntermediateConfig || sliceContains(cm.oldConfig, ack.id) {
				replicationLedger[ack.idx][0] = 1
			}

			if cm.isIntermediateConfig && sliceContains(cm.newConfig, ack.id) {
				replicationLedger[ack.idx][1] = 1
			}
		} else {
			// update it otherwise
			replicationLedger[ack.idx] = replicationLedger[ack.idx] + 1
		}

		// if quota reached for the first time; cm.commitChan <- ack.idx
		if true {
			if isintermediateConfig && cm.log[ack.idx].cmd[0] == "CC" {
				// start phase 2
				go applyLeaderConfigChangePhase2()
			}
		}

	}
}

func (cm *ConsensusModule) commitHandler() {
	// TODO:
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
		// register term change and fallback to Follower
		cm.currentTerm = reqTerm
		cm.nodeStatus = FOLLOWER
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

// Perform consistency ccheck for safely appending logs and return the boolean result
func (cm *ConsensusModule) ConsistencyCheck(ccIdx, ccTerm int) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

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

func (cm *ConsensusModule) applyLeaderConfigCange(cfg Configuration) {
	cm.nonVotingNodes = make([]NodeID, 0, 0)
	for _, newNode := range cfg[1:] {
		found := false
		for currNode := range cm.clusterConfiguration {
			found = found || currNode == newNode
		}

		// if node is a new node for the cluster
		if !found {
			cm.newConnChan <- NodeID(newNode)                      // init new connection to node
			cm.nonVotingNodes = append(cm.nonVotingNodes, newNode) // add node to the "non voting" filter

			cm.replicatorChannels[newNode] = make(chan struct{}) // prepare infrastructure for replicator worker

			// init related replicatio worker
			go cm.replicatorWorker(newNode, cm.replicatorChannels[newNode], false)
		}
	}

	// wait for all nodes to get upt to commit level and gain voting privileges
	for range cm.nonVotingNodes {
		<-cm.nonVotingMembersChan
	}
	cm.nonVotingNodes = []NodeID{}

	// append intermiediate config to log and start replciation
	cm.currentIdx++
	entry := logEntry{
		idx:  cm.currentIdx,
		term: cm.currentTerm,
		cmd:  Command{"CC", "IC", cfg[1:]},
	}
	cm.log = append(cm.log, entry)

	// wait for intermediate config to be committed
	<-cm.configChanges // committed CC
	// INFO: deprecated
}

func (cm *ConsensusModule) applyLeaderConfigChangePhase2() {
	cm.isIntermediateConfig = false

	// append new config to log and start replication
	cm.currentIdx++
	entry := logEntry{
		idx:  cm.currentIdx,
		term: cm.currentTerm,
		cmd:  Command{"CC", "NC", cfg[1:]},
	}
	cm.log = append(cm.log, entry)

	// TODO: chang eocnfig cinfastructure to new ocnfig

	// if not leader anymore (not in new config)
	cm.nodeStatus = FOLLOWER
	// destroy all replicators
	// and all leader loops

	// else if sitll leader (in new config)
	// destroy replicators for old nodes
}

/*
 * Election Logic
 */

func (cm *ConsensusModule) startElection() {
	// TODO: implement eletion logic

	cm.nodeStatus = CANDIDATE

	ch := make(chan bool)
	for range cm.clusterConfiguration {
		go func(ch chan bool) {
			res := true // request vote rpc
			ch <- res
		}(ch)
	}

	quorum := len(cm.configChanges)/2 + 1
	for i := 0; i < quorum; i++ {
	}
	won := false
	winPossible := true
	replyCounter := 0
	voteCounter := 0
	for !won || winPossible {
		res := <-ch
		replyCounter++
		if res {
			voteCounter++
		}

		won = voteCounter >= quorum
		winPossible = (replyCounter - voteCounter) >= quorum
	}

	if won {
		cm.nodeStatus = LEADER

		// TODO: init leader related stuff
		// send heartbeats, send noop
	} else {
		cm.nodeStatus = FOLLOWER
	}
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

// Filters map kes in the filter array
func filterIn[K comparable, V any](m map[K]V, keys []K) (res map[K]V) {
	for k, v := range m {
		if sliceContains(keys, k) {
			res[k] = v
		}
	}

	return res
}

// Filters map keys outside the filter key
func filterOut[K comparable, V any](m map[K]V, keys []K) (res map[K]V) {
	for k, v := range m {
		if !sliceContains(keys, k) {
			res[k] = v
		}
	}

	return res
}

// Tells if the map contains the given key
func mapContains[K comparable, V any](m map[K]V, key K) bool {
	_, ok := m[key]

	return ok
}

// Tells if the slice contains the given element
func sliceContains[T comparable](slice []T, element T) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}

	return false
}

// Returns a slice without the given element (if there is)
func sliceDelete[T comparable](slice []T, element T) []T {
	for i, v := range slice {
		if v == element {
			return append(slice[:i], slice[i+1:]...)
		}
	}

	return slice
}
