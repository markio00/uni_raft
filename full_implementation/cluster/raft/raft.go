package raft

import (
	"context"
	"errors"
	"math/rand"
	"net/rpc"
	"slices"
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

type queue struct{}

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
	lastConfigChangeIdx  int

	// Election related fields
	electionTimer    time.Timer
	lastRpcTimestamp time.Time
	votedFor         NodeID

	// Communication with action handler goroutine
	signalNewEntryToReplicate chan struct{}
	replicatorChannels        map[NodeID]chan struct{}
	commitSignalingChans      map[int]chan error

	// Signaling for leader configuration change
	configChanges chan Command

	// Communication with the connection manager
	newConnChan chan NodeID // add new connections
	delConnChan chan NodeID // delete old connections

	// Communication with commit handler goroutine
	replicationAckChan   chan ReplicationAck
	commitChan           chan int // send index to commit
	newVotingMembersChan chan NodeID

	requestQueue queue

	// Signaling for replication related threads
	leaderCtx       context.Context    // used by threads to receive cancel signals
	leaderCtxCancel context.CancelFunc // the function sending those signals
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

	// Initialise new connections
	for _, node := range cm.newConfig {
		cm.newConnChan <- node
	}
	cm.newConfig = []NodeID{}

	// Start election timer
	cm.electionTimer = *time.AfterFunc(getRandomDuration(ELEC_TIMER_MIN, ELEC_TIMER_MAX), cm.startElection)

	// TODO: start all handlers
}

// Aplies the given command to the distributed cluster
// The call blocks until the command is committed to the cluster returning a nil
// If the current node is not the leader, an error redirectng to the correct leader will be returned
// In exceptional circumstances when committing fails, an explainative error will be returned instead

func (cm *ConsensusModule) ApplyCommand(cmd Command) error {
	if cm.nodeStatus != LEADER {
		// If the node is not the leader, send an error specifying the correct leader
		// FIX: should be discrete error type
		return errors.New("Not the leader, contact " + string(cm.votedFor))
	}

	// if handling config change, take necessary  preparation steps
	if cmd[0] == "CC" {
		cmd = cm.prepareLeaderConfigChange(cmd)
	}

	// append and start replication of the configuration entry
	return cm.appendAndReplicate(cmd)
}

// TODO: when configuration change happens (both leader and follower), update the cc entry idx

func (cm *ConsensusModule) appendAndReplicate(cmd Command) error { // TODO: Send heartbeat for read-only requests

	// create and append log entry, return related idx
	idx := cm.appendNewLogEntry(cmd)

	// create and store a commit signaling channel
	ch := make(chan error)
	cm.commitSignalingChans[idx] = ch

	// signal new entry available to replicator
	cm.signalNewEntryToReplicate <- struct{}{}

	// wait for commit signal on dedicated channel, then deallocate
	response := <-ch
	close(ch)
	delete(cm.commitSignalingChans, idx)

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
	cm.leaderCtxCancel()
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

// Perform consistency ccheck for safely appending logs and return the boolean result
func (cm *ConsensusModule) ConsistencyCheck(ccIdx, ccTerm int) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	lastEntry := cm.log[len(cm.log)-1]

	// FIX: delete entries if no match ??

	return lastEntry.idx == ccIdx && lastEntry.term == ccTerm
}

func (cm *ConsensusModule) appendToLog(entry logEntry) {
	cm.log = append(cm.log, entry)

	if entry.cmd[0] == "CC" {
		cm.lastConfigChangeIdx = cm.currentIdx
	}
}

// Append already consistent entry to the local log and apply configuration change if one is detected
func (cm *ConsensusModule) AppendEntry(entry logEntry) {
	if entry.cmd[0] == "CC" {
		// if config change detected, start the application process
		cm.applyFollowerConfigChange(Configuration(entry.cmd[1:]))
	}

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
	cm.nodeStatus = CANDIDATE

	ch := make(chan ElectionReply)

	reqArgs := RequestVoteArgs{
		candidateID: SRV_ID,
		term:        cm.currentTerm,
		checkIdx:    cm.log[len(cm.log)-1].idx,
		checkTerm:   cm.log[len(cm.log)-1].term,
	}

	for id := range filterOut(cm.clusterConfiguration, cm.nonVotingNodes) {
		go func(ch chan ElectionReply) {
			res := cm.sendRequestVoteRPC(id, reqArgs)
			ch <- ElectionReply{voteGranted: res.voteGranted, id: id}
		}(ch)
	}

	canWin := true
	positiveAnswers := 0
	positiveAnswersOld := 0
	positiveAnswersNew := 0
	isQuorumReached := false
	negativeAnswers := 0
	negativeAnswersOld := 0
	negativeAnswersNew := 0

	for !isQuorumReached && canWin {
		select {
		case electionReply := <-ch:
			if !cm.isIntermediateConfig {
				if electionReply.voteGranted {
					positiveAnswers++
				} else {
					negativeAnswers++
				}

				quorumOld, _ := cm.getQuorums()
				canWin = negativeAnswers < quorumOld
				isQuorumReached = positiveAnswers >= quorumOld
			} else {

				if slices.Contains(cm.oldConfig, electionReply.id) {
					if electionReply.voteGranted {
						positiveAnswersOld++
					} else {
						negativeAnswersOld++
					}
				}

				if slices.Contains(cm.newConfig, electionReply.id) {
					if electionReply.voteGranted {
						positiveAnswersNew++
					} else {
						negativeAnswersNew++
					}
				}

				quorumOld, quorumNew := cm.getQuorums()
				canWin = negativeAnswersOld < quorumOld && negativeAnswersNew < quorumNew
				isQuorumReached = positiveAnswersOld >= quorumOld && positiveAnswersNew >= quorumNew
			}
		default:
		}
	}

	if isQuorumReached {
		cm.nodeStatus = LEADER

		go cm.replicationManager()

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
	res = map[K]V{}
	for k, v := range m {
		if slices.Contains(keys, k) {
			res[k] = v
		}
	}

	return res
}

// Filters slice values in the filter array
func sliceFilterIn[T comparable](slice []T, filter []T) (res []T) {
	for _, v := range slice {
		if slices.Contains(filter, v) {
			res = append(res, v)
		}
	}

	return res
}

func sliceFilterOut[T comparable](slice []T, filter []T) (res []T) {
	for _, v := range slice {
		if !slices.Contains(filter, v) {
			res = append(res, v)
		}
	}

	return res
}

// Filters map keys outside the filter key
func filterOut[K comparable, V any](m map[K]V, keys []K) (res map[K]V) {
	res = map[K]V{}
	for k, v := range m {
		if !slices.Contains(keys, k) {
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

// Returns a slice without the given element (if there is)
func sliceDelete[T comparable](slice []T, element T) []T {
	for i, v := range slice {
		if v == element {
			return append(slice[:i], slice[i+1:]...)
		}
	}

	return slice
}

func (cm *ConsensusModule) getQuorums() (int, int) {
	if !cm.isIntermediateConfig {
		quorum := len(filterOut(cm.clusterConfiguration, cm.nonVotingNodes))/2 + 1
		return quorum, -1
	} else {
		quorumOld := len(cm.oldConfig)/2 + 1
		quorumNew := len(cm.newConfig)/2 + 1
		return quorumOld, quorumNew
	}
}
