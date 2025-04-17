package raft

import (
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

const (
	RAFT_PORT = ":8530"
	MY_ID     = "xxx.xxx.xxx.xxx"

	ELEC_TIMER_MIN = 1000
	ELEC_TIMER_MAX = 3000

	HEARTBEAT_TIMER = 15

	APPEND_ENTRIES_MAX_LOG_SLICE_LENGTH = 5
)

type ConsensusModule struct {
	// INFO: defined in server scope
	state_update_callback func(operation []string) error

	status ServerStatus

	rpcSrv     rpc.Server
	rpcClients map[string]*rpc.Client

	votedFor string // TODO: Should be an option type (nodes may not have yet voted)

	currTerm uint
	currIdx  uint

	commitIdx int // INFO: Index of highest log entry known to be committed

	electiontimer *time.Timer

	log []LogEntry

	commitQueue  chan struct{}
	commitResult chan error

	// INFO: each replicator sends info on this chan to signal entries replication
	// TODO: add flag (uint8) enum
	ackChan chan struct {
		idx  uint  // replicated index
		flag uint8 // majority flag {0:old, 1:both, 2:new)}
	}
}

func NewConsensusModule(callback func(op []string) error, config []string) *ConsensusModule {
	// TODO: proper initialization of CM
	cm := ConsensusModule{}
	cm.state_update_callback = callback

	return &cm
}

func (cm *ConsensusModule) canIVote() bool {
	// TODO: determine if can vote
	// INFO: can not vote if parat of new config but still syncing log

	// WARN: refactor
	return true
}

func (cm *ConsensusModule) ConsistencyCheck(cc_term, cc_idx int) bool {
	// TODO: perform log concsistency check
	// INFO: leader sends log UUID (term, idx). follower check if exist

	// WARN: refactor
	return true
}

func (cm *ConsensusModule) termCheck(term int) bool {
	// TODO: cm.term:  > term reject, if = term ok, if > term fall back to follower
	// INFO: handle term difference

	// WARN: refactor
	return true
}

func (cm *ConsensusModule) appendLogEntries(cc_idx int, entries []LogEntry) {
	cm.log = append(cm.log[:cc_idx+1], entries...)

	// INFO: if received CC, apply it rightaway
	for _, entry := range entries {
		if entry.command[0] == "CC" {
			cm.apply_CC(entry.command[1:])
		}
	}
}

func (cm *ConsensusModule) applyToState(leader_commit_idx int) {
	target_commit_idx := min(leader_commit_idx, len(cm.log)-1)

	for _, entry := range cm.log[cm.commitIdx+1 : target_commit_idx+1] {
		if entry.command[0] != "CC" {
			ok, err := cm.state_update_callback(entry.command)
			if !ok || err != nil {
				// TODO: handle state application error
				panic("logic fault or hw error occurred")
			}
		}
	}
}

func (cm *ConsensusModule) resetElectionTimer() {
	// TODO: reset election timeout when received RPC
}

func (cm *ConsensusModule) apply_CC(cfg []string) {
	// TODO: change running config and apply
}

type LogEntry struct {
	term int
	idx  int

	command []string
}

type ServerStatus int

const (
	STATUS_FOLLOWER ServerStatus = iota
	STATUS_CANDIDATE
	STATUS_LEADER
)

func getRandTimer(min, max int) (time.Duration, error) {
	randTime := rand.IntN(max-min) + min

	d, err := time.ParseDuration(strconv.Itoa(randTime) + "ms")
	if err != nil {
		return 0, err // FIXME: Shouldn't panic?
	}

	return d, nil
}

func (cm *ConsensusModule) startElection() {
	// TODO: add election timer check

	d, err := getRandTimer(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	if err != nil {
		panic("timer setup fail")
	}
	cm.electiontimer.Reset(d)

	cm.status = STATUS_CANDIDATE
	cm.currTerm++

	cm.votedFor = MY_ID

	elecCounter := 1
	respCounter := 1

	resultChan := make(chan bool, 0)

	for k := range cm.rpcClients {
		go func(resChan chan<- bool) {
			args := RequestVoteRPCArgs{}
			resp := RPCResponse{}
			cm.rpcClients[k].Call("RpcObject.RequestVoteRPC", args, &resp)

			// WARN: we explicitely decided not to implement a different retry timeout (from the election timer)

			resChan <- resp.state == RPC_VOTE_GRANTED
		}(resultChan)
	}

	quota := (((len(cm.rpcClients) + 1) / 2) + 1)
	// while quota(1/2 +1) not met
	for elecCounter < quota && (respCounter-elecCounter) <= quota {
		elecRes := <-resultChan
		respCounter++
		if elecRes {
			elecCounter++
		}
	}

	// if election fail
	if elecCounter < quota {
		cm.status = STATUS_FOLLOWER
		return
	}

	go cm.startLeaderCycle()
}

func (cm *ConsensusModule) startLeaderCycle() {
	cm.status = STATUS_LEADER

	// TODO: heartbeats

	noOpLogEntry := LogEntry{
		term: int(cm.currTerm),
		idx:  int(cm.currIdx),

		command: []string{},
	}
}

func (cm *ConsensusModule) CommitEntry(cmd []string) error {
	cm.replicateCommand(cmd)

	cm.commitQueue <- struct{}{}
	err := <-cm.commitResult
	if err != nil {
		return err
	}

	return nil
}

func (cm *ConsensusModule) replicateCommand(cmd []string) {
	cm.currIdx++

	cm.log = append(cm.log, LogEntry{
		term:    int(cm.currTerm),
		idx:     int(cm.currIdx),
		command: cmd,
	})
}

func (cm *ConsensusModule) startHeartbeatCycle() {
	d := HEARTBEAT_TIMER * time.Millisecond
	hbTimer := time.NewTimer(d)

	for {
		for _, client := range cm.rpcClients {
			go func() {
				client.Call("RpcObject.AppendEntriesRPC", AppendEntriesRPCArgs{}, &RPCResponse{})
			}()
		}

		<-hbTimer.C
	}
}

func (cm *ConsensusModule) startreplicationCycle() {
	for _, client := range cm.rpcClients {
		go cm.followerReplicator(client)
	}
}

func (cm *ConsensusModule) followerReplicator(cl *rpc.Client, newNode bool, ackChan chan<- uint) {
	// TODO: extend "nil idx" comcept to whole program
	followerCommitIdx := new(uint)
	*followerCommitIdx = uint(cm.commitIdx)
	followerIdx := uint(0)

	for true {
		// TODO: should probably be a locking channel
		if canReplicate() {
			args := &AppendEntriesRPCArgs{
				term:    int(cm.currTerm),
				cc_term: cm.log[followerIdx].term,
				cc_idx:  int(followerIdx),
				// TODO: allow entries bundle, fix appendEntriesRPC entries possible out of bounds
				entries: []LogEntry{cm.log[int(followerIdx+1)]}, // : followerIdx+1+APPEND_ENTRIES_MAX_LOG_SLICE_LENGTH],

				leader_commit_idx: cm.commitIdx,
			}

			ret := RPCResponse{}
			err := cl.Call("RpcObject.AppendEntrieRPC", args, &ret)
			if err != nil {
				// FIXME: communicate client ID
				// TODO: should probably retry
				panic("couldn't call client RPC")
			}

			if ret.state == RV_RPC_C_CHECK_FAIL {
				*followerCommitIdx--
			} else {
				// TODO: use buffer chan
				ackChan <- followerIdx + 1

				if !partOfBothCfgs {
					ackChan <- []uint{followerIdx + 1}
				}
			}
		}
	}
}

func (cm *ConsensusModule) ConsensusTrackerLoop() {
	confA := map[uint]uint{}
	confB := map[uint]uint{}

	for true {
		data := <-cm.ackChan

		idx := data.idx

		// create ID entry
		if _, ok := confA[idx]; !ok {
			confA[idx] = 0
			confB[idx] = 0
		}

		// increment count for ID
		switch data.flag {
		case 0:
			confA[idx]++
		case 1:
			confA[idx]++
			confB[idx]++
		case 2:
			confB[idx]++

		}

		// check for majority replication
		if confA[idx] >= cm.commitQuorum && uint(cm.commitIdx) < idx && (!intermediateConf() || confB[idx] >= cm.CommitQuorumB) {
			cm.commitIdx = idx

			// TODO: apply state change
			err := cm.state_update_callback(cm.log[idx].command)

			<-cm.commitQueue

			cm.commitResult <- err
		}
	}
}
