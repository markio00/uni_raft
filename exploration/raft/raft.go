package raft

import (
	"math/rand/v2"
	"net/rpc"
	"strconv"
	"time"
)

func assert(cond bool, msg string) {
	if !cond {
		panic("Assertion " + msg + " failed")
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

	rpcSrv *rpc.Server

	clients map[string]configType // INFO: All nodes in the cluster except me

	votedFor string

	currTerm int
	currIdx  int

	commitIdx     int         // INFO: Index of highest log entry known to be committed
	electiontimer *time.Timer // INFO: timer init in 'startRpcSrv'

	log []LogEntry

	commitQueue  chan struct{}
	commitResult chan error

	// INFO: each replicator sends info on this chan to signal entries replication
	ackChan chan replicationAck

	idleFlag bool
}

type replicationAck struct {
	idx  int        // replicated index
	flag configType // majority flag (which majority to account the ack for)
}

type configType int

const (
	CONF_OLD configType = iota
	CONF_INTERMEDIATE
	CONF_NEW
)

func (cm *ConsensusModule) sendRPC(ip string, rpcType string, args any, resp any) error {
	// PERF: Using this function implies having a distinct client for each sent RPC
	client, err := rpc.Dial("tcp", ip+RAFT_PORT)
	if err != nil {
		return err
	}
	defer client.Close()
	// INFO: resp is already sent as a pointer by whoever calls `SendRPC`
	err = client.Call(rpcType, args, resp)
	if err != nil {
		return err
	}
	return nil
}

func NewConsensusModule(callback func(op []string) error, config []string) *ConsensusModule {
	cm := ConsensusModule{}
	cm.state_update_callback = callback

	cm.status = STATUS_FOLLOWER

	cm.rpcSrv = rpc.NewServer()

	for _, ip := range config {
		cm.clients[ip] = CONF_NEW
	}

	cm.votedFor = ""

	cm.currTerm = 0
	cm.currIdx = -1

	cm.commitIdx = -1

	cm.log = []LogEntry{}

	cm.commitQueue = make(chan struct{})
	cm.commitResult = make(chan error)

	cm.ackChan = make(chan replicationAck)

	cm.idleFlag = true

	return &cm
}

func (cm *ConsensusModule) ConsistencyCheck(cc_term, cc_idx int) bool {
	// INFO: check entry UUID (term, idx) exists in the log
	return cc_idx < len(cm.log) && cm.log[cc_idx].term == cc_term
}

func (cm *ConsensusModule) termCheck(term int) bool {
	// INFO: handle term difference

	if term > cm.currTerm {
		cm.currTerm = term
		cm.status = STATUS_FOLLOWER
	}

	return term >= cm.currTerm
}

func (cm *ConsensusModule) appendLogEntries(cc_idx int, entries []LogEntry) {
	cm.log = append(cm.log[:cc_idx+1], entries...)

	// INFO: if received CC, apply it rightaway
	for _, entry := range entries {
		if entry.command[0] == "CC" && !cm.idleFlag {
			cm.apply_CC(entry.command[1:])
		}
	}
}

func (cm *ConsensusModule) applyToState(leader_commit_idx int) error {
	target_commit_idx := min(leader_commit_idx, len(cm.log)-1)
	// INFO: check if replicas behind of leader commit, have to apply whole log

	// WARN: when used from leader, log length will always be ahead of commit level
	// WARN: return type only useful to leader (?)

	for _, entry := range cm.log[cm.commitIdx+1 : target_commit_idx+1] {
		if entry.command[0] != "CC" {
			err := cm.state_update_callback(entry.command)
			if err != nil {
				panic("HW error occurred: " + err.Error())
			}
		}
	}

	return nil
}

func (cm *ConsensusModule) resetElectionTimer() {
	d := getRandTimer(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	cm.electiontimer.Reset(d)
}

func (cm *ConsensusModule) apply_CC(cfg []string) {
	// TODO: change running config and apply

	if cm.status == STATUS_FOLLOWER {
		for ip := range cm.clients {
			cm.clients[ip] = CONF_OLD
		}
		for _, ip := range cfg {
			_, ok := cm.clients[ip]
			if ok {
				cm.clients[ip] = CONF_INTERMEDIATE
			} else {
				cm.clients[ip] = CONF_NEW
			}
		}
	} else if cm.status == STATUS_LEADER {
		// TODO: Bring new clients to commit level
	}
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

func getRandTimer(min, max int) time.Duration {
	randTime := rand.IntN(max-min) + min

	d, err := time.ParseDuration(strconv.Itoa(randTime) + "ms")
	if err != nil {
		panic("timer setup fail")
	}

	return d
}

func (cm *ConsensusModule) startElection() {
	// TODO: add election timer check

	cm.resetElectionTimer()

	cm.status = STATUS_CANDIDATE
	cm.currTerm++

	cm.votedFor = MY_ID

	elecCounter := 1
	respCounter := 1

	resultChan := make(chan bool, 0)

	for ip := range cm.clients {
		go func(resChan chan<- bool) {
			args := RequestVoteRPCArgs{}
			resp := RPCResponse{}
			cm.sendRPC(ip, "RpcObject.RequestVoteRPC", args, &resp)

			// WARN: we explicitely decided not to implement a different retry timeout (from the election timer)

			resChan <- resp.state == RPC_VOTE_GRANTED
		}(resultChan)
	}

	quota := (((len(cm.clients) + 1) / 2) + 1)
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
		term: cm.currTerm,
		idx:  cm.currIdx,

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
		term:    cm.currTerm,
		idx:     cm.currIdx,
		command: cmd,
	})
}

func (cm *ConsensusModule) startHeartbeatCycle() {
	d := HEARTBEAT_TIMER * time.Millisecond
	hbTimer := time.NewTimer(d)

	for {
		for ip := range cm.clients {
			go func() {
				cm.sendRPC(ip, "RpcObject.AppendEntriesRPC", AppendEntriesRPCArgs{}, &RPCResponse{})
			}()
		}

		<-hbTimer.C
	}
}

func (cm *ConsensusModule) startreplicationCycle() {
	for _, client := range cm.clients {
		// TODO: go cm.followerReplicator(...)
	}
}

func (cm *ConsensusModule) followerReplicator(cl string, newNode bool, ackChan chan<- replicationAck) {

	// TODO: wait time for RPC should be bounded
	/* Maybe this behaviour can be achieved by using a `select` over two channels:
	   1. One to receive the RPC result
		 2. The other to receive a signal from a timer
	*/

	followerCommitIdx := cm.commitIdx
	followerIdx := -1

	// INFO: '*uint' too complex & hard to read. Efficiency gains not relevant at this time. Reverted to 'int'

	for true {
		// TODO: should probably be a locking channel
		if areThereNewEntries() {
			args := &AppendEntriesRPCArgs{
				term:    cm.currTerm,
				cc_term: cm.log[followerIdx].term,
				cc_idx:  followerIdx,
				// TODO: allow entries bundle, fix appendEntriesRPC entries possible out of bounds
				entries: []LogEntry{cm.log[followerIdx+1]}, // : followerIdx+1+APPEND_ENTRIES_MAX_LOG_SLICE_LENGTH],

				leader_commit_idx: cm.commitIdx,
			}

			ret := RPCResponse{}
			err := cm.sendRPC(cl, "RpcObject.AppendEntriesRPC", args, &ret)
			if err != nil {
				// TODO: should probably retry
				panic("couldn't call client #" + cl + " RPC")
			}

			if ret.state == RPC_CC_FAIL {
				followerCommitIdx--
			} else {

				msg := replicationAck{
					idx: followerIdx + 1,
					// INFO: delelgated to CM state because it's dynamic and less efficient to propagate the change here
					flag: cm.clients[cl],
				}

				// TODO: use buffered chan
				ackChan <- msg
			}
		}
	}
}

func (cm *ConsensusModule) ConsensusTrackerLoop() {
	confA := map[int]int{}
	confB := map[int]int{}

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
			case CONF_OLD:
				confA[idx]++
			case CONF_INTERMEDIATE:
				confA[idx]++
				confB[idx]++
			case CONF_NEW:
				confB[idx]++
		}

		// check for majority replication
		if confA[idx] >= cm.commitQuorum && cm.commitIdx < idx && (!intermediateConf() || confB[idx] >= cm.CommitQuorumB) {
			cm.commitIdx = idx

			// TODO: apply state change
			err := cm.applyToState(idx)
			<-cm.commitQueue

			cm.commitResult <- err
		}
	}
}
