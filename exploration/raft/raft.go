package raft

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

const (
	RAFT_PORT = ":8530"
	MY_ID     = "xxx.xxx.xxx.xxx"

	ELEC_TIMER_MIN = 1000
	ELEC_TIMER_MAX = 3000

	HEARTBEAT_TIMER = 15
)

type RpcObject struct {
	cm *ConsensusModule
}

type ConsensusModule struct {
	state_update_callback func(operation []string) (bool, error)

	status ServerStatus

	rpcSrv     rpc.Server
	rpcClients map[string]*rpc.Client

	votedFor string

	currTerm uint
	currIdx  uint

	commitIdx int

	electiontimer *time.Timer

	log []LogEntry

	commitQueue  chan<- struct{}
	commitResult <-chan error
}

func NewConsensusModule(callback func(op []string) (bool, error), config []string) *ConsensusModule {

	// TODO: proper initialization of CM
	cm := ConsensusModule{}
	cm.state_update_callback = callback

	return &cm
}

func (cm *ConsensusModule) Start() {

	l, err := net.Listen("tcp", RAFT_PORT)
	if err != nil {
		fmt.Println(err)
	}
	defer l.Close()

	obj := new(RpcObject)
	obj.cm = cm
	cm.rpcSrv.Register(obj)

	log.Println("Listening RPCs on port", RAFT_PORT)

	d, err := getRandTimer(ELEC_TIMER_MIN, ELEC_TIMER_MAX)
	if err != nil {
		panic("timer setup fail")
	}
	cm.electiontimer = time.AfterFunc(d, cm.StartElection)

	for {
		cm.rpcSrv.Accept(l)
		cm.resetElectionTimer()
		log.Println("Accepted new conn")
	}
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
	// TODO: cm.term:  > term reject, if = term ok, if > term fall back to follwer
	// INFO: handle term difference

	// WARN: refactpr
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

type rpcStateResponse int

const (
	RV_RPC_UNABLE_TO_VOTE rpcStateResponse = iota
	RV_RPC_ALREADY_VOTED
	RV_RPC_C_CHECK_FAIL
	RV_RPC_VOTE_GRANTED
	RV_RPC_TERM_TOO_LOW
)

type LogEntry struct {
	term int
	idx  int

	command []string
}

type AppendEntriesRPCArgs struct {
	term int

	cc_term int
	cc_idx  int

	entries []LogEntry

	leader_commit_idx int
}

type AppendEntriesRPCResponse struct {
	state rpcStateResponse
}

func (o *RpcObject) AppendEntriesRPC(args AppendEntriesRPCArgs, response *AppendEntriesRPCResponse) error {

	if !o.cm.termCheck(args.term) {
		response.state = RV_RPC_TERM_TOO_LOW
		return nil
	}

	// INFO: is Heartbeat?
	if len(args.entries) == 0 {
		// TODO: respond 'HB received' to caller
		return nil
	}

	if !o.cm.ConsistencyCheck(args.cc_term, args.cc_idx) {
		response.state = RV_RPC_C_CHECK_FAIL
		return nil
	}

	o.cm.appendLogEntries(args.cc_idx, args.entries)

	go o.cm.applyToState(args.leader_commit_idx)

	return nil
}

type RequestVoteRPCArgs struct {
	caller string

	cc_idx  uint
	cc_term uint
}
type RequestVoteRPCResponse struct {
	state rpcStateResponse
}

func (o *RpcObject) RequestVoteRPC(args RequestVoteRPCArgs, response *RequestVoteRPCResponse) error {

	// TODO: if minimum election timeout not reached, ingnore
	// INFO: because comese from stray node waiting shutdown, not in the cluster anymore

	if !o.cm.termCheck(int(args.cc_term)) {
		response.state = RV_RPC_TERM_TOO_LOW
		return nil
	}

	if !o.cm.canIVote() {
		response.state = RV_RPC_UNABLE_TO_VOTE
		return nil
	}

	if o.cm.votedFor != "" {
		response.state = RV_RPC_ALREADY_VOTED
		return nil
	}

	// FIXME: C check is for append, RV_RPC grants vote if candidate has higher term  on last entry or same term and higher id on last entry than local (raft paper $ 5.4.1)
	if !o.cm.ConsistencyCheck(int(args.cc_term), int(args.cc_idx)) {
		// FIXME: not a consistency check (raft paper $ 5.4.1)
		response.state = RV_RPC_C_CHECK_FAIL
	}

	// Vote
	o.cm.votedFor = args.caller

	response.state = RV_RPC_VOTE_GRANTED

	return nil
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
		return 0, err
	}

	return d, nil
}

func (cm *ConsensusModule) StartElection() {

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

	for k, _ := range cm.rpcClients {

		go func(resChan chan<- bool) {

			args := RequestVoteRPCArgs{}
			resp := RequestVoteRPCResponse{}
			cm.rpcClients[k].Call("RpcObject.RequestVoteRPC", args, &resp)

			// WARN: we explicitely decided not to implement a different retry timeout (from the election timer)

			resChan <- resp.state == RV_RPC_VOTE_GRANTED
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

func (cm *ConsensusModule) startHeartbeadCycle() {

	d := HEARTBEAT_TIMER * time.Millisecond
	hbTimer := time.NewTimer(d)

	for {
		for _, client := range cm.rpcClients {

			go func() {
				client.Call("RpcObject.AppendEntriesRPC", AppendEntriesRPCArgs{}, &AppendEntriesRPCResponse{})
			}()

		}

		<-hbTimer.C
	}
}

func (cm *ConsensusModule) startreplicationCycle() {

	for _, client := range cm.rpcClients {

		go func(cl *rpc.Client) {

		}(client)
	}

}
