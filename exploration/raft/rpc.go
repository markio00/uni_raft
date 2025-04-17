package raft

import (
	"log"
	"net"
	"time"
)

func (cm *ConsensusModule) startRpcServer() {
	l, err := net.Listen("tcp", RAFT_PORT)
	if err != nil {
		panic(err.Error())
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
	cm.electiontimer = time.AfterFunc(d, cm.startElection)

	for {
		// TODO: either move elecTimer.Reset in RPC calls or handle rpc.ServeConn(l.Listen ... ) manually
		// WARN: srv.Accept runs a for loop within itself
		cm.rpcSrv.Accept(l)
		cm.resetElectionTimer()
		log.Println("Accepted new connection")
	}
}

type rpcResponseState int

const (
	RPC_CANT_VOTE rpcResponseState = iota
	RPC_VOTED_ALREADY
	RPC_CC_FAIL
	RPC_VOTE_GRANTED
	RPC_TERM_OUTDATED
)

type RpcObject struct {
	cm *ConsensusModule
}

type RPCResponse struct {
	state rpcResponseState
}

type AppendEntriesRPCArgs struct {
	term int

	cc_term int
	cc_idx  int

	entries []LogEntry

	leader_commit_idx int
}

func (o *RpcObject) AppendEntriesRPC(args AppendEntriesRPCArgs, response *RPCResponse) error {
	if !o.cm.termCheck(args.term) {
		response.state = RPC_TERM_OUTDATED
		return nil
	}

	// INFO: is Heartbeat?
	if len(args.entries) == 0 {
		// TODO: respond 'HB received' to caller
		return nil
	}

	if !o.cm.ConsistencyCheck(args.cc_term, args.cc_idx) {
		response.state = RPC_CC_FAIL
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

func (o *RpcObject) RequestVoteRPC(args RequestVoteRPCArgs, response *RPCResponse) error {
	// TODO: if minimum election timeout not reached, ingnore
	// INFO: because comes from stray node waiting shutdown, not in the cluster anymore

	if !o.cm.termCheck(int(args.cc_term)) {
		response.state = RPC_TERM_OUTDATED
		return nil
	}

	if !o.cm.canIVote() {
		response.state = RPC_CANT_VOTE
		return nil
	}

	if o.cm.votedFor != "" {
		response.state = RPC_VOTED_ALREADY
		return nil
	}

	// FIXME: C check is for append, RV_RPC grants vote if candidate has higher term  on last entry or same term and higher id on last entry than local (raft paper $ 5.4.1)
	if !o.cm.ConsistencyCheck(int(args.cc_term), int(args.cc_idx)) {
		// FIXME: not a consistency check (raft paper $ 5.4.1)
		response.state = RPC_CC_FAIL
	}

	// Vote
	o.cm.votedFor = args.caller

	response.state = RPC_VOTE_GRANTED

	return nil
}
