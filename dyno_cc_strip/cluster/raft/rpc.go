package raft

import (
	"log"
	"net"
	"net/rpc"
)

type CMInnerInterface interface {
	// Shared APIs
	HandleTerm(reqTerm int, leaderID string, isFromRequestForVote bool) bool
	ResetElectionTimer()

	// AppendEntries APIs
	ConsistencyCheck(ccIdx, ccTerm int) bool
	AppendEntry(entry logEntry)
	SyncCommitIdx(leaderCommitIdx int)

	// RequestVote APIs
	CanVoteFor(checkIdx, checkTerm int) bool
	VoteFor(candidateID string) bool

	// State APIs
	SyncToLeaderFileSystem(targetCommitIdx int)

	// Mutual exclusion related stuff
	Lock()
	Unlock()
}

type RpcObject struct {
	cm CMInnerInterface
}

// Starts the rpc server for the current node
func (cm *ConsensusModule) startRpcServer() {

	rpcObj := new(RpcObject)
	rpc.Register(rpcObj)
	l, err := net.Listen("tcp", ":1234")

	if err != nil {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}
		go rpc.ServeConn(conn)
	}
	log.Fatal("listen error:", err)
}

type AppendEntriesArgs struct {
	leaderID  string
	commitIdx int
	term      int
	ccIdx     int
	ccTerm    int
	entry     *logEntry
}
type AppendEntriesResponse struct {
	ccPass bool
}

func (obj *RpcObject) AppendEntriesRPC(args AppendEntriesArgs, resp *AppendEntriesResponse) error {

	log.Printf("Received AppendEntriesRPC from %s\n", args.leaderID)

	obj.cm.Lock()
	defer obj.cm.Unlock()

	obj.cm.ResetElectionTimer()

	if !obj.cm.HandleTerm(args.term, args.leaderID, false) {
		log.Printf("Refused AppendEntriesRPC from %s due to term %d\n", args.leaderID, args.term)
		return nil
	}

	if !obj.cm.ConsistencyCheck(args.ccIdx, args.ccTerm) {
		log.Printf("Refused AppendEntriesRPC from %s due to consistency check failure\n", args.leaderID)
		resp.ccPass = false
		return nil
	}

	// handle payload
	// - if empty => heartbeat => ignore
	// - apply entries otherwise
	if entry := args.entry; entry != nil {
		obj.cm.AppendEntry(*entry)
	}

	// update local commit index with the leader provided one
	obj.cm.SyncCommitIdx(args.commitIdx)

	obj.cm.SyncToLeaderFileSystem(args.commitIdx) // PERF: this should be async., carried on by worker threads
																								// FIX:  this should be async., carried on by worker threads

	log.Printf("AppendEntriesRPC from %s accepted (commit synced)\n", args.leaderID)

	return nil
}

type RequestVoteArgs struct {
	candidateID string
	term        int
	checkIdx    int
	checkTerm   int
}
type RequestVoteResponse struct {
	voteGranted bool
}

func (obj *RpcObject) RequestVoteRPC(args RequestVoteArgs, resp *RequestVoteResponse) error {

	log.Printf("Received RequestVoteRPC from %s\n", args.candidateID)

	obj.cm.Lock()
	defer obj.cm.Unlock()

	obj.cm.ResetElectionTimer()

	// INFO: Resetting leader id at election start (only at first vote)
	if !obj.cm.HandleTerm(args.term, "", true) {
		log.Printf("Refused RequestVoteRPC from %s due to term %d\n", args.candidateID, args.term)
		return nil
	}

	if !obj.cm.CanVoteFor(args.checkIdx, args.checkTerm) {
		log.Printf("Refused RequestVoteRPC from %s due to log discrepancy\n", args.candidateID)
		resp.voteGranted = false
		return nil
	}

	resp.voteGranted = obj.cm.VoteFor(args.candidateID)

	log.Printf("Accepted RequestVoteRPC from %s (vote granted)\n", args.candidateID)

	return nil
}

