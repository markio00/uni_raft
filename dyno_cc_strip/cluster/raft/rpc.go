package raft

import (
	"log"
	"net"
	"net/rpc"
)

type CMInnerInterface interface {
	// Shared APIs
	HandleTerm(reqTerm int, leaderID string) bool
	ResetElectionTimer()

	// AppendEntries APIs
	ConsistencyCheck(ccIdx, ccTerm int) bool
	AppendEntry(entry LogEntry)
	SyncCommitIdx(leaderCommitIdx int)

	// RequestVote APIs
	CanVoteFor(checkIdx, checkTerm int, id NodeID) bool
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
func (cm *ConsensusModule) startRpcServer(done chan struct{}) {

	log.Println("startRpcServer: invoked")
	rpcObj := new(RpcObject)
	rpcObj.cm = cm
	rpc.Register(rpcObj)
	l, err := net.Listen("tcp", RPC_PORT)
	if err != nil {
		log.Fatal("startRpcServer: RPC listen error: '", err, "'")
	}

	done <- struct{}{}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("RPC server: accept error: '", err, "'")
		}
		log.Println("RPC server: received request")

		go rpc.ServeConn(conn)
	}

}

type AppendEntriesArgs struct {
	LeaderID  string
	CommitIdx int
	Term      int
	CcIdx     int
	CcTerm    int
	Entry     *LogEntry
}
type AppendEntriesResponse struct {
	CcPass bool
}

func (obj *RpcObject) AppendEntriesRPC(args AppendEntriesArgs, resp *AppendEntriesResponse) error {

	log.Printf("AppendEntriesRPC from %s\n", args.LeaderID)

	obj.cm.Lock()
	defer obj.cm.Unlock()

	obj.cm.ResetElectionTimer()

	if !obj.cm.HandleTerm(args.Term, args.LeaderID) {
		log.Printf("AppendEntriesRPC from %s: refused after HandleTerm\n", args.LeaderID)
		return nil
	}

	// if heartbeat, lazy && operator avoids usless and fatal check
	if args.Entry != nil && !obj.cm.ConsistencyCheck(args.CcIdx, args.CcTerm) {
		log.Printf("AppendEntriesRPC from %s: refused after ConsistencyCheck\n", args.LeaderID)
		resp.CcPass = false
		return nil
	}

	// handle payload
	// - if empty => heartbeat => ignore
	// - apply entries otherwise
	if entry := args.Entry; entry != nil {
		obj.cm.AppendEntry(*entry)
	}

	// update local commit index with the leader provided one
	obj.cm.SyncCommitIdx(args.CommitIdx)

	obj.cm.SyncToLeaderFileSystem(args.CommitIdx) // PERF: this should be async., carried on by worker threads
	                                              // FIX:  this should be async., carried on by worker threads

	log.Printf("AppendEntriesRPC from %s: accepted\n", args.LeaderID)

	return nil
}

type RequestVoteArgs struct {
	CandidateID string
	Term        int
	CheckIdx    int
	CheckTerm   int
}
type RequestVoteResponse struct {
	VoteGranted bool
}

func (obj *RpcObject) RequestVoteRPC(args RequestVoteArgs, resp *RequestVoteResponse) error {

	log.Printf("RequestVoteRPC from '%s'\n", args.CandidateID)

	obj.cm.Lock()
	defer obj.cm.Unlock()

	obj.cm.ResetElectionTimer()

	// INFO: Resetting leader id at election start (only at first vote)
	if !obj.cm.HandleTerm(args.Term, "") {
		resp.VoteGranted = false
		log.Printf("RequestVoteRPC from '%s': discarded after HandleTerm\n", args.CandidateID)
		return nil
	}

	if !obj.cm.CanVoteFor(args.CheckIdx, args.CheckTerm, args.CandidateID) {
		log.Printf("RequestVoteRPC from %s: denied after CanVoteFor\n", args.CandidateID)
		resp.VoteGranted = false
		return nil
	}

	resp.VoteGranted = obj.cm.VoteFor(args.CandidateID)

	var deniedOrGranted string
	if resp.VoteGranted {
		deniedOrGranted = "granted"
	} else {
		deniedOrGranted = "denied"
	}
	log.Printf("RequestVoteRPC from %s: vote '%s'\n", args.CandidateID, deniedOrGranted)

	return nil
}
