package raft

type CMInnerInterface interface {
	// Shared APIs
	HandleTerm(reqTerm int, leaderID string) bool
	ResetElectionTimer()

	// AppendEntries APIs
	ConsistencyCheck(ccIdx, ccTerm int) bool
	AppendEntry(entry logEntry)
	SyncCommitIdx(leaderCommitIdx int)

	// RequestVote APIs
	ValidVoteRequest() bool
	CanVoteFor(checkIdx, checkTerm int) bool
	VoteFor(candidateID string) bool
}

type RpcObject struct {
	cm CMInnerInterface
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
	termTooLow bool
	ccPass     bool
}

func (obj *RpcObject) AppendEntriesRPC(args AppendEntriesArgs, resp *AppendEntriesResponse) error {
	obj.cm.ResetElectionTimer()

	if !obj.cm.HandleTerm(args.term, args.leaderID) {
		resp.termTooLow = true
		return nil
	}

	if !obj.cm.ConsistencyCheck(args.ccIdx, args.ccTerm) {
		resp.ccPass = false
		return nil
	}

	// handle payload
	// - if empty => hartbeat => ignore
	// - apply entries otherwise
	if entry := args.entry; entry != nil {
		obj.cm.AppendEntry(*entry)
	}

	// update local commit index with the leader provided one
	obj.cm.SyncCommitIdx(args.commitIdx)

	return nil
}

type RequestVoteArgs struct {
	candidateID string
	term        int
	checkIdx    int
	checkTerm   int
}
type RequestVoteResponse struct {
	termTooLow  bool
	voteGranted bool
}

func (obj *RpcObject) RequestVoteRPC(args RequestVoteArgs, resp *RequestVoteResponse) error {
	obj.cm.ResetElectionTimer()
	if !obj.cm.HandleTerm(args.term, "") {
		resp.termTooLow = true
		return nil
	}

	// minimum election timer not elapsed
	if !obj.cm.ValidVoteRequest() {
		return nil
	}

	if !obj.cm.CanVoteFor(args.checkIdx, args.checkTerm) {
		resp.voteGranted = false
		return nil
	}

	resp.voteGranted = obj.cm.VoteFor(args.candidateID)
	return nil
}
