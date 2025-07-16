package raft

type path = string
type opType = int

const (
	READ opType = iota
	WRITE
	NONE
)

func isCmdValid(cmd Command) bool {

	isCmdFound := false
	var mainCmd string
	supportedCmds := []string{"CREATE", "READ", "UPDATE", "DELETE"}

	if len(cmd) < 2 || len(cmd) > 3 {
		return false
	}

	mainCmd = cmd[0]

	for _, supportedCmd := range supportedCmds {
		isCmdFound = supportedCmd == mainCmd
		if isCmdFound {
			break
		}
	}

	if !isCmdFound {
		return false
	}

	if mainCmd == "UPDATE" && len(cmd) != 3 {
		// UPDATE <path> <content> syntax not followed
		return false
	}

	if mainCmd != "UPDATE" && len(cmd) != 2 {
		// <CREATE|DELETE|READ> <path> syntax not followed
		return false
	}

	return true
}

func getPath(cmd Command) path {

	if !isCmdValid(cmd) {
		return ""
	} else {
		return path(cmd[1])
	}
}

func getOpType(cmd Command) opType {

	if !isCmdValid(cmd) {
		return NONE
	} else if cmd[0] == "READ" {
		return READ
	} else {
		return WRITE
	}
}

func doOpTypesConflict(opType1 opType, opType2 opType) bool {
	return opType1 == READ && opType2 == WRITE ||
				 opType1 == WRITE && opType2 == READ ||
		     opType1 == WRITE && opType2 == WRITE
}

func (cm *ConsensusModule) SyncToLeaderFilesystem(targetCommitIdx int) {
	i := cm.commitIdx + 1
	for range targetCommitIdx - cm.commitIdx {
		cm.applyToState(cm.log[i].cmd)
		i++
	}
}
