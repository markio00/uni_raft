package main

import "mmarkia/uni/raft/explorative/raft"

func main() {

	cm := raft.NewConsensusModule(updateState, []string{})

	cm.Start()
}

func updateState(some string) (bool, error) {
	return some == "hey", nil
}
