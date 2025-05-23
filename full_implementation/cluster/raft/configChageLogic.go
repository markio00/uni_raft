package raft

/*
 * Configuration Change Logic
 */

// Apply the configuratino change to a receiving follower
func (cm *ConsensusModule) applyFollowerConfigChange(cfg Configuration) {
	defer cm.mu.Unlock()
	cm.mu.Lock()

	if cfg[0] == "IC" {
		// if 'IF' flag set, set intermediate config
		cm.isIntermediateConfig = true

		// make the old config from the actual cluste config
		cm.oldConfig = make([]NodeID, 0, len(cm.clusterConfiguration))
		for k := range cm.clusterConfiguration {
			cm.oldConfig = append(cm.oldConfig, k)
		}

		// make the new config from the received config
		cm.newConfig = make([]NodeID, 0, len(cfg[1:]))
		for _, v := range cfg[1:] {
			cm.newConfig = append(cm.newConfig, NodeID(v))
		}

		// add connections for the new nodes
		for _, newNode := range cfg[1:] {
			found := false
			for _, v := range cm.oldConfig {
				found = found || NodeID(newNode) == v
			}

			if !found {
				// send the NodeID to the connManager to add the connection
				cm.newConnChan <- NodeID(newNode)
			}
		}
	}

	if cfg[0] == "NC" {
		// delete connections for the old nodes
		for _, oldNode := range cm.oldConfig {
			found := false
			for _, v := range cm.newConfig {
				found = found || NodeID(oldNode) == v
			}

			if !found {
				// send the NodeID to the connManager to delete the connection
				cm.delConnChan <- NodeID(oldNode)
			}
		}

		// reset intermediate config fields
		cm.oldConfig = []NodeID{}
		cm.newConfig = []NodeID{}
		cm.isIntermediateConfig = false
	}
}

func (cm *ConsensusModule) prepareLeaderConfigChange(cfg Configuration) Command {
	cm.nonVotingNodes = make([]NodeID, 0, 0)
	for _, newNode := range cfg[1:] {
		found := false
		for currNode := range cm.clusterConfiguration {
			found = found || currNode == newNode
		}

		// if node is a new node for the cluster
		if !found {
			cm.newConnChan <- NodeID(newNode)                      // init new connection to node
			cm.nonVotingNodes = append(cm.nonVotingNodes, newNode) // add node to the "non voting" filter

			cm.replicatorChannels[newNode] = make(chan struct{}) // prepare infrastructure for replicator worker

			// init related replicatio worker
			go cm.replicatorWorker(newNode, cm.replicatorChannels[newNode], true)
		}
	}

	// wait for all nodes to get upt to commit level and gain voting privileges
	for range cm.nonVotingNodes {
		<-cm.newVotingMembersChan
	}
	cm.nonVotingNodes = []NodeID{}

	// when config change entry is committed, consensus tracker loop will start phase 2

	// return raft command to replicate
	return append([]string{"CC", "IC"}, cfg[1:]...)
}

func (cm *ConsensusModule) applyLeaderConfigChangePhase2() {
	cm.isIntermediateConfig = false

	// append new config to log and start replication
	cm.appendNewLogEntry(append([]string{"CC", "NC"}, cm.newConfig...))

	// TODO: change config infastructure to new config

	// if not leader anymore (not in new config)
	cm.nodeStatus = FOLLOWER
	// destroy all replicators
	// and all leader loops

	// else if sitll leader (in new config)
	// destroy replicators for old nodes
}
