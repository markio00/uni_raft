package raft

import "time"

/*
 * Replicatin Logic
 */

// receives commands from clients and starts the replication process
// - starts the replicator workers
// - when new logs available to replicate, signals workers to wake
func (cm *ConsensusModule) replicationManager() {
	// initialize workers and related infrastructure
	for id := range cm.clusterConfiguration {
		// create and save wakeup channel
		ch := make(chan struct{})
		cm.replicatorChannels[id] = ch

		// initizlize worker
		go cm.replicatorWorker(id, ch, false)
	}

	for {
		// When receiving command from client
		cmd := <-cm.signalNewEntryToReplciate
		cm.mu.Lock()

		// wakeup replicators
		for _, ch := range cm.replicatorChannels {
			select {
			case ch <- struct{}{}: // signal replicator if idle
			default: // let it work otherwise
			}
		}

		cm.mu.Unlock()
	}
}

// replicates all the available log entries to the target node and sends heartbeats as needed
// - when no further logs available for replication, goes to sleep
// - when woken probes for new logs and eventually starts the process
// - NEARTBEAT_DELAY time after last replicatin, an heartbeat is sent
func (cm *ConsensusModule) replicatorWorker(node NodeID, newLogsAvailable chan struct{}, newNode bool) {
	remoteNodeIdx := cm.currentIdx
	heartbeatTimer := time.NewTimer(HEARTBEAT_DELAY)

	for {
		// start heartbeat timer
		heartbeatTimer.Reset(HEARTBEAT_DELAY)

		// wait for new entries if no more logs to replicate
		if remoteNodeIdx == cm.currentIdx {

			// when "commit level" reached for the first time by new (non voting) node
			// - signal readyness to vote (now voting member)
			if !newNode {
				newNode = true
				cm.newVotingMembersChan <- node
			}

			// wait for new entries or send heartbeat
			select {
			case <-newLogsAvailable:
			case <-heartbeatTimer.C:
				// when heartbeat timer ticks, send heartbeat and continue with next iteration
				// TODO: send heartbeat RPC
				continue
			}
		}

		currentReplicatingIdx := remoteNodeIdx + 1
		result := true // TODO: make AppendEntry rpc
		if result {
			// if replication successful, update remote idx tracker and send ack to consensus loop to calculate majority
			remoteNodeIdx = currentReplicatingIdx
			cm.replicationAckChan <- ReplicationAck{
				id:  node,
				idx: currentReplicatingIdx,
			}
		} else {
			// if replication unsuccessful (consistency check fail) decrease remote idx tracker
			remoteNodeIdx--
		}

	}
}

func (cm *ConsensusModule) betterConsensusTrackerLoop() {
	// initialize idx ledger (keeps track of last replicated idx for each node)
	ledger := map[NodeID]int{}

	for {
		// update ledger when receiving ack
		ack := <-cm.replicationAckChan
		ledger[ack.id] = ack.idx

		// if entry already committed, continue to next iteration
		if ack.idx <= cm.commitIdx {
			continue
		}

		// check commit consensus
		isQuorumReached := false

		if !cm.isIntermediateConfig {
			count := 0
			for id, commitIdx := range ledger {
				if !sliceContains(cm.nonVotingNodes, id) && ack.idx <= commitIdx {
					count++
				}
			}
			quorumOld, _ := cm.getQuorums()
			isQuorumReached = count >= quorumOld

		} else {
			countOld := 0
			countNew := 0
			for id, commitIdx := range ledger {
				if sliceContains(cm.oldConfig, id) && ack.idx <= commitIdx {
					countOld++
				}
			}
			for id, commitIdx := range ledger {
				if sliceContains(cm.newConfig, id) && ack.idx <= commitIdx {
					countNew++
				}
			}

			quorumOld, quorumNew := cm.getQuorums()
			isQuorumReached = countOld >= quorumOld && countNew >= quorumNew
		}

		if isQuorumReached {
			cm.commitIdx = ack.idx
			cm.registry[ack.idx] <- nil
			// TODO: apply to state (trigger callback)

			if cm.isIntermediateConfig && cm.lastConfigChangeIdx <= cm.commitIdx {
				// start phase 2
				go cm.applyLeaderConfigChangePhase2()
				// WARN: isIntermediateConfig has to be set to false before any other idx is committeed because otherwiswe
				// phase 2 will execute multiple times
				// INFO: the condition checks if we are in intermdiate config and it's been committed to avoid edge case where
				// leader sets config change committed before sending new config
				// FIX:	keep a state lock until both idx set to committed and new cfg entry appended so that no node will ever
				// be in intermediate cfg with no new cfg
			}

		}
	}
}
