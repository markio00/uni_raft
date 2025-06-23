package raft

import (
	"context"
	"time"
)

/*
 * Replication Logic
 */

// receives commands from clients and starts the replication process
// - starts the replicator workers
// - when new logs available to replicate, signals workers to wake
func (cm *ConsensusModule) replicationManager() {
	// initialize workers and related infrastructure

	cm.appendNewLogEntry(Command{"NOOP"})

	ctx, cancel := context.WithCancel(context.Background())

	cm.leaderCtx = ctx
	cm.leaderCtxCancel = cancel
	// TODO: implement ctx based cascade cancel for all leader related activities

	go cm.betterConsensusTrackerLoop()

	replicatorChannels := map[NodeID]chan struct{}{}

	for id := range cm.clusterConfiguration {
		// do not replicate to themselves
		if SRV_ID == id {
			continue
		}

		// create and save wakeup channel
		ch := make(chan struct{})
		replicatorChannels[id] = ch

		// initialize worker
		go cm.replicatorWorker(id, ch)
	}

	for {
		select {
		// when leader deposed, stop the loop
		case <-cm.leaderCtx.Done():
			return
		// When receiving command from client
		case <-cm.signalNewEntryToReplicate:
		}

		// wakeup replicators
		for _, ch := range replicatorChannels {
			select {
			case ch <- struct{}{}: // signal replicator if idle
			default: // let it work otherwise
			}
		}
	}
}

// replicates all the available log entries to the target node and sends heartbeats as needed
// - when no further logs available for replication, goes to sleep
// - when woken probes for new logs and eventually starts the process
// - NEARTBEAT_DELAY time after last replicatin, an heartbeat is sent
func (cm *ConsensusModule) replicatorWorker(node NodeID, newLogsAvailable chan struct{}) {

	// Remote node replication index
	remoteNodeIdx := cm.currentIdx

	heartbeatTimer := time.NewTimer(HEARTBEAT_DELAY)

	for {
		// when leader deposed, stop the loop
		select {
		case <-cm.leaderCtx.Done():
			return
		default:
		}

		// reset heartbeat timer
		heartbeatTimer.Reset(HEARTBEAT_DELAY)

		// wait for new entries if no more logs to replicate
		if remoteNodeIdx == cm.currentIdx {

			// wait for new entries or send heartbeat
			select {
			case <-newLogsAvailable:
			case <-heartbeatTimer.C:
				// when heartbeat timer ticks, send heartbeat and continue with next iteration
				cm.sendAppendEntriesRPC(node, AppendEntriesArgs{
					leaderID:  SRV_ID,
					commitIdx: cm.commitIdx,
					term:      cm.currentTerm,
					ccIdx:     -1,
					ccTerm:    -1,
					entry:     nil,
				})
				continue
			}
		}

		currentReplicatingIdx := remoteNodeIdx + 1

		result := cm.sendAppendEntriesRPC(node, AppendEntriesArgs{
			leaderID:  SRV_ID,
			commitIdx: cm.commitIdx,
			term:      cm.currentTerm,
			ccIdx:     cm.log[remoteNodeIdx].idx,
			ccTerm:    cm.log[remoteNodeIdx].term,
			entry:     &cm.log[currentReplicatingIdx],
		})

		if result.ccPass {
			// if replication successful, update remote idx tracker and send ack to consensus tracker to calculate majority
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
		// when leader deposed, stop the loop
		select {
		case <-cm.leaderCtx.Done():
			return
		default:
		}

		// update ledger when receiving ack
		ack := <-cm.replicationAckChan
		ledger[ack.id] = ack.idx

		// if entry already committed, continue to next iteration
		if ack.idx <= cm.commitIdx {
			continue
		}

		// check commit consensus
		countReplicas := 0
		for _, commitIdx := range ledger {
			if ack.idx <= commitIdx {
				countReplicas++
			}
		}

		if countReplicas >= cm.quorum {
			cm.commitIdx = ack.idx
			cm.commitSignalingChans[ack.idx] <- nil
		}
	}
}
