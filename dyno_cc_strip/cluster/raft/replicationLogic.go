package raft

import (
	"context"
	"log"
	"time"
)

/*
 * Replication Logic
 */

// receives commands from clients and starts the replication process
// - starts the replicator workers
// - when new logs available to replicate, signals workers to wake
func (cm *ConsensusModule) replicationManager() {

	ctx, cancel := context.WithCancel(context.Background())
	cm.ctx = ctx
	cm.ctxCancel = cancel

	// initialize workers and related infrastructure

	cm.mu.Lock()
	cm.appendNewLogEntry(Command{"NOOP"})

	log.Printf("replicationManager: Appended NOOP log entry\n")

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

	cm.mu.Unlock()

	for {
		select {
		// when leader deposed, stop the loop
		case <-cm.ctx.Done():
			log.Printf("replicationManager: Exiting due to cancellation signal\n")

			return
		// When receiving command from client
		case <-cm.signalNewEntryToReplicate:
			log.Printf("replicationManager: A new entry has to be replicated!\n")
		}

		// wakeup replicators
		for id, ch := range replicatorChannels {
			select {
			case ch <- struct{}{}: // signal replicator if idle
			default: // let it work otherwise
				log.Printf("replicationManager: Couldn't signal replicator for %s as it wasn't idle\n", id)
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
	cm.mu.Lock()
	remoteNodeIdx := cm.currentIdx
	cm.mu.Unlock()

	heartbeatTimer := time.NewTimer(HEARTBEAT_DELAY)

	for {
		// when leader deposed, stop the loop
		select {
		case <-cm.ctx.Done():
			log.Printf("replicatorWorker#%s: Exiting due to cancellation signal\n", node)
			return
		default:
		}

		// reset heartbeat timer
		heartbeatTimer.Reset(HEARTBEAT_DELAY)

		// wait for new entries if no more logs to replicate
		cm.mu.Lock()
		if remoteNodeIdx == cm.currentIdx {
			cm.mu.Unlock()

			// wait for new entries or send heartbeat
			select {
			case <-newLogsAvailable:
			case <-heartbeatTimer.C:
				// when heartbeat timer ticks, send heartbeat and continue with next iteration
				log.Printf("replicatorWorker#%s: Sending heartbeat\n", node)
				cm.mu.Lock()
				args := AppendEntriesArgs{
					leaderID:  SRV_ID,
					commitIdx: cm.commitIdx,
					term:      cm.currentTerm,
					ccIdx:     -1,
					ccTerm:    -1,
					entry:     nil,
				}
				cm.mu.Unlock()
				cm.sendAppendEntriesRPC(node, args)
				continue
			}
		} else {
			cm.mu.Unlock()
		}

		currentReplicatingIdx := remoteNodeIdx + 1
		
		cm.mu.Lock()
		args := AppendEntriesArgs{
			leaderID:  SRV_ID,
			commitIdx: cm.commitIdx,
			term:      cm.currentTerm,
			ccIdx:     cm.log[remoteNodeIdx].idx,
			ccTerm:    cm.log[remoteNodeIdx].term,
			entry:     &cm.log[currentReplicatingIdx],
		}
		cm.mu.Unlock()

		result := cm.sendAppendEntriesRPC(node, args)

		if result.ccPass {
			// if replication successful, update remote idx tracker and send ack to consensus tracker to calculate majority
			remoteNodeIdx = currentReplicatingIdx
			cm.replicationAckChan <- ReplicationAck{
				id:  node,
				idx: currentReplicatingIdx,
			}

			log.Printf("replicatorWorker#%s: entry %d accepted\n", node, currentReplicatingIdx)
		} else {
			// if replication unsuccessful (consistency check fail) decrease remote idx tracker

			log.Printf("replicatorWorker#%s: entry %d not accepted\n", node, currentReplicatingIdx)
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
		case <-cm.ctx.Done():
			log.Printf("betterConsensusTrackerLoop: exiting due to cancellation signal\n")
			return
		default:
		}

		// update ledger when receiving ack
		ack := <-cm.replicationAckChan
		ledger[ack.id] = ack.idx

		// if entry already committed, continue to next iteration
		cm.mu.Lock()
		if ack.idx <= cm.commitIdx {
			cm.mu.Unlock()
			continue
		}

		// check commit consensus
		countReplicas := 1 // leader obviously stores the entry
		for _, commitIdx := range ledger {
			if ack.idx <= commitIdx {
				countReplicas++
			}
		}

		if countReplicas >= cm.quorum {
			cm.mu.Lock()
			cm.commitIdx = ack.idx
			cm.mu.Unlock()
			cm.commitSignalingChans[ack.idx] <- nil

			log.Printf("betterConsensusTrackerLoop: entry %d now committed\n", ack.idx)
		}
	}
}
