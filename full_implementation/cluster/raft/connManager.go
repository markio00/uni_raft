package raft

import (
	"context"
	"log/slog"
	"net"
	"net/rpc"
	"time"
)

const (
	RPC_PORT              = ":xxxx"
	RPC_CONN_WARN_TIMEOUT = 3 * time.Second
	RPC_CONN_TIMEOUT      = 5 * time.Second
	RPC_CONN_LONG_TIMEOUT = 1 * time.Minute
	RPC_CONN_RETRIES      = 5
	SRV_ID                = "" // TODO: add server ip constant
)

// Connection manager loop
// - receives connection related events through channels and applies the repated action
// - adds connections
// - deletes connections
func (cm *ConsensusModule) connectionManager() {
	for {
		select {
		case id := <-cm.newConnChan:
			go cm.tryConnection(id)
		case id := <-cm.delConnChan:
			go cm.dropConnection(id)
		}
	}
}

// connects to a new node's RPC server
// - log warnings after RPC_CONN_WARN_TIMEOUT
// - auto retry at RPC_CONN_TIMEOUT interval
// - after RPC_CONN_RETRIES attempts, interval is increased to RPC_CONN_LONG_TIMEOUT
func (cm *ConsensusModule) tryConnection(ip NodeID) {
	// FIX: when performing a 'Call' the underlying client may still be connecting and must wait and queue
	// INFO: most likely to happen in the first call to RequestVoteRPC electing first leader because first call to be made
	// since other calls are AppendEntriesRPC made by a leader that does not yet exist
	// Unlikely to happen in leader behaviour since comms are synchronous for each node but could still happen for
	// minority nodes still connecting during election won by the majority who was online
	// IDEA: since no requests come in parallel, put sync chan to queue the reqeust.
	// Remember to flush the queue when reqeust expires (e.g. vote terminated, leader deposed, ...)

	// Use net.Dialer to provide context with timeout
	dialer := &net.Dialer{}
	attempts := 0

	// Init timer with short timeout
	retryTimer := time.NewTimer(RPC_CONN_TIMEOUT)
	for {
		// After RPC_CONN_RETRIES retry, wait longer
		attempts++
		if attempts > RPC_CONN_RETRIES {
			retryTimer.Reset(RPC_CONN_TIMEOUT)
		} else {
			retryTimer.Reset(RPC_CONN_LONG_TIMEOUT)
		}

		// Create the timed-out context
		ctx, cancel := context.WithTimeout(context.Background(), RPC_CONN_TIMEOUT)
		defer cancel()

		// Send a warning if connection takes long
		warnTimer := time.AfterFunc(RPC_CONN_WARN_TIMEOUT, func() {
			slog.Warn("Server %v unresponsive after %v", ip, RPC_CONN_WARN_TIMEOUT)
		})

		// Try connection
		conn, err := dialer.DialContext(ctx, "tcp", string(ip)+RPC_PORT)
		if err == nil {
			// if conn successful delete context and add client to the configuration
			slog.Info("Connection to server %v successful", ip)
			warnTimer.Stop()
			cm.mu.Lock()
			cm.clusterConfiguration[ip] = rpc.NewClient(conn)
			cm.mu.Unlock()
			// ============================= idea about conn, call decoupling
			ch <- struct{}{}
			// ============================= idea about conn, call decoupling
			return
		}

		// if timeout or conn err occurrs, log it and cancel the context
		slog.Warn("Couldn't connect to %v: %v", ip, err)
		cancel()

		// wait for retry timer
		// - if runs out before conn timeout won't be blocking
		// - may be longer than conn timeout
		<-retryTimer.C
	}
}

// delete connection and
func (cm *ConsensusModule) dropConnection(id NodeID) {
	cm.mu.Lock()
	cm.clusterConfiguration[id].Close()
	delete(cm.clusterConfiguration, id)
	cm.mu.Unlock()
}

func (cm *ConsensusModule) sendRpcRequest(id NodeID, method string, request any, reply any) {
	for {
		// FIX: if connection attempt in progress, wait
		if cm.clusterConfiguration[id] == nil {
			return
		}

		// try request
		err := cm.clusterConfiguration[id].Call("RpcObject."+method, request, reply)
		if err == nil {
			// if ok return
			return
		}

		// if net fail drop connection
		cm.dropConnection(id)
		// reconnect
		// ============================= idea about conn, call decoupling
		ch := make(chan struct{})
		go cm.tryConnection(id, ch)

		ctx, cancel := context.WithCancel(cm.leaderCtx)
		defer cancel()
		// parent context used for prpagation of cancel signals
		select {
		case <-ctx.Done():
			return
		case <-ch:
		}
		// ============================= idea about conn, call decoupling

		// and retry
	}
}

func (cm *ConsensusModule) sendAppendEntriesRPC(id NodeID, request AppendEntriesArgs) *AppendEntriesResponse {
	result := AppendEntriesResponse{}

	cm.sendRpcRequest(id, "AppendEntriesRPC", request, &result)

	return &result
}

func (cm *ConsensusModule) sendRequestVoteRPC(id NodeID, request RequestVoteArgs) *RequestVoteResponse {
	result := RequestVoteResponse{}

	cm.sendRpcRequest(id, "RequestVoteRPC", request, &result)

	return &result
}
