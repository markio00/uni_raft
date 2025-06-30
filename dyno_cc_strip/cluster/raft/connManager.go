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

// connects to a new node's RPC server
// - log warnings after RPC_CONN_WARN_TIMEOUT
// - auto retry at RPC_CONN_TIMEOUT interval
// - after RPC_CONN_RETRIES attempts, interval is increased to RPC_CONN_LONG_TIMEOUT
func (cm *ConsensusModule) tryConnection(ip NodeID) {

	// Use net.Dialer to provide context with timeout
	dialer := &net.Dialer{}
	attempts := 0

	// Init timer with short timeout
	retryTimer := time.NewTimer(RPC_CONN_TIMEOUT)
	for {
		// After RPC_CONN_RETRIES retry, wait longer
		attempts++
		if attempts > RPC_CONN_RETRIES {
			retryTimer.Reset(RPC_CONN_LONG_TIMEOUT)
		} else {
			retryTimer.Reset(RPC_CONN_TIMEOUT)
		}

		// Create the timed-out context
		ctx, cancel := context.WithTimeout(context.Background(), RPC_CONN_TIMEOUT)
		defer cancel()

		// Send a warning if connection takes long
		warnTimer := time.AfterFunc(RPC_CONN_WARN_TIMEOUT, func() {
			slog.Warn("Server %v unresponsive after %v", string(ip), RPC_CONN_WARN_TIMEOUT)
		})

		// Try connection
		conn, err := dialer.DialContext(ctx, "tcp", string(ip)+RPC_PORT)
		if err == nil {
			// if conn successful delete context and add client to the configuration
			slog.Info("Connection to server %v successful", string(ip))
			warnTimer.Stop()
			cm.mu.Lock()
			cm.clusterConfiguration[ip].client = rpc.NewClient(conn)
			cm.mu.Unlock()

			// signal conenction now available
			cm.clusterConfiguration[ip].canConnect <- struct{}{}
			return
		}

		// if timeout or conn err occurrs, log it and cancel the context
		slog.Warn("Couldn't connect to %v: %v", string(ip), err)
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
	cm.clusterConfiguration[id].client.Close()
	delete(cm.clusterConfiguration, id)
	cm.mu.Unlock()
}

func (cm *ConsensusModule) sendRpcRequest(id NodeID, method string, request any, reply any) {

	cm.clusterConfiguration[id].mu.Lock()
	defer cm.clusterConfiguration[id].mu.Unlock()

	for {
		// if try conn in progress wait
		if cm.clusterConfiguration[id].client == nil {
			select {
			// if cancelation signal, early return
			case <-cm.ctx.Done():
				return
			// when try conn terminates proceed with call request
			case <-cm.clusterConfiguration[id].canConnect:
			}
		}

		// try request
		done := make(chan *rpc.Call)
		cm.clusterConfiguration[id].client.Go("RpcObject."+method, request, reply, done)

		select {
		// if call succeed, return with correct result
		case call := <-done:
			if call.Error == nil {
				return
			}
		// if cancelation signal, early return
		case <-cm.ctx.Done():
			return
		}

		// if call error, crop conn and try new one
		cm.dropConnection(id)
		go cm.tryConnection(id)
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
