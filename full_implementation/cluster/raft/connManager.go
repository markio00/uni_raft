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
			go cm.connectNode(id)
		case id := <-cm.delConnChan:
			go cm.disconnectNode(id)
		}
	}
}

// connects to a new node's RPC server
// - log warnings after RPC_CONN_WARN_TIMEOUT
// - auto retry at RPC_CONN_TIMEOUT interval
// - after RPC_CONN_RETRIES attempts, interval is increased to RPC_CONN_LONG_TIMEOUT
func (cm *ConsensusModule) connectNode(ip NodeID) {
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
		time.AfterFunc(RPC_CONN_WARN_TIMEOUT, func() {
			slog.Warn("Server %v unresponsive after %v", ip, RPC_CONN_WARN_TIMEOUT)
		})

		// Try connection
		conn, err := dialer.DialContext(ctx, "tcp", string(ip)+RPC_PORT)
		if err == nil {
			// if conn successful delete context and add client to the configuration
			slog.Info("Connection to server %v successful", ip)
			cm.mu.Lock()
			cm.clusterConfiguration[ip] = rpc.NewClient(conn)
			cm.mu.Unlock()
			return
		}

		// if timeout or conn err occurrs, log it and cancel the context
		slog.Warn("Couldn't connect to %v: %v", ip, err)
		cancel()

		// exit after 10 attempts
		// WARN: should probably retry indefinitely since it may get back online
		if attempts >= 10 {
			slog.Error("Stopping eonn attempts for %v after 10 attempts", ip)
			return // errors.New("too many attempts")
		}

		// wait for retry timer
		// - if runs out before conn timeout won't be blocking
		// - may be longer than conn timeout
		<-retryTimer.C
	}
}

// delete connection and
func (cm *ConsensusModule) disconnectNode(id NodeID) {
	cm.mu.Lock()
	cm.clusterConfiguration[id].Close()
	delete(cm.clusterConfiguration, id)
	cm.mu.Unlock()
}
