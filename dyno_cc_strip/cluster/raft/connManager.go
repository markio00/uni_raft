package raft

import (
	"context"
	"log"
	"net"
	"net/rpc"
	"time"
)

const (
	RPC_PORT              = ":1234"
	RPC_CONN_WARN_TIMEOUT = 10 * time.Second
	RPC_CONN_TIMEOUT      = 15 * time.Second
	RPC_CONN_LONG_TIMEOUT = 1 * time.Minute
	RPC_CONN_RETRIES      = 5
)

// connects to a new node's RPC server
// - log warnings after RPC_CONN_WARN_TIMEOUT
// - auto retry at RPC_CONN_TIMEOUT interval
// - after RPC_CONN_RETRIES attempts, interval is increased to RPC_CONN_LONG_TIMEOUT
func (cm *ConsensusModule) tryConnection(ip NodeID) {

	log.Printf("try conn: '%s'\n", ip)

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
			log.Printf("Server %s unresponsive after %d ns\n", string(ip), RPC_CONN_WARN_TIMEOUT)
		})

		// Try connection
		conn, err := dialer.DialContext(ctx, "tcp", string(ip)+RPC_PORT)
		if err == nil {
			// if conn successful delete context and add client to the configuration
			log.Printf("try conn: '%s' successful\n", ip)
			warnTimer.Stop()
			cm.connMutex.Lock()
			cm.clusterConfiguration[ip].client = rpc.NewClient(conn)
			cm.connMutex.Unlock()

			// signal connection now available
			cm.clusterConfiguration[ip].canConnect <- struct{}{}

			return
		}

		// if timeout or conn err occurrs, log it and cancel the context
		log.Printf("try conn: '%s' - ERR - %s\n", string(ip), err)
		cancel()

		// wait for retry timer
		// - if runs out before conn timeout won't be blocking
		// - may be longer than conn timeout
		<-retryTimer.C
	}
}

// delete connection and
func (cm *ConsensusModule) dropConnection(id NodeID) {
	cm.connMutex.Lock()
	cm.clusterConfiguration[id].client.Close()
	cm.connMutex.Unlock()
}

func (cm *ConsensusModule) sendRpcRequest(id NodeID, method string, request any, reply any) {

	log.Printf("attempting RPC: '%s' to '%s'\n", method, id)
	for {
		cm.connMutex.RLock()
		canConnect := cm.clusterConfiguration[id].canConnect
		// if try conn in progress wait
		if cm.clusterConfiguration[id].client == nil {

			cm.connMutex.RUnlock()
			select {
			// if cancelation signal, early return
			case <-cm.ctx.Done():
				log.Printf("attempting RPC: '%s' to '%s' - closed by cancelation signal\n", method, id)
				return
			// when try conn terminates proceed with call request
			case <-canConnect:
			}
		}

		log.Printf("attempting RPC: '%s' to '%s' - no try conn in progres\n", method, id)

		// try request
		cm.connMutex.RLock()
		done := make(chan *rpc.Call, 10)
		cm.clusterConfiguration[id].client.Go("RpcObject."+method, request, reply, done)
		log.Printf("attempting RPC: '%s' to '%s' - Invoked async\n", method, id)
		cm.connMutex.RUnlock()
		var err error
		err = nil
		select {
		// if call succeeds, return with correct result
		case call := <-done:
			if call.Error == nil {
				log.Printf("attempting RPC: '%s' to '%s' - returned successfuly\n", method, id)
				return
			} else {
				err = call.Error
			}
		// if cancelation signal, early return
		case <-cm.ctx.Done():
			log.Printf("attempting RPC: '%s' to '%s' - closed by cancelation signal\n", method, id)
			return
		}

		// if call error, crop conn and try new one
		log.Printf("attempting RPC: '%s' to '%s' - ERR - %s\n", method, id, err.Error())
		log.Printf("attempting RPC: '%s' to '%s' - trying new connection\n", method, id)
		cm.dropConnection(id)
		go cm.tryConnection(id)
	}
}

func (cm *ConsensusModule) sendAppendEntriesRPC(id NodeID, request AppendEntriesArgs) *AppendEntriesResponse {
	result := AppendEntriesResponse{
		CcPass: false,
	}

	cm.sendRpcRequest(id, "AppendEntriesRPC", request, &result)

	return &result
}

func (cm *ConsensusModule) sendRequestVoteRPC(id NodeID, request RequestVoteArgs) *RequestVoteResponse {
	result := RequestVoteResponse{
		VoteGranted: false,
	}

	cm.sendRpcRequest(id, "RequestVoteRPC", request, &result)

	return &result
}
