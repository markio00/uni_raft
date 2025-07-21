package main

import (
	"fmt"
	"io"
	"log"
	"mmarkia/uni/raft/full/cluster/raft"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {

	role := os.Getenv("ROLE")
	switch role {
	case "SRV":
		server()
		break
	case "CLIENT":

		time.Sleep(30 * time.Second)
		id := os.Getenv("ID")
		nodes, err := strconv.Atoi(os.Getenv("NODES"))
		if err != nil {
			panic("n_nodes has to be a positive, non zero integer")
		}

		for i := range nodes {
			addr := string(id)[:len(id)-1] + strconv.Itoa(i+4)
			client(addr)
		}
		break
	}
}

func client(addr string) {

	ret, err := http.Get(addr + ":1234/ping")
	if err != nil {
		panic("CANNOT PING")
	}
	bytes, err := io.ReadAll(ret.Body)
	if err != nil {
		panic("CANNOT PING - READ BODY")
	}
	if err != nil || string(bytes) == "NOT LEADER" {
		panic("WAS NOT THE LEADER")
	}

	panic("WAS LEADER")

}

func sendReq(addr string, command string) (bool, error) {
	_, err := http.Get(addr + ":1234/cmd=" + strings.ReplaceAll(command, " ", "|"))
	return true, err
}

func server() {

	log.Println("=======================================================")
	log.Println("||                      STARTUP                      ||")
	log.Println("=======================================================")

	id := os.Getenv("ID")
	nodes, err := strconv.Atoi(os.Getenv("NODES"))
	if err != nil {
		panic("n_nodes has to be a positive, non zero integer")
	}

	log.Printf("Env vars: id: %s , nodes: %d\n", id, nodes)

	var conf raft.Configuration
	for i := range nodes {
		addr := string(id)[:len(id)-1] + strconv.Itoa(i+4)
		conf = append(conf, addr)
	}

	consMod := raft.NewRaftInstance(conf, id)
	consMod.Start()

	http.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {
		if consMod.IsLeader() {
			fmt.Fprintln(w, "LEADER")
		} else {

			fmt.Fprintln(w, "NOT LEADER")
		}
	})
	http.HandleFunc("GET /command", func(w http.ResponseWriter, r *http.Request) {
		// TODO: get actual arg
		var args []string

		res, err := consMod.ApplyCommand(args)
		if err != nil {
			fmt.Fprintf(w, "ERR - %s", err.Error())
		} else {
			fmt.Fprintf(w, "%s", res)
		}
	})
	http.ListenAndServe(":80", nil)

}

// TODO: Put waitgroups to wait for architectural goroutines
