package main

import (
	"log"
	"mmarkia/uni/raft/full/cluster/raft"
	"os"
	"strconv"
	"time"
)

func main() {

	role := os.Getenv("ROLE")
	switch role {
	case "SRV":
		server()
	case "CLIENT":
		client()
	}
}

func client() {
	// TODO: Implement
}

func server() {

	log.Println("=======================================================")
	log.Println("||                      STARTUP                      ||")
	log.Println("=======================================================")

	id := os.Getenv("ID")
	nodes, err := strconv.Atoi(os.Getenv("NODES"))
	if err != nil {
		log.Panic("NODES has to be a positive nonzero integer")
	}
	offset := nodes - 1

	log.Printf("Env. vars: id: %s, nodes: %d\n", id, nodes)

	var conf raft.Configuration
	for i := range nodes {
		addr := string(id)[:len(id)-1] + strconv.Itoa(i+offset)
		conf = append(conf, addr)
	}

	consMod := raft.NewRaftInstance(conf, id)
	consMod.Start()

	time.Sleep(10 * time.Minute) // FIX: Use waitgroups instead
}
