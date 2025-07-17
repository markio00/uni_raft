package main

import (
	"log"
	"mmarkia/uni/raft/full/cluster/raft"
	"os"
	"strconv"
	"time"
)

func main() {

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

	time.Sleep(10 * time.Minute)

}

var d = 2 * time.Second
var x *time.Timer

func main2() {
	x = time.AfterFunc(d, f)
	time.Sleep(1 * time.Minute)
}

func f() {
	x.Reset(d)
	println("GG")
	time.Sleep(3 * time.Second)
	println("Hey!")
}

// TODO: Put waitgroups to wait for architectural goroutines
