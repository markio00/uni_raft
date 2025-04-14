package main

import (
	"fmt"
	"net"
	"net/rpc"
	//"sync"
	"time"
)

func rpcMain1() {
	go server()

	time.Sleep(1 * time.Second)

	client, err := rpc.Dial("tcp", "localhost:8888")
	if err != nil {
		fmt.Println(err.Error())
		panic("error creating client")
	}
	/*

		ch := chan struct{}
			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func(wg *sync.WaitGroup, chan struct{}) {
				defer wg.Done()
				resp1 := new(Ret)
				call1 := client.Call("RPC.RemoteCall", Args{Wait: true, Tag: "1"}, &resp1)
				if call1 != nil {
					panic("1 + " + call1.Error())
				}
				ch<- struct{}{}

			}(wg)
			// time.Sleep(3 * time.Second)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				resp2 := new(Ret)
				call2 := client.Call("RPC.RemoteCall", Args{Wait: false, Tag: "2"}, &resp2)
				if call2 != nil {
					panic("2 + " + call2.Error())
				}
			}(wg)

			<-ch
			wg.Wait()
	*/

	resp1 := new(Ret)
	call1 := client.Go("RPC.RemoteCall", Args{Wait: true, Tag: "1"}, &resp1, nil)
	resp2 := new(Ret)
	call2 := client.Go("RPC.RemoteCall", Args{Wait: false, Tag: "2"}, &resp2, nil)

	<-call1.Done
	<-call2.Done

	call2.Error.Error()
}

type (
	RPC  struct{}
	Args struct {
		Wait bool
		Tag  string
	}
	Ret int
)

func server() {
	l, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic("error listening")
	}

	obj := new(RPC)
	rpc.Register(obj)

	for {
		println("serving conn")
		conn, err := l.Accept()
		if err != nil {
			panic("conn err = " + err.Error())
			continue
		}
		println("hola")
		// handle incoming connection
		go rpc.ServeConn(conn)
	}
}

func (o *RPC) RemoteCall(args Args, resp *Ret) error {
	println(args.Tag + "REMOTE (?)")
	if args.Wait {
		time.Sleep(2 * time.Second)
	}

	fmt.Println(args.Tag)

	return nil
}

func some() {
	time.Sleep(1 * time.Second)
	print("oh $hit")
}

func rpcMain2() {
	go some()
	go func() {
		time.Sleep(1 * time.Second)
		print("PD")
	}()

	time.Sleep(5 * time.Second)
}

func main() {
	rpcMain2()
}
