package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/adammck/ranger/examples/kv/pkg/controller"
	"github.com/adammck/ranger/examples/kv/pkg/node"
	"github.com/adammck/ranger/examples/kv/pkg/proxy"
)

func main() {
	fnod := flag.Bool("node", false, "start a node")
	fprx := flag.Bool("proxy", false, "start a proxy")
	fctl := flag.Bool("controller", false, "start a controller")

	addrLis := flag.String("addr", "localhost:8000", "address to start grpc server on")
	addrPub := flag.String("pub-addr", "", "address for other nodes to reach this (default: same as -listen)")
	flag.Parse()

	if *addrPub == "" {
		*addrPub = *addrLis
	}

	sig := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sig, syscall.SIGINT)

	// Wait for SIGINT
	go func() {
		s := <-sig
		fmt.Fprintf(os.Stderr, "Signal: %v\n", s)
		done <- true
	}()

	if *fnod && !*fprx && !*fctl {
		n, err := node.New(*addrLis, *addrPub)
		if err != nil {
			exit(err)
		}

		if err := n.Run(done); err != nil {
			exit(err)
		}

	} else if !*fnod && *fprx && !*fctl {
		c, err := proxy.New(*addrLis, *addrPub)
		if err != nil {
			exit(err)
		}

		if err := c.Run(done); err != nil {
			exit(err)
		}

	} else if !*fnod && !*fprx && *fctl {
		// TODO: This branch seems very similar to the previous...

		c, err := controller.New(*addrLis, *addrPub)
		if err != nil {
			exit(err)
		}

		if err := c.Run(done); err != nil {
			exit(err)
		}

	} else {
		exit(errors.New("must provide one of -node, -controller"))
	}

}

func exit(err error) {
	fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	os.Exit(1)
}
