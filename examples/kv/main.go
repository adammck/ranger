package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
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

	fonce := flag.Bool("once", false, "controller: perform one rebalance cycle and exit")

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
		log.Printf("Signal: %v", s)
		done <- true
	}()

	if *fnod && !*fprx && !*fctl {
		defaultLogger(fmt.Sprintf("node %s", *addrPub))

		n, err := node.New(*addrLis, *addrPub)
		if err != nil {
			exit(err)
		}

		if err := n.Run(done); err != nil {
			exit(err)
		}

	} else if !*fnod && *fprx && !*fctl {
		defaultLogger(fmt.Sprintf("prxy %s", *addrPub))

		c, err := proxy.New(*addrLis, *addrPub)
		if err != nil {
			exit(err)
		}

		if err := c.Run(done); err != nil {
			exit(err)
		}

	} else if !*fnod && !*fprx && *fctl {
		defaultLogger(fmt.Sprintf("ctrl %s", *addrPub))

		c, err := controller.New(*addrLis, *addrPub, *fonce)
		if err != nil {
			exit(err)
		}

		if err := c.Run(done); err != nil {
			exit(err)
		}

	} else {
		exit(errors.New("must provide one of -controller, -node, -proxy"))
	}

}

func exit(err error) {
	log.Fatalf("Error: %s", err)
	os.Exit(1)
}

func defaultLogger(prefix string) {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", prefix), log.Lshortfile)
	*log.Default() = *logger
}
