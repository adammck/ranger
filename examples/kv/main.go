package main

import (
	"errors"
	"flag"
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

	// Replace default logger.
	logger := log.New(os.Stdout, "", 0)
	*log.Default() = *logger

	if *addrPub == "" {
		*addrPub = *addrLis
	}

	sig := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
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
