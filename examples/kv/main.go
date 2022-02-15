package main

import (
	"context"
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

type Runner interface {
	Run(ctx context.Context) error
}

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

	// Replace default logger.
	logger := log.New(os.Stdout, "", 0)
	*log.Default() = *logger

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	var cmd Runner
	var err error

	if *fnod && !*fprx && !*fctl {
		cmd, err = node.New(*addrLis, *addrPub)

	} else if !*fnod && *fprx && !*fctl {
		cmd, err = proxy.New(*addrLis, *addrPub)

	} else if !*fnod && !*fprx && *fctl {
		cmd, err = controller.New(*addrLis, *addrPub, *fonce)

	} else {
		err = errors.New("must provide one of -controller, -node, -proxy")
	}

	if err != nil {
		exit(err)
	}

	err = cmd.Run(ctx)
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	log.Fatalf("Error: %s", err)
}
