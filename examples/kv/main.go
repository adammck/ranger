package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adammck/ranger/examples/kv/pkg/node"
	"github.com/adammck/ranger/examples/kv/pkg/proxy"
)

type Runner interface {
	Run(ctx context.Context) error
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
	fnod := flag.Bool("node", false, "start a node")
	fprx := flag.Bool("proxy", false, "start a proxy")

	addrLis := flag.String("addr", "localhost:8000", "address to start grpc server on")
	addrPub := flag.String("pub-addr", "", "address for other nodes to reach this (default: same as -listen)")
	drain := flag.Bool("drain", false, "node: drain ranges before shutting down")
	LogReqs := flag.Bool("log-reqs", false, "proxy, node: enable request logging")
	chaos := flag.Bool("chaos", false, "enable random failures and delays")
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

	if *fnod && !*fprx {
		cmd, err = node.New(*addrLis, *addrPub, *drain, *LogReqs, *chaos)

	} else if !*fnod && *fprx {
		cmd, err = proxy.New(*addrLis, *addrPub, *LogReqs)

	} else {
		err = errors.New("must provide one of -node, -proxy")
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
