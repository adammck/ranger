package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addrLis := flag.String("addr", "localhost:8000", "address to start grpc server on")
	addrPub := flag.String("pub-addr", "", "address for other nodes to reach this (default: same as -addr)")
	interval := flag.Duration("interval", 250*time.Millisecond, "frequency of orchestration loop")
	once := flag.Bool("once", false, "perform one rebalance cycle and exit")
	flag.Parse()

	if *addrPub == "" {
		*addrPub = *addrLis
	}

	// Replace default logger.
	// TODO: Switch to a better logging package.
	logger := log.New(os.Stdout, "", 0)
	*log.Default() = *logger

	cmd, err := New(*addrLis, *addrPub, *interval, *once)
	if err != nil {
		exit(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	err = cmd.Run(ctx)
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	log.Fatalf("Error: %s", err)
}
