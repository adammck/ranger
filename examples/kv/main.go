package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/adammck/ranger/examples/kv/pkg/controller"
	"github.com/adammck/ranger/examples/kv/pkg/node"
)

func main() {
	fnod := flag.Bool("node", false, "start a kv node")
	fctl := flag.Bool("controller", false, "start a controller node")
	addr := flag.String("addr", ":9000", "address to listen on")
	flag.Parse()

	var err error
	if *fnod && !*fctl {
		err = node.Serve(*addr)

	} else if *fctl && !*fnod {
		err = controller.Serve(*addr)

	} else {
		fmt.Fprintln(os.Stderr, "Must provide one of -node, -controller")
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
