package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/adammck/ranger/examples/kv/pkg/controller"
	"github.com/adammck/ranger/examples/kv/pkg/node"
)

func main() {
	fnod := flag.Bool("node", false, "start a kv node")
	fctl := flag.Bool("controller", false, "start a controller node")
	addr := flag.String("addr", ":9000", "address to listen on")
	flag.Parse()

	if *fnod && !*fctl {
		node.Start(*addr)

	} else if *fctl && !*fnod {
		controller.Start(*addr)

	} else {
		fmt.Fprintln(os.Stderr, "Must provide one of -node, -controller")
		os.Exit(1)
	}
}
