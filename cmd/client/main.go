package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func main() {
	w := flag.CommandLine.Output()

	flag.Usage = func() {
		fmt.Fprintf(w, "Usage: %s [-addr=host:port] <action> [<args>]\n", os.Args[0])
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "Action and args must be one of:\n")
		fmt.Fprintf(w, "  - range <rangeID>\n")
		fmt.Fprintf(w, "  - node <nodeID>\n")
		//fmt.Fprintf(w, "  - move <rangeID> [<nodeID>]\n")
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "Flags:\n")
		flag.PrintDefaults()
	}

	addr := flag.String("addr", "localhost:5000", "controller address")
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// TODO: Catch signals for cancellation.
	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, *addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("Error dialing controller: %v", err)
	}

	client := pb.NewDebugClient(conn)

	action := flag.Arg(0)
	switch action {
	case "range", "r":
		if flag.NArg() != 2 {
			fmt.Fprintf(w, "Usage: %s range <rangeID>\n", os.Args[0])
			os.Exit(1)
		}

		rID, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			fmt.Fprintf(w, "Invalid rangeID: %v", err)
			os.Exit(1)
		}

		cmdRange(client, ctx, rID)

	case "node", "n":
		if flag.NArg() != 2 {
			fmt.Fprintf(w, "Usage: %s node <nodeID>\n", os.Args[0])
			os.Exit(1)
		}

		cmdNode(client, ctx, flag.Arg(1))

	default:
		flag.Usage()
		os.Exit(1)
	}
}

func cmdRange(client pb.DebugClient, ctx context.Context, rID uint64) {
	w := flag.CommandLine.Output()

	res, err := client.Range(ctx, &pb.RangeRequest{Range: &pb.Ident{
		Key: rID,
	}})

	if err != nil {
		fmt.Fprintf(w, "Range service returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdNode(client pb.DebugClient, ctx context.Context, nID string) {
	w := flag.CommandLine.Output()

	res, err := client.Node(ctx, &pb.NodeRequest{Node: nID})

	if err != nil {
		fmt.Fprintf(w, "Node service returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func output(res protoreflect.ProtoMessage) {
	opts := protojson.MarshalOptions{
		Multiline:       true,
		EmitUnpopulated: true,
	}

	fmt.Println(opts.Format(res))
}
