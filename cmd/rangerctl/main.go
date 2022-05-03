package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

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
		fmt.Fprintf(w, "  - ranges\n")
		fmt.Fprintf(w, "  - range <rangeID>\n")
		fmt.Fprintf(w, "  - nodes\n")
		fmt.Fprintf(w, "  - node <nodeID>\n")
		fmt.Fprintf(w, "  - move <rangeID> [<nodeID>]\n")
		fmt.Fprintf(w, "  - split <rangeID> <boundary> [<nodeID>] [<nodeID>]\n")
		fmt.Fprintf(w, "  - join <rangeID> <rangeID> [<nodeID>]\n")
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "Flags:\n")
		flag.PrintDefaults()
	}

	addr := flag.String("addr", "localhost:5000", "controller address")
	printReq := flag.Bool("request", false, "print gRPC request instead of sending it")
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// TODO: Catch signals for cancellation.
	ctx := context.Background()

	ctxDial, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctxDial, *addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("Error dialing controller: %v\n", err)
		os.Exit(1)
	}

	action := flag.Arg(0)
	switch action {
	case "ranges":
		if flag.NArg() != 1 {
			fmt.Fprintf(w, "Usage: %s ranges\n", os.Args[0])
			os.Exit(1)
		}

		client := pb.NewDebugClient(conn)
		cmdRanges(*printReq, client, ctx)

	case "range", "r":
		if flag.NArg() != 2 {
			fmt.Fprintf(w, "Usage: %s range <rangeID>\n", os.Args[0])
			os.Exit(1)
		}

		rID, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			fmt.Fprintf(w, "Invalid rangeID: %v\n", err)
			os.Exit(1)
		}

		client := pb.NewDebugClient(conn)
		cmdRange(*printReq, client, ctx, rID)

	case "nodes":
		if flag.NArg() != 1 {
			fmt.Fprintf(w, "Usage: %s nodes\n", os.Args[0])
			os.Exit(1)
		}

		client := pb.NewDebugClient(conn)
		cmdNodes(*printReq, client, ctx)

	case "node", "n":
		if flag.NArg() != 2 {
			fmt.Fprintf(w, "Usage: %s node <nodeID>\n", os.Args[0])
			os.Exit(1)
		}

		client := pb.NewDebugClient(conn)
		cmdNode(*printReq, client, ctx, flag.Arg(1))

	case "move", "m":
		if flag.NArg() < 2 || flag.NArg() > 3 {
			fmt.Fprintf(w, "Usage: %s move <rangeID> [<nodeID>]\n", os.Args[0])
			os.Exit(1)
		}

		rID, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			fmt.Fprintf(w, "Invalid rangeID: %v\n", err)
			os.Exit(1)
		}

		client := pb.NewOrchestratorClient(conn)
		cmdMove(*printReq, client, ctx, rID, flag.Arg(2))

	case "split", "s":
		if flag.NArg() < 3 || flag.NArg() > 5 {
			fmt.Fprintf(w, "Usage: %s split <rangeID> <boundary> [<nodeID>] [<nodeID>]\n", os.Args[0])
			os.Exit(1)
		}

		boundary := []byte(flag.Arg(2))

		// If the boundary is prefixed with 'b64:' then decode the rest.
		// Sometimes we want to split at points which are not printable chars.
		p := []byte("b64:")
		if bytes.HasPrefix(boundary, p) {
			b := bytes.TrimPrefix(boundary, p)
			_, err = base64.StdEncoding.Decode(boundary, b)

			if err != nil {
				fmt.Fprintf(w, "Invalid base64-encoded boundary: %v\n", b)
				os.Exit(1)
			}
		}

		rID, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			fmt.Fprintf(w, "Invalid rangeID: %v\n", err)
			os.Exit(1)
		}

		client := pb.NewOrchestratorClient(conn)
		cmdSplit(*printReq, client, ctx, rID, boundary, flag.Arg(3), flag.Arg(4))

	case "join", "j":
		if flag.NArg() < 3 || flag.NArg() > 4 {
			fmt.Fprintf(w, "Usage: %s join <rangeID> <rangeID> [<nodeID>]\n", os.Args[0])
			os.Exit(1)
		}

		rIDs := [2]uint64{}
		for i, s := range []string{flag.Arg(1), flag.Arg(2)} {
			rID, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				fmt.Fprintf(w, "Invalid rangeID: %v\n", err)
				os.Exit(1)
			}
			rIDs[i] = rID
		}

		client := pb.NewOrchestratorClient(conn)
		cmdJoin(*printReq, client, ctx, rIDs[0], rIDs[1], flag.Arg(3))

	default:
		flag.Usage()
		os.Exit(1)
	}
}

func cmdRanges(printReq bool, client pb.DebugClient, ctx context.Context) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req := &pb.RangesListRequest{}

	if printReq {
		output(req)
		return
	}

	res, err := client.RangesList(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.RangesList returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdRange(printReq bool, client pb.DebugClient, ctx context.Context, rID uint64) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req := &pb.RangeRequest{Range: rID}

	if printReq {
		output(req)
		return
	}

	res, err := client.Range(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.Range returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdNodes(printReq bool, client pb.DebugClient, ctx context.Context) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req := &pb.NodesListRequest{}

	if printReq {
		output(req)
		return
	}

	res, err := client.NodesList(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.NodesList returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdNode(printReq bool, client pb.DebugClient, ctx context.Context, nID string) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req := &pb.NodeRequest{Node: nID}

	if printReq {
		output(req)
		return
	}

	res, err := client.Node(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.Node returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdMove(printReq bool, client pb.OrchestratorClient, ctx context.Context, rID uint64, nID string) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := &pb.MoveRequest{
		Range: rID,
		Node:  nID,
	}

	if printReq {
		output(req)
		return
	}

	res, err := client.Move(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.Move returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdSplit(printReq bool, client pb.OrchestratorClient, ctx context.Context, rID uint64, boundary []byte, nID1, nID2 string) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := &pb.SplitRequest{
		Range:     rID,
		Boundary:  boundary,
		NodeLeft:  nID1,
		NodeRight: nID2,
	}

	if printReq {
		output(req)
		return
	}

	res, err := client.Split(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.Split returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func cmdJoin(printReq bool, client pb.OrchestratorClient, ctx context.Context, rID1, rID2 uint64, nID string) {
	w := flag.CommandLine.Output()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := &pb.JoinRequest{
		RangeLeft:  rID1,
		RangeRight: rID2,
		Node:       nID,
	}

	if printReq {
		output(req)
		return
	}

	res, err := client.Join(ctx, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.Join returned: %v\n", err)
		os.Exit(1)
	}

	output(res)
}

func output(res protoreflect.ProtoMessage) {
	opts := protojson.MarshalOptions{
		Multiline:       true,
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}

	fmt.Println(opts.Format(res))
}
