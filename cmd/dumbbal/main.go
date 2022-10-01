package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

func main() {
	w := flag.CommandLine.Output()

	addr := flag.String("addr", "localhost:5000", "controller address")
	dryRun := flag.Bool("dry-run", false, "just print operations")
	force := flag.Bool("force", false, "actually run operations")
	num := flag.Uint("num", 10, "maximum number of operations to perform")
	splitScore := flag.Uint64("split", 100, "score threshold to split ranges")
	joinScore := flag.Uint64("join", 20, "score threshold to join adjacent ranges")
	flag.Parse()

	if !*dryRun && !*force {
		fmt.Fprint(w, "Error: either -dry-run or -force is required\n")
		os.Exit(1)
	}

	// TODO: Catch signals for cancellation.
	ctx := context.Background()

	// -- Create a debug client (like rangerctl)

	ctxDial, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctxDial, *addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(w, "Error dialing controller: %v\n", err)
		os.Exit(1)
	}

	client := pb.NewDebugClient(conn)

	// -- Fetch the current state of all the ranges

	ctxRL, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req := &pb.RangesListRequest{}

	res, err := client.RangesList(ctxRL, req)

	if err != nil {
		fmt.Fprintf(w, "Debug.RangesList returned: %v\n", err)
		os.Exit(1)
	}

	// -- Build list of ranges to examine

	type Placement struct {
		index int
		info  *pb.LoadInfo
	}

	type Range struct {
		rID        uint64
		placements []Placement
	}

	ranges := []Range{}

	for _, r := range res.Ranges {

		// Ignore non-active ranges (probably already being split/joined).
		if r.State != pb.RangeState_RS_ACTIVE {
			continue
		}

		placements := []Placement{}
		for i, p := range r.Placements {
			if p.Placement.State == pb.PlacementState_PS_ACTIVE {
				placements = append(placements, Placement{
					index: i,
					info:  p.RangeInfo.Info,
				})
			}
		}

		// Ignore ranges with no ready placements.
		if len(placements) == 0 {
			continue
		}

		ranges = append(ranges, Range{
			rID:        r.Meta.Ident,
			placements: placements,
		})
	}

	// -- Score each range

	type RangeWithScore struct {
		rID   uint64
		score uint64
		split string
	}

	scores := make([]RangeWithScore, len(ranges))

	for i, r := range ranges {
		var maxScore uint64
		var maxSplit string

		for _, p := range r.placements {
			s := p.info.Keys
			if s > maxScore {
				maxScore = s

				if len(p.info.Splits) > 0 {
					maxSplit = p.info.Splits[0]
				}
			}
		}

		scores[i] = RangeWithScore{
			rID:   r.rID,
			score: maxScore,
			split: maxSplit,
		}
	}

	// -- Find any ranges higher than the threshold, having splits, in descending order

	splits := []int{}

	for i, r := range scores {
		if r.split == "" {
			continue
		}
		if r.score > *splitScore {
			splits = append(splits, i)
		}
	}

	sort.Slice(splits, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	// -- Send RPCs (in parallel) to split each one

	var ops uint
	orchClient := pb.NewOrchestratorClient(conn)
	ctxSp, cancel := context.WithTimeout(ctx, 30*time.Second)
	wg := sync.WaitGroup{}

	for _, s := range splits {
		if *dryRun {
			fmt.Printf("Would split: range %d (score: %d) at %q\n", scores[s].rID, scores[s].score, scores[s].split)
			continue
		}
		if ops >= *num {
			continue
		}

		s := s
		ops += 1
		wg.Add(1)
		go func() {
			defer cancel()

			req := &pb.SplitRequest{
				Range:    scores[s].rID,
				Boundary: []byte(scores[s].split),
			}

			fmt.Printf("Splitting: range %d (score: %d)\n", scores[s].rID, scores[s].score)
			_, err := orchClient.Split(ctxSp, req)

			if err != nil {
				fmt.Printf("Error splitting range %d: %v\n", scores[s].rID, err)
				wg.Done()
				return
			}

			wg.Done()
		}()
	}

	wg.Wait()

	// -- Find any adjacent range pairs lower than the threshold, in ascending order

	joins := []int{}

	for i := 0; i < len(scores)-1; i++ {
		if scores[i].score+scores[i+1].score < *joinScore {
			joins = append(joins, i)
		}
	}

	sort.Slice(joins, func(i, j int) bool {
		return (scores[i].score + scores[i+1].score) < (scores[j].score + scores[j+1].score) // uhhh
	})

	// -- Send RPCs (in parallel) to join each pair

	ctxJo, cancel := context.WithTimeout(ctx, 30*time.Second)

	for _, j := range joins {
		left := scores[j].rID
		right := scores[j+1].rID
		score := scores[j].score + scores[j].score
		if *dryRun {
			fmt.Printf("Would join: range %d, range %d (combined score: %d)\n", left, right, score)
			continue
		}
		if ops >= *num {
			continue
		}

		ops += 1
		wg.Add(1)
		go func() {
			defer cancel()

			req := &pb.JoinRequest{
				RangeLeft:  left,
				RangeRight: right,
			}

			fmt.Printf("Joining: range %d, range %d (combined score: %d)\n", left, right, score)
			_, err := orchClient.Join(ctxJo, req)

			if err != nil {
				fmt.Printf("Error joining ranges %d and %d: %v\n", left, right, err)
				wg.Done()
				return
			}

			wg.Done()
		}()
	}

	wg.Wait()

	// -- Exit
}
