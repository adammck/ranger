package node

import (
	"context"
	"fmt"
	"log"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type src struct {
	meta ranje.Meta
	node string
}

type fetcher struct {
	meta ranje.Meta
	srcs []src
}

func newFetcher(rm ranje.Meta, parents []api.Parent) *fetcher {
	srcs := []src{}

	// If this is a range move, we can just fetch the whole thing from a single
	// node. Writes to that node will be disabled (via PrepareDropRange) before
	// the fetch occurs (via AddRange).

	for _, par := range parents {
		for _, plc := range par.Placements {
			if plc.State == ranje.PsReady {
				src := src{meta: par.Meta, node: plc.Node}
				srcs = append(srcs, src)
			}
		}
	}

	// TODO: Verify that the src ranges cover the entire dest range.

	switch len(srcs) {
	case 1:
		log.Printf("looks like a range move: %s", rm.Ident)
	case 2:
		log.Printf("looks like a split or join: %s", rm.Ident)
	default:
		log.Printf("no idea what's going on: %s (n=%d)", rm.Ident, len(srcs))
	}

	return &fetcher{
		meta: rm,
		srcs: srcs,
	}
}

func (f *fetcher) Fetch(dest *Range) error {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Fetch each source range in parallel.
	g, ctx := errgroup.WithContext(ctx)
	for i := range f.srcs {

		// lol, golang
		// https://golang.org/doc/faq#closures_and_goroutines
		i := i

		g.Go(func() error {
			return fetch(ctx, dest, f.meta, f.srcs[i].node, f.srcs[i].meta)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func fetch(ctx context.Context, dest *Range, meta ranje.Meta, addr string, src ranje.Meta) error {
	log.Printf("fetch: %s from: %s", src.Ident, addr)

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock())
	if err != nil {
		// TODO: Probably a bit excessive
		return fmt.Errorf("fetch failed: DialContext: %v", err)
	}

	defer conn.Close()
	client := pbkv.NewKVClient(conn)

	res, err := client.Dump(ctx, &pbkv.DumpRequest{RangeIdent: uint64(src.Ident)})
	if err != nil {
		log.Printf("fetch failed: Dump: %s (rID=%d, addr=%s)", err, src.Ident, addr)
		return err
	}

	load := 0
	skip := 0

	func() {
		// Hold lock for duration rather than flapping.
		dest.dataMu.Lock()
		defer dest.dataMu.Unlock()
		for _, pair := range res.Pairs {

			// Ignore any keys which are not in the destination range, since we
			// might be reading from a dump of a superset (if this is a join).
			if !meta.Contains(ranje.Key(pair.Key)) {
				skip += 1
				continue
			}

			dest.data[string(pair.Key)] = pair.Value
			load += 1
		}
	}()

	log.Printf("Inserted %d keys from range %s via node %s (skipped %d)", load, src.Ident, addr, skip)

	return nil
}
