package node

import (
	"context"
	"fmt"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/api"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type src struct {
	meta api.Meta
	node string
}

type fetcher struct {
	meta api.Meta
	srcs []src
}

func newFetcher(rm api.Meta, parents []api.Parent) *fetcher {
	srcs := []src{}

	// If this is a range move, we can just fetch the whole thing from a single
	// node. Writes to that node will be disabled (via Deactivate) before the
	// fetch occurs (via Activate).

	for _, par := range parents {
		for _, plc := range par.Placements {
			if plc.State == api.PsActive {
				src := src{meta: par.Meta, node: plc.Node}
				srcs = append(srcs, src)
			}
		}
	}

	// TODO: Verify that the src ranges cover the entire dest range.

	return &fetcher{
		meta: rm,
		srcs: srcs,
	}
}

func (f *fetcher) Fetch(dest *Range) error {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

func fetch(ctx context.Context, dest *Range, meta api.Meta, addr string, src api.Meta) error {
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		// TODO: Probably a bit excessive
		return fmt.Errorf("fetch failed: DialContext: %v", err)
	}

	defer conn.Close()
	client := pbkv.NewKVClient(conn)

	res, err := client.Dump(ctx, &pbkv.DumpRequest{RangeIdent: uint64(src.Ident)})
	if err != nil {
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
			if !meta.Contains(api.Key(pair.Key)) {
				skip += 1
				continue
			}

			dest.data[string(pair.Key)] = pair.Value
			load += 1
		}
	}()

	return nil
}
