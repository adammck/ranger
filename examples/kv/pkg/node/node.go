package node

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/rangelet/storage/null"
	"github.com/adammck/ranger/pkg/ranje"
	consulapi "github.com/hashicorp/consul/api"
)

// func (rd *RangeData) fetchMany(dest ranje.Meta, parents []*pbr.Placement) {

// 	// Parse all the parents before spawning threads. This is fast and failure
// 	// indicates a bug more than a transient problem.
// 	rms := make([]*ranje.Meta, len(parents))
// 	for i, p := range parents {
// 		rms[i] = &rm
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
// 	defer cancel()

// 	mu := sync.Mutex{}

// 	// Fetch each source range in parallel.
// 	g, ctx := errgroup.WithContext(ctx)
// 	for i := range parents {

// 		// Ignore ranges which aren't currently assigned to any node. This kv
// 		// example doesn't have an external write log, so if it isn't placed,
// 		// there's nothing we can do with it.
// 		//
// 		// This includes ranges which are being placed for the first time, which
// 		// includes new split/join ranges! That isn't an error. Their state is
// 		// reassembled from their parents.
// 		if parents[i].Node == "" {
// 			//log.Printf("not fetching unplaced range: %s", rms[i].ident)
// 			continue
// 		}

// 		// lol, golang
// 		// https://golang.org/doc/faq#closures_and_goroutines
// 		i := i

// 		g.Go(func() error {
// 			return rd.fetchOne(ctx, &mu, dest, parents[i].Node, rms[i])
// 		})
// 	}

// 	if err := g.Wait(); err != nil {
// 		return err
// 		return
// 	}

// 	// Can't go straight into rsReady, because that allows writes. The source
// 	// node(s) are still serving reads, and if we start writing, they'll be
// 	// wrong. We can only serve reads until the assigner tells them to stop,
// 	// which will redirect all reads to us. Then we can start writing.
// 	rd.state = state.NsPrepared
// }

// func (rd *RangeData) fetchOne(ctx context.Context, mu *sync.Mutex, dest ranje.Meta, addr string, src *ranje.Meta) error {
// 	if addr == "" {
// 		log.Printf("FetchOne: %s with no addr", src.ident)
// 		return nil
// 	}

// 	log.Printf("FetchOne: %s from: %s", src.ident, addr)

// 	conn, err := grpc.DialContext(
// 		ctx,
// 		addr,
// 		grpc.WithInsecure(),
// 		grpc.WithBlock())
// 	if err != nil {
// 		// TODO: Probably a bit excessive
// 		log.Fatalf("fail to dial: %v", err)
// 	}

// 	client := pbkv.NewKVClient(conn)

// 	res, err := client.Dump(ctx, &pbkv.DumpRequest{RangeIdent: uint64(src.ident)})
// 	if err != nil {
// 		log.Printf("FetchOne failed: %s from: %s: %s", src.ident, addr, err)

// 		return err
// 	}

// 	// TODO: Optimize loading by including range start and end in the Dump response. If they match, can skip filtering.

// 	c := 0
// 	s := 0
// 	mu.Lock()
// 	for _, pair := range res.Pairs {

// 		// TODO: Untangle []byte vs string mess
// 		if dest.Contains(pair.Key) {
// 			rd.data[string(pair.Key)] = pair.Value
// 			c += 1
// 		} else {
// 			s += 1
// 		}
// 	}
// 	mu.Unlock()
// 	log.Printf("Inserted %d keys from range %s via node %s (skipped %d)", c, src.ident, addr, s)

// 	return nil
// }

type KeysVals struct {
	data   map[string][]byte
	writes bool
}

type Node struct {
	cfg config.Config

	data map[ranje.Ident]*KeysVals
	mu   sync.RWMutex // guards data

	addrLis string
	addrPub string
	srv     *grpc.Server
	disc    discovery.Discoverable

	rglt *rangelet.Rangelet

	// Options
	logReqs bool
}

func init() {
	// Ensure that nodeServer implements the NodeServer interface
	var ns *Node = nil
	var _ rangelet.Node = ns
}

func New(cfg config.Config, addrLis, addrPub string, logReqs bool) (*Node, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.New("node", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:     cfg,
		data:    map[ranje.Ident]*KeysVals{},
		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,

		logReqs: logReqs,
	}

	rglt := rangelet.NewRangelet(n, srv, &null.NullStorage{})
	n.rglt = rglt

	kv := kvServer{node: n}
	pbkv.RegisterKVServer(srv, &kv)

	return n, nil
}

func (n *Node) Run(ctx context.Context) error {

	// For the gRPC server.
	lis, err := net.Listen("tcp", n.addrLis)
	if err != nil {
		return err
	}

	log.Printf("listening on: %s", n.addrLis)

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := n.srv.Serve(lis)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Register with service discovery
	err = n.disc.Start()
	if err != nil {
		return err
	}

	// Block until context is cancelled, indicating that caller wants shutdown.
	<-ctx.Done()

	// We're shutting down. First drain all of the ranges off this node (which
	// may take a while and involve a bunch of RPCs.) If this is disabled, the
	// node will just disappear, and the controller will wait until it expires
	// before assigning the range to other nodes.
	if n.cfg.DrainNodesBeforeShutdown {
		n.DrainRanges()
	} else {
		log.Printf("not draining ranges")
	}

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by srv.Serve (above) or be closed with no error.
	n.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		log.Printf("error from srv.Serve: %v", err)
		return err
	}

	// Remove ourselves from service discovery. Not strictly necessary, but lets
	// the other nodes respond quicker.
	err = n.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}

// TODO: Move this to rangelet?
func (n *Node) DrainRanges() {
	log.Printf("draining ranges...")

	// This is included in probe responses. The next time the controller probes
	// this node, it will notice that the node wants to drain (probably because
	// it's shutting down), and start removing ranges.
	n.rglt.SetWantDrain(true)

	// Log the number of ranges remaining every five seconds while waiting.

	tick := time.NewTicker(5 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				c := n.rglt.Len()
				log.Printf("ranges remaining: %d", c)
			}
		}
	}()

	// Block until the number of ranges hits zero.

	for {
		if c := n.rglt.Len(); c == 0 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Stop the logger.

	tick.Stop()
	done <- true

	log.Printf("finished draining ranges.")
}
