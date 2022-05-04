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
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/rangelet/storage/null"
	"github.com/adammck/ranger/pkg/ranje"
	consulapi "github.com/hashicorp/consul/api"
)

type Range struct {
	data     map[string][]byte
	dataMu   sync.RWMutex // guards data
	fetcher  *fetcher
	writable uint32 // semantically bool, but uint so we can read atomically
}

type Node struct {
	cfg config.Config

	ranges   map[ranje.Ident]*Range
	rangesMu sync.RWMutex // guards ranges

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
	var _ api.Node = ns
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
		ranges:  map[ranje.Ident]*Range{},
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
