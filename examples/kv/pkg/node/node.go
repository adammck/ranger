package node

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
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

// ---- control plane

func (n *Node) PrepareAddShard(rm ranje.Meta) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.data[rm.Ident]
	if ok {
		// Special case: We already have this range, but gave up on fetching it.
		// To keep things simple, delete it. it'll be added again (while still
		// holding the lock) below.
		// if rd.state == state.NsPreparingError {
		// 	delete(n.node.data, rm.ident)
		// 	n.node.ranges.Remove(rm.ident)
		// }

		panic("rangelet gave duplicate range!")
	}

	// TODO: How to get parents in here?
	// if req.Parents != nil && len(req.Parents) > 0 {
	// 	rd.fetchMany(rm, req.Parents)
	// } else {
	// 	// TODO: Restore support for this:
	// 	// -- No current host nor parents. This is a brand new range. We're
	// 	// -- probably initializing a new empty keyspace.
	// 	// -- rd.state = state.NsReady
	// 	rd.state = state.NsPrepared
	// }

	n.data[rm.Ident] = &KeysVals{
		data:   map[string][]byte{},
		writes: false,
	}

	log.Printf("Given: %s", rm.Ident)
	return nil
}

func (n *Node) AddShard(rID ranje.Ident) error {
	// lol
	n.mu.Lock()
	defer n.mu.Unlock()

	kv, ok := n.data[rID]
	if !ok {
		panic("rangelet tried to serve unknown range!")
	}

	kv.writes = true

	log.Printf("Servng: %s", rID)
	return nil
}

func (n *Node) PrepareDropShard(rID ranje.Ident) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	kv, ok := n.data[rID]
	if !ok {
		panic("rangelet tried to serve unknown range!")
	}

	kv.writes = false

	log.Printf("Taking: %s", rID)
	return nil
}

// func (n *Node) Untake(ctx context.Context, req *pbr.UntakeRequest) (*pbr.UntakeResponse, error) {
// 	// lol
// 	s.node.mu.Lock()
// 	defer s.node.mu.Unlock()

// 	ident, rd, err := s.getRangeData(req.Range)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if rd.state != state.NsTaken {
// 		return nil, status.Error(codes.FailedPrecondition, "can only untake ranges in the TAKEN state")
// 	}

// 	rd.state = state.NsReady

// 	log.Printf("Untaken: %s", ident)
// 	return &pbr.UntakeResponse{}, nil
// }

func (n *Node) DropShard(rID ranje.Ident) error {
	// lol
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.data, rID)

	log.Printf("Dropped: %s", rID)
	return nil
}

// ---- data plane

type kvServer struct {
	pbkv.UnimplementedKVServer
	node *Node
}

func (s *kvServer) Dump(ctx context.Context, req *pbkv.DumpRequest) (*pbkv.DumpResponse, error) {
	ident := ranje.Ident(req.RangeIdent)
	if ident == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range_ident")
	}

	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	rd, ok := s.node.data[ident]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range not found")
	}

	if rd.writes {
		return nil, status.Error(codes.FailedPrecondition, "can only dump ranges while writes are disabled")
	}

	res := &pbkv.DumpResponse{}
	for k, v := range rd.data {
		res.Pairs = append(res.Pairs, &pbkv.Pair{Key: []byte(k), Value: v})
	}

	log.Printf("Dumped: %s", ident)
	return res, nil
}

func (s *kvServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.rglt.Find(ranje.Key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	v, ok := rd.data[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such key")
	}

	if s.node.logReqs {
		log.Printf("get %q", k)
	}

	return &pbkv.GetResponse{
		Value: v,
	}, nil
}

func (s *kvServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.rglt.Find(ranje.Key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	if !rd.writes {
		return nil, status.Error(codes.FailedPrecondition, "can only PUT to ranges unless writes are enabled")
	}

	if req.Value == nil {
		delete(rd.data, k)
	} else {
		rd.data[k] = req.Value
	}

	if s.node.logReqs {
		log.Printf("put %q", k)
	}

	return &pbkv.PutResponse{}, nil
}

func init() {
	// Ensure that nodeServer implements the NodeServer interface
	var ns *Node = nil
	var _ rangelet.Node = ns

	// Ensure that kvServer implements the KVServer interface
	var kvs *kvServer = nil
	var _ pbkv.KVServer = kvs

}

type NopStorage struct {
}

func (s *NopStorage) Read() []*info.RangeInfo {
	return nil
}

func (s *NopStorage) Write() {
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

	rglt := rangelet.NewRangelet(n, srv, &NopStorage{})
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
