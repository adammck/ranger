package proxy

import (
	"context"
	"log"
	"net"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/rangelet/mirror"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type Proxy struct {
	name    string
	addrLis string
	addrPub string // do we actually need this? maybe only discovery does.
	srv     *grpc.Server
	disc    discovery.Discoverer
	mirror  *mirror.Mirror

	// Options
	logReqs bool
}

func New(addrLis, addrPub string, logReqs bool) (*Proxy, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.NewDiscoverer(consulapi.DefaultConfig())
	if err != nil {
		return nil, err
	}

	mir := mirror.New(disc).WithDialler(func(ctx context.Context, rem api.Remote) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, rem.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	})

	p := &Proxy{
		name:    "proxy",
		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,
		mirror:  mir,
		logReqs: logReqs,
	}

	ps := proxyServer{
		proxy: p,
	}
	pbkv.RegisterKVServer(srv, &ps)

	return p, nil
}

func (p *Proxy) Run(ctx context.Context) error {

	// For the gRPC server.
	lis, err := net.Listen("tcp", p.addrLis)
	if err != nil {
		return err
	}

	log.Printf("listening on: %s", p.addrLis)

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := p.srv.Serve(lis)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Block until context is cancelled, indicating that caller wants shutdown.
	<-ctx.Done()

	// Stop mirroring ranges. This isn't necessary, just cleanup.
	err = p.mirror.Stop()
	if err != nil {
		return err
	}

	// Let in-flight RPCs finish and then stop serving. errChan will contain the
	// error returned by srv.Serve (see above) or be closed with no error.
	p.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		log.Printf("error from srv.Serve: %v", err)
		return err
	}

	return nil
}

func (p *Proxy) getClient(k string) (pbkv.KVClient, mirror.Result, error) {
	results := p.mirror.Find(api.Key(k), api.NsActive)
	res := mirror.Result{}

	if len(results) == 0 {
		return nil, res, status.Errorf(
			codes.FailedPrecondition,
			"no nodes have key")
	}

	// Just pick the first one for now.
	// TODO: Pick a random one? Should the server-side shuffle them?
	res = results[0]

	conn, ok := p.mirror.Conn(res.NodeID())
	if !ok {
		// This should not happen.
		return nil, res, status.Errorf(
			codes.FailedPrecondition,
			"no client connection for node id %s", res.NodeID())
	}

	// We could cache it, but constructing clients is cheap.
	return pbkv.NewKVClient(conn), res, nil
}
