package proxy

import (
	"context"
	"log"
	"net"
	"sync"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/rangelet/mirror"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type Proxy struct {
	name      string
	addrLis   string
	addrPub   string // do we actually need this? maybe only discovery does.
	srv       *grpc.Server
	disc      discovery.Discoverer
	mirror    *mirror.Mirror
	clients   map[api.NodeID]pbkv.KVClient
	clientsMu sync.RWMutex

	// Options
	logReqs bool
}

func New(addrLis, addrPub string, logReqs bool) (*Proxy, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.NewDiscoverer(consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	mir := mirror.New(disc, func(ctx context.Context, rem discovery.Remote) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, rem.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	})

	p := &Proxy{
		name:    "proxy",
		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,
		mirror:  mir,
		clients: make(map[api.NodeID]pbkv.KVClient),
		logReqs: logReqs,
	}

	ps := proxyServer{proxy: p}
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

	// Make the proxy discoverable.
	// TODO: Do we actually need this? Clients can just call any proxy.
	// dget, err := p.disc.Discover("node")
	// if err != nil {
	// 	return err
	// }

	// Block until context is cancelled, indicating that caller wants shutdown.
	<-ctx.Done()

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by srv.Serve (above) or be closed with no error.
	p.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		log.Printf("error from srv.Serve: %v", err)
		return err
	}

	// Stop mirroring ranges.
	err = p.mirror.Stop()
	if err != nil {
		return err
	}

	return nil
}
