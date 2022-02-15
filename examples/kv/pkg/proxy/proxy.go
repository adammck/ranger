package proxy

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/roster2"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Proxy struct {
	name    string
	addrLis string
	addrPub string // do we actually need this? maybe only discovery does.
	srv     *grpc.Server
	disc    discovery.Discoverable
	rost    *roster2.Roster2

	clients   map[string]pbkv.KVClient
	clientsMu sync.RWMutex
}

func New(addrLis, addrPub string) (*Proxy, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.New("proxy", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	p := &Proxy{
		name:    "proxy",
		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,
		rost:    nil,
		clients: make(map[string]pbkv.KVClient),
	}

	p.rost = roster2.New(disc, p.Add, p.Remove)

	ps := proxyServer{proxy: p}
	pbkv.RegisterKVServer(srv, &ps)

	return p, nil
}

func (p *Proxy) Add(rem *discovery.Remote) {
	conn, err := grpc.DialContext(context.Background(), rem.Addr(), grpc.WithInsecure())

	// TODO: Not sure what to do here.
	if err != nil {
		log.Printf("error while dialing %s: %v", rem.Addr(), err)
		return
	}

	p.clientsMu.Lock()
	defer p.clientsMu.Unlock()

	p.clients[rem.Ident] = pbkv.NewKVClient(conn)
}

func (p *Proxy) Remove(rem *discovery.Remote) {
	p.clientsMu.Lock()
	defer p.clientsMu.Unlock()

	// Check first to make debugging easier.
	// Could just call delete and ignore no-ops.

	_, ok := p.clients[rem.Ident]
	if !ok {
		log.Printf("tried to remove non-existent client %s", rem.Ident)
		return
	}

	delete(p.clients, rem.Ident)
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
	err = p.disc.Start()
	if err != nil {
		return err
	}

	// Start roster. Periodically asks all nodes for their ranges.
	ticker := time.NewTicker(1005 * time.Millisecond)
	go p.rost.Run(ticker)

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

	// Remove ourselves from service discovery.
	err = p.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}
