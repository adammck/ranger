package proxy

import (
	"context"
	"fmt"
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
		fmt.Printf("error while dialing %s: %v\n", rem.Addr(), err)
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
		fmt.Printf("tried to remove non-existent client %s\n", rem.Ident)
		return
	}

	delete(p.clients, rem.Ident)
}

func (c *Proxy) Run(done chan bool) error {

	// For the gRPC server.
	lis, err := net.Listen("tcp", c.addrLis)
	if err != nil {
		return err
	}

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := c.srv.Serve(lis)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Make the proxy discoverable.
	// TODO: Do we actually need this? Clients can just call any proxy.
	err = c.disc.Start()
	if err != nil {
		return err
	}

	// Start roster. Periodically asks all nodes for their ranges.
	ticker := time.NewTicker(1005 * time.Millisecond)
	go c.rost.Run(ticker)

	// Dump range state periodically
	// TODO: Move this to a statusz type page
	go func() {
		t := time.NewTicker(1 * time.Second)
		for ; true; <-t.C {
			fmt.Print("\033[H\033[2J")
			fmt.Println("roster:")
			c.rost.DumpForDebug()
		}
	}()

	// Block until channel closes, indicating that caller wants shutdown.
	<-done

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by server.Serve (above) or be closed with no error.
	c.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		fmt.Printf("Error from server.Serve: ")
		return err
	}

	// Remove ourselves from service discovery.
	err = c.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}