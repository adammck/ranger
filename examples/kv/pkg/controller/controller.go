package controller

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/adammck/ranger/pkg/balancer"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/ranje"
	consulpers "github.com/adammck/ranger/pkg/ranje/persisters/consul"
	"github.com/adammck/ranger/pkg/roster"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Controller struct {
	name    string
	addrLis string
	addrPub string // do we actually need this? maybe only discovery does.
	once    bool   // run one rebalance cycle and exit

	srv  *grpc.Server
	disc discovery.Discoverable
	ks   *ranje.Keyspace
	rost *roster.Roster
	bal  *balancer.Balancer
}

func New(addrLis, addrPub string, once bool) (*Controller, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	api, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		return nil, err
	}

	// TODO: Pass in the Consul client here.
	disc, err := consuldisc.New("controller", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	pers := consulpers.New(api)
	ks := ranje.New(pers)

	// TODO: Hook up the callbacks (or replace with channels)
	rost := roster.New(disc, nil, nil)

	return &Controller{
		name:    "controller",
		addrLis: addrLis,
		addrPub: addrPub,
		once:    once,
		srv:     srv,
		disc:    disc,
		ks:      ks,
		rost:    rost,
		bal:     balancer.New(ks, rost, srv),
	}, nil
}

func (c *Controller) Run(ctx context.Context) error {

	// For the gRPC server.
	lis, err := net.Listen("tcp", c.addrLis)
	if err != nil {
		return err
	}

	log.Printf("listening on: %s", c.addrLis)

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := c.srv.Serve(lis)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Make the controller discoverable.
	// TODO: Do we actually need this? Can move disc into Roster if not.
	err = c.disc.Start()
	if err != nil {
		return err
	}

	// TODO: Fetch current assignment status from nodes
	// TODO: Reconcile divergence etc

	// Perform a single blocking probe cycle, to ensure that the first rebalance
	// happens after we have the current state of the nodes.
	c.rost.Tick()

	if !c.once {
		// Periodically probe all nodes to keep their state up to date.
		ticker := time.NewTicker(time.Second)
		go c.rost.Run(ticker)
	}

	if c.once {
		c.bal.Tick()
		c.bal.FinishOps()

	} else {

		// Start rebalancing loop.
		// TODO: This should probably be reactive rather than running in a loop. Could run after probes complete.
		go c.bal.Run(time.NewTicker(1005 * time.Millisecond))
	}

	// If we're staying active (not --once), block until context is cancelled,
	// indicating that caller wants shutdown.
	if !c.once {
		<-ctx.Done()
	}

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by srv.Serve (above) or be closed with no error.
	c.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		log.Printf("Error from srv.Serve: ")
		return err
	}

	// Remove ourselves from service discovery. Not strictly necessary, but lets
	// the other nodes respond quicker.
	err = c.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}
