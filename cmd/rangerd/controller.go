package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/adammck/ranger/pkg/actuator"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/orchestrator"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	consulpers "github.com/adammck/ranger/pkg/persister/consul"
	consulapi "github.com/hashicorp/consul/api"
)

type Controller struct {
	cfg config.Config

	addrLis  string
	addrPub  string // do we actually need this? maybe only discovery does.
	interval time.Duration
	once     bool // run one rebalance cycle and exit

	srv  *grpc.Server
	disc discovery.Discoverable
	ks   *keyspace.Keyspace
	rost *roster.Roster
	act  *actuator.Actuator
	orch *orchestrator.Orchestrator
}

func New(cfg config.Config, addrLis, addrPub string, interval time.Duration, once bool) (*Controller, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	// TODO: Pass in the Consul client here.
	disc, err := consuldisc.New("controller", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	api, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		return nil, err
	}

	pers := consulpers.New(api)

	// This loads the ranges from storage, so will fail if the persister (e.g.
	// Consul) isn't available. Starting with an empty keyspace should be rare.
	ks, err := keyspace.New(cfg, pers)
	if err != nil {
		return nil, err
	}

	// TODO: Hook up the callbacks (or replace with channels)
	rost := roster.New(cfg, disc, nil, nil, nil)

	act := actuator.New(ks, rost)

	orch := orchestrator.New(cfg, ks, rost, act, srv)

	return &Controller{
		cfg:      cfg,
		addrLis:  addrLis,
		addrPub:  addrPub,
		interval: interval,
		once:     once,
		srv:      srv,
		disc:     disc,
		ks:       ks,
		rost:     rost,
		act:      act,
		orch:     orch,
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

	// Wait a bit for other services to come up before starting. This makes
	// development easier my minimizing log spam, and is no big deal in prod.
	time.Sleep(1 * time.Second)

	// Perform a single blocking probe cycle, to ensure that the first rebalance
	// happens after we have the current state of the nodes.
	c.rost.Tick()

	if c.once {
		c.orch.Tick()

	} else {

		// Periodically probe all nodes to keep their state up to date.
		ticker := time.NewTicker(1 * time.Second)
		go c.rost.Run(ticker)

		// Start rebalancing loop.
		go c.orch.Run(time.NewTicker(c.interval))

		// Block until context is cancelled, indicating that caller wants
		// shutdown.
		if !c.once {
			<-ctx.Done()
		}
	}

	// Let in-flight outgoing RPCs finish. (This isn't necessary, but allows
	// us to persist remote state now rather than at next startup.)
	c.act.WaitRPCs()

	// Let in-flight incoming RPCs finish and then stop. errChan will contain
	// the error returned by srv.Serve (above) or be closed with no error.
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
