package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/adammck/ranger/pkg/actuator"
	rpc_actuator "github.com/adammck/ranger/pkg/actuator/rpc"
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

func New(addrLis, addrPub string, interval time.Duration, once bool) (*Controller, error) {
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
	ks, err := keyspace.New(pers)
	if err != nil {
		return nil, err
	}

	// TODO: Hook up the callbacks (or replace with channels)
	rost := roster.New(disc, nil, nil, nil)

	actImpl := rpc_actuator.New(ks, rost)
	act := actuator.New(ks, rost, time.Duration(3*time.Second), actImpl)

	orch := orchestrator.New(ks, rost, srv)

	return &Controller{
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
		c.act.Tick()

	} else {

		// Periodically probe all nodes to keep their state up to date.
		ticker := time.NewTicker(1 * time.Second)
		go c.rost.Run(ticker)

		// Start rebalancing loop.
		go c.orch.Run(time.NewTicker(c.interval))

		// Start incredibly actuation loop. The interval only affects how soon
		// it will notice a pending actuation. Failed actuations should retry
		// slower than this.
		go c.act.Run(time.NewTicker(500 * time.Millisecond))

		// Block until context is cancelled, indicating that caller wants
		// shutdown.
		if !c.once {
			<-ctx.Done()
		}
	}

	// Let in-flight commands finish. This isn't strictly necessary, but allows
	// us to minmize the stuff which will need reconciling at next startup.
	c.act.Wait()

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
