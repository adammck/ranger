package controller

import (
	"fmt"
	"net"
	"time"

	"github.com/adammck/ranger/pkg/balancer"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Controller struct {
	name    string
	addrLis string
	addrPub string // do we actually need this? maybe only discovery does.
	srv     *grpc.Server
	disc    discovery.Discoverable
	ks      *ranje.Keyspace
	rost    *roster.Roster
	bal     *balancer.Balancer
}

func New(addrLis, addrPub string) (*Controller, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.New("controller", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	ks := ranje.New()
	rost := roster.New(disc)

	return &Controller{
		name:    "controller",
		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,
		ks:      ks,
		rost:    rost,
		bal:     balancer.New(ks, rost, srv),
	}, nil
}

// func (c *Controller) MainLoop() {

// 	ticker := time.NewTicker(5 * time.Second)

// 	go roster.Heartbeat(ticker)
// }

// func (c *Controller) Tick() {

// }

func (c *Controller) Run(done chan bool) error {

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

	// Make the controller discoverable.
	// TODO: Do we actually need this? Can move disc into Roster if not.
	err = c.disc.Start()
	if err != nil {
		return err
	}

	// TODO: Read assignment history from database
	// TODO: Connect to all nodes
	// TODO: Fetch current assignment status from nodes
	// TODO: Reconcile divergence etc

	// Start roster. Periodically probes all nodes to get their state.
	ticker := time.NewTicker(time.Second)
	go c.rost.Run(ticker)

	// Start rebalancing loop.
	// TODO: This should probably be reactive rather than running in a loop. Could run after probes complete.
	go c.bal.Run(time.NewTicker(3 * time.Second))

	// Dump range state periodically
	// TODO: Move this to a statusz type page
	go func() {
		t := time.NewTicker(time.Second)
		for ; true; <-t.C {
			fmt.Println("nodes:")
			c.rost.DumpForDebug()

			fmt.Println("ranges:")
			c.ks.DumpForDebug()

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

	// Remove ourselves from service discovery. Not strictly necessary, but lets
	// the other nodes respond quicker.
	err = c.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}
