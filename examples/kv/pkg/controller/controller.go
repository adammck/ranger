package controller

import (
	"fmt"
	"net"

	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
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

	return &Controller{
		name:    "controller",
		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,
	}, nil
}

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
	// TODO: Do we actually need this?
	err = c.disc.Start()
	if err != nil {
		return err
	}

	//???
	listNodes(c)

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

// TODO: Remove this
func listNodes(c *Controller) {
	res, err := c.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		fmt.Printf("node: %v\n", r)
	}
}
