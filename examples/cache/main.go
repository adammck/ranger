package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/rangelet/storage/null"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
)

type Runner interface {
	Run(ctx context.Context) error
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
	addrGRPC := flag.String("grpc", "localhost:8000", "grpc server")
	addrHTTP := flag.String("http", "localhost:8001", "http server")
	flag.Parse()

	// Replace default logger.
	log.Default().SetOutput(os.Stdout)
	log.Default().SetPrefix("")
	log.Default().SetFlags(0)

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	var cmd Runner
	var err error

	cmd, err = NewNode(*addrGRPC, *addrHTTP)

	if err != nil {
		exit(err)
	}

	err = cmd.Run(ctx)
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	log.Fatalf("Error: %s", err)
}

// ----

type Node struct {
	addrGRPC string
	addrHTTP string

	// Ranger stuff
	gsrv *grpc.Server
	rglt *rangelet.Rangelet
	disc discovery.Discoverable

	// Cache stuff
	hsrv  *http.Server
	cache map[string][32]byte
}

func NewNode(addrGRPC, addrHTTP string) (*Node, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	disc, err := consuldisc.New("node", addrGRPC, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	n := &Node{
		addrGRPC: addrGRPC,
		addrHTTP: addrHTTP,
		gsrv:     srv,
		disc:     disc,
		cache:    map[string][32]byte{},
	}

	n.rglt = rangelet.NewRangelet(n, srv, &null.NullStorage{})

	n.hsrv = &http.Server{
		Addr:    addrHTTP,
		Handler: n,
	}

	return n, nil
}

func (n *Node) Run(ctx context.Context) error {

	// For the gRPC server.
	lisg, err := net.Listen("tcp", n.addrGRPC)
	if err != nil {
		return err
	}

	log.Printf("grpc listening on: %s", n.addrGRPC)

	// For the HTTP server
	lish, err := net.Listen("tcp", n.addrHTTP)
	if err != nil {
		return err
	}

	log.Printf("http listening on: %s", n.addrHTTP)

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := n.gsrv.Serve(lisg)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Register with service discovery, so rangerd can find us.
	err = n.disc.Start()
	if err != nil {
		return err
	}

	go func() {
		n.hsrv.Serve(lish)
	}()

	// Block until context is cancelled, indicating that caller wants shutdown.
	<-ctx.Done()

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by srv.Serve (above) or be closed with no error.
	n.gsrv.GracefulStop()
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

	// Gracefully stop the HTTP server.
	ctx2, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	n.hsrv.Shutdown(ctx2)
	cancel()

	return nil
}

// Control plane

func (n *Node) GetLoadInfo(rID api.RangeID) (api.LoadInfo, error) {
	return api.LoadInfo{}, nil
}

func (n *Node) PrepareAddRange(rm api.Meta, parents []api.Parent) error {
	return nil
}

func (n *Node) AddRange(rID api.RangeID) error {
	return nil
}

func (n *Node) PrepareDropRange(rID api.RangeID) error {
	return nil
}

func (n *Node) DropRange(rID api.RangeID) error {
	return nil
}

// Data plane

func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	k := r.URL.Path
	k = strings.TrimLeft(k, "/")

	_, ok := n.rglt.Find(api.Key(k))
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "not found.")
		return
	}

	w.WriteHeader(http.StatusOK)

	// TODO: Block if same key is being concurrently hashed.
	h, ok := n.cache[k]
	if !ok {
		h = hash(k)
		n.cache[k] = h
	}

	fmt.Fprintln(w, h)
}

func hash(input string) [32]byte {
	x := sha256.Sum256([]byte(input))

	for i := 0; i < 10_000_000; i++ {
		x = sha256.Sum256(x[:])
	}

	return x
}
