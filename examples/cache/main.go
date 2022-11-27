package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/adammck/ranger/pkg/rangelet/mirror"
	"github.com/adammck/ranger/pkg/rangelet/storage/null"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	mir  *mirror.Mirror

	// Cache stuff
	hsrv  *http.Server
	cache map[string][32]byte
}

func NewNode(addrGRPC, addrHTTP string) (*Node, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// To make ourselves discoverable by the controller and other nodes. This is
	// unnecessary in environments with ambient service discovery.
	disc, err := consuldisc.New("node", addrGRPC, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	// The mirror watches range assignments (to any node) so that this node can
	// handle any request, not just those contained by ranges assigned to it.
	// For those not assigned, the mirror can be queried to find the relevant
	// nodes, and forward the request there. This allows clients to send their
	// requests to any arbitrary node, and not have to perform range lookup
	// themselves. This is especially useful for short-lived clients.
	discoverer, err := consuldisc.NewDiscoverer(consulapi.DefaultConfig())
	if err != nil {
		return nil, err
	}
	mir := mirror.New(discoverer).WithDialler(func(ctx context.Context, rem api.Remote) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, rem.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	})

	n := &Node{
		addrGRPC: addrGRPC,
		addrHTTP: addrHTTP,
		gsrv:     srv,
		disc:     disc,
		mir:      mir,
		cache:    map[string][32]byte{},
	}

	n.rglt = rangelet.New(n, srv, &null.NullStorage{})

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
		err := n.hsrv.Serve(lish)
		if err != nil && err != http.ErrServerClosed {
			log.Printf("error from hsrv.Serve: %v", err)
		}
	}()

	// Block until context is cancelled, indicating that caller wants shutdown.
	<-ctx.Done()

	// Terminate in-flight RPCs and stop the gRPC server. errChan will contain
	// the error returned by gsrv.Serve (above) or be closed with no error.
	n.gsrv.Stop()
	err = <-errChan
	if err != nil {
		log.Printf("error from gsrv.Serve: %v", err)
		return err
	}

	// Gracefully stop the HTTP server.
	ctx2, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	n.hsrv.Shutdown(ctx2)
	cancel()

	// Stop range assignement mirror. It's useless now that we are not receiving
	// any more requests.
	err = n.mir.Stop()
	if err != nil {
		log.Printf("error from Mirror.Stop: %v", err)
	}

	// Remove ourselves from service discovery. Not strictly necessary, but lets
	// the other nodes respond quicker versus us disappearing and timing out.
	err = n.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}

// Control plane

func (n *Node) GetLoadInfo(rID api.RangeID) (api.LoadInfo, error) {
	return api.LoadInfo{}, nil
}

func (n *Node) Prepare(rm api.Meta, parents []api.Parent) error {
	return nil
}

func (n *Node) AddRange(rID api.RangeID) error {
	return nil
}

func (n *Node) Deactivate(rID api.RangeID) error {
	return nil
}

func (n *Node) Drop(rID api.RangeID) error {
	return nil
}

// Data plane

func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	k := r.URL.Path
	k = strings.TrimLeft(k, "/")
	ak := api.Key(k)

	_, ok := n.rglt.Find(ak)
	if !ok {
		// The key is not assigned to this node, so we will helpfully redirect
		// the caller to a node which it *is* assigned to, which we know via the
		// range assignment mirror.
		res := n.mir.Find(ak, api.NsActive)
		if len(res) == 0 {

			// No results! The range is either unassigned or our mirror is out
			// of date. Either way, return an error so the client can retry
			// against a different node.
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "not found.")
			return
		}

		// Construct the HTTP (data-plane) address fo the node which we believe
		// the key is assigned to, and redirect to it. The caller might follow
		// this automatically. We could alteratively proxy the request to the
		// relevant node (like TCube cacheservers did/do), but this is just a
		// simple demo.
		url := fmt.Sprintf("http://%s/%s", dataAddr(res[0].Remote), k)
		http.Redirect(w, r, url, http.StatusFound)
		return
	}

	// TODO: Block if same key is being concurrently hashed.
	h, ok := n.cache[k]
	if !ok {
		h = hash(k)
		n.cache[k] = h
	}

	w.Header().Set("Server", n.hsrv.Addr)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, hex.EncodeToString(h[:]))
}

// hash performs a lot of useless work, and then returns a hash of the given
// input string. This cache example uses this to represent some expensive but
// arbitrary data which benefits from being cached.
func hash(input string) [32]byte {
	x := sha256.Sum256([]byte(input))

	for i := 0; i < 10_000_000; i++ {
		x = sha256.Sum256(x[:])
	}

	return x
}

// dataAddr returns the remote addr for the given remote, but with the leading 1
// of the port replaced with a 2. This is of course a terrible hack! But rangerd
// has no clue about the port conventions of this cache service, and the mirror
// currently provides no way to smuggle that info across the wire.
//
// Ideally the rangelet would accept some arbitrary blob of bytes (like an empty
// interface, but serializable) which could be included with responses to probes
// and range assignments, like "extra info about this node". Obviously services
// would have to deserialize it themselves.
//
// In the case of this service, the extra payload would be simply:
//   type CacheNodeInfo struct {
// 	   dataPort int
//   }
//
// More complex services may smuggle signals like ingestion or replication lag,
// which clients then use to decide which result to use, although those examples
// sound more like per-range info than per-node...
//
// Services can already, of course, provide endpoints to exchange info entirely
// outside of ranger. But it feels unnecessarily boiler-platey to provide a gRPC
// endpoint just to ask "what is your HTTP port?".
func dataAddr(rem api.Remote) string {
	cp := fmt.Sprintf("%d", rem.Port)
	if cp[0] != '1' {
		panic(fmt.Sprintf("not a control plane port: %d", rem.Port))
	}

	return fmt.Sprintf("%s:2%s", rem.Host, cp[1:])
}
