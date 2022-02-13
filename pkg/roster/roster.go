package roster

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/ranje"
)

const (
	probeTimeout = 3 * time.Second
)

type Roster struct {
	// Public so Balancer can read the Nodes
	// node ident (not ranje.Ident!!) -> Node
	Nodes map[string]*ranje.Node
	sync.RWMutex

	disc discovery.Discoverable
}

func New(disc discovery.Discoverable) *Roster {
	return &Roster{
		Nodes: make(map[string]*ranje.Node),
		disc:  disc,
	}
}

// TODO: Replace this with a statusz-type page
func (ros *Roster) DumpForDebug() {
	ros.RLock()
	defer ros.RUnlock()

	for nid, n := range ros.Nodes {
		log.Printf(" - %s", nid)
		n.DumpForDebug()
	}
}

//func (ros *Roster) NodeBy(opts... NodeByOpts)
// TODO: Return an error from this func, to avoid duplicating it in callers.
func (ros *Roster) NodeByIdent(nodeIdent string) *ranje.Node {
	ros.RLock()
	defer ros.RUnlock()

	for nid, n := range ros.Nodes {
		if nid == nodeIdent {
			return n
		}
	}

	return nil
}

func (ros *Roster) discover() {
	res, err := ros.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		n, ok := ros.Nodes[r.Ident]

		// New Node?
		if !ok {
			n = ranje.NewNode(r)
			log.Printf("new node: %v", n.Ident())
			ros.Nodes[r.Ident] = n
		}

		n.Seen(time.Now())
	}
}

func (ros *Roster) expire() {
	now := time.Now()

	for k, v := range ros.Nodes {
		if v.IsStale(now) {
			log.Printf("expiring node: %v", v)
			// TODO: Don't do this! Mark it as expired instead. There might still be ranges placed on it which need cleaning up.
			delete(ros.Nodes, k)
		}
	}
}

func (ros *Roster) probe() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	// TODO: We already do this in expire, bundle them together.
	for _, node := range ros.Nodes {
		wg.Add(1)

		// Copy node since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *ranje.Node) {
			defer wg.Done()
			err := n.Probe(ctx)
			if err != nil {
				log.Printf("Error probing %v: %v", n.Ident(), err)
				return
			}
		}(node)
	}

	wg.Wait()
}

func (r *Roster) Tick() {
	// TODO: anything but this
	r.Lock()
	defer r.Unlock()

	// Grab any new nodes from service discovery.
	r.discover()

	// Expire any nodes that have gone missing service discovery.
	r.expire()

	r.probe()
}

func (r *Roster) Run(t *time.Ticker) {
	for ; true; <-t.C {
		r.Tick()
	}
}
