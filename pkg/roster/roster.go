package roster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
		fmt.Printf(" - %s\n", nid)
		n.DumpForDebug()
	}
}

//func (ros *Roster) NodeBy(opts... NodeByOpts)
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
			n = ranje.NewNode(r.Host, r.Port)
			fmt.Printf("new node: %v\n", r.Ident)
			ros.Nodes[r.Ident] = n
		}

		n.Seen(time.Now())
	}
}

func (ros *Roster) expire() {
	now := time.Now()

	for k, v := range ros.Nodes {
		if v.IsStale(now) {
			fmt.Printf("expiring node: %v\n", v)
			// TODO: Don't do this! Mark it as expired instead. There might still be ranges placed on it which need cleaning up.
			delete(ros.Nodes, k)
		}
	}
}

func (ros *Roster) probe() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	// Measure how long this takes and how many succeed.
	start := time.Now()
	var success uint64

	// TODO: We already do this in expire, bundle them together.
	for _, node := range ros.Nodes {
		wg.Add(1)

		// Copy node since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *ranje.Node) {
			defer wg.Done()
			err := n.Probe(ctx)
			if err != nil {
				fmt.Println(err)
				return
			}
			atomic.AddUint64(&success, 1)
		}(node)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Printf("probed %d nodes in %s\n", success, elapsed.String())
}

// Probe sends a heartbeat RPC to the given addr, and updates the node seen time.
//func (r *Roster) ProbeOne(n *ranje.Node) {
// 	//zap.L().Info("probe", zap.String("addr", n.addr))

// 	conn, err := n.Conn()
// 	if err != nil {
// 		// shouldn't happen except race; call Connected before Probe
// 		//zap.L().Error("error from n.Conn()", zap.Error(err))
// 		return
// 	}

// 	// todo: cache this stub?
// 	client := pb.NewAssigneeClient(conn)

// 	_, err = client.Status(context.Background(), &pb.StatusRequest{})
// 	if err != nil {
// 		// don't keep state, just let .seen get stale to indicate failure
// 		//zap.L().Info("error probing", zap.String("addr", n.addr), zap.Error(err))
// 		return
// 	}

// 	if n.seen.IsZero() {
// 		//zap.L().Info("new node responded to probe", zap.String("addr", n.addr))
// 	}

// 	// todo: inject clock
// 	n.seen = time.Now()
// }

// func (r *Roster) Tick() {
// 	//zap.L().Info("r.tick", zap.Int("nodes_len", len(r.nodes)))

// 	// todo: inject clock iface
// 	// todo: extract age limit (5s)
// 	// todo: jitter; older -> higher chance
// 	probeTime := time.Now().Add(time.Second * -2)

// 	// todo: anything but this
// 	r.mu.Lock()
// 	defer r.mu.Unlock()

// 	// send probes to stale nodes
// 	for _, n := range r.nodes {
// 		if n.seen.After(probeTime) {
// 			continue
// 		}

// 		if !n.Connected() {
// 			//zap.L().Info("skipping non-connected node", zap.String("addr", n.addr))
// 			continue
// 		}
// 		r.Probe(n)
// 	}

// 	doaTime := time.Now().Add(time.Second * -5)

// 	// drop nodes which haven't connected in time
// 	for _, n := range r.nodes {
// 		if n.seen.IsZero() && !n.init.After(doaTime) {
// 			//zap.L().Info("dropping doa node", zap.String("addr", n.addr))
// 			n.Drop()
// 			delete(r.nodes, n.addr)
// 		}
// 	}

// 	dropTime := time.Now().Add(time.Second * -10)

// 	// drop nodes which were probing but stopped
// 	for _, n := range r.nodes {
// 		if !n.seen.IsZero() && !n.seen.After(dropTime) {
// 			//zap.L().Info("dropping dead node", zap.String("addr", n.addr))
// 			n.Drop()
// 			delete(r.nodes, n.addr)
// 		}
// 	}
//}

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
