package roster

import (
	"fmt"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
)

type Roster struct {
	nodes map[string]*Node
	mu    sync.Mutex

	disc discovery.Discoverable
}

func New(disc discovery.Discoverable) *Roster {
	return &Roster{
		nodes: make(map[string]*Node),
		disc:  disc,
	}
}

func (ros *Roster) discover() {
	res, err := ros.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		n, ok := ros.nodes[r.Ident]

		// New Node?
		if !ok {
			n = NewNode(r.Host, r.Port)
			fmt.Printf("new node: %v\n", r.Ident)
			ros.nodes[r.Ident] = n
		}

		n.seen = time.Now()
	}
}

func (ros *Roster) expire() {
	staleTime := time.Now().Add(-10 * time.Second)

	for k, v := range ros.nodes {
		if v.seen.Before(staleTime) {
			fmt.Printf("expiring node: %v\n", v)
			delete(ros.nodes, k)
		}
	}
}

func (ros *Roster) probe() {
	// TODO: We already do this in expire, bundle them together.
	for _, node := range ros.nodes {
		ros.ProbeOne(node)
	}
}

// Probe sends a heartbeat RPC to the given addr, and updates the node seen time.
func (r *Roster) ProbeOne(n *Node) {
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
}

func (r *Roster) Tick() {
	// TODO: anything but this
	r.mu.Lock()
	defer r.mu.Unlock()

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
	//zap.L().Info("returning from Heartbeat")
}
