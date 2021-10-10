package balancer

import (
	"fmt"
	"time"

	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc"
)

type Balancer struct {
	ks   *keyspace.Keyspace
	rost *roster.Roster
	srv  *grpc.Server
	bs   *balancerServer
}

func New(ks *keyspace.Keyspace, rost *roster.Roster, srv *grpc.Server) *Balancer {
	b := &Balancer{
		ks:   ks,
		rost: rost,
		srv:  srv,
	}

	// Register the gRPC server to receive instructions from operators. This
	// will hopefully not be necessary once balancing actually works!
	b.bs = &balancerServer{bal: b}
	pb.RegisterBalancerServer(srv, b.bs)

	return b
}

func (b *Balancer) Force(id *ranje.Ident, node string) error {
	r, err := b.ks.GetByIdent(*id)
	if err != nil {
		return err
	}

	// No validation here. The node might not exist yet, or have temporarily
	// gone away, or who knows what else.
	r.ForceNodeIdent = node

	return nil
}

func (b *Balancer) Rebalance() {
	fmt.Printf("rebalancing\n")
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Rebalance()
	}
}
