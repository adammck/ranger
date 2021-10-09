package balancer

import (
	"fmt"
	"time"

	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/roster"
)

type Balancer struct {
	ks   *keyspace.Keyspace
	rost *roster.Roster
}

func New(ks *keyspace.Keyspace, rost *roster.Roster) *Balancer {
	return &Balancer{ks, rost}
}

func (b *Balancer) Rebalance() {
	fmt.Printf("rebalancing\n")
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Rebalance()
	}
}
