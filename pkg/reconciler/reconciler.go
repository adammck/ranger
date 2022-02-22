package reconciler

import (
	"log"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Reconciler struct {
	ks *ranje.Keyspace
	ch chan roster.NodeInfo
}

func New(ks *ranje.Keyspace) *Reconciler {
	ch := make(chan roster.NodeInfo)
	return &Reconciler{ks, ch}
}

func (r *Reconciler) Chan() chan roster.NodeInfo {
	return r.ch
}

func (r *Reconciler) Run() {
	for nInfo := range r.ch {
		err := r.nodeInfo(nInfo)
		if err != nil {
			log.Printf("err! %v\n", err)
		}
	}
}

type states struct {
	expected *ranje.PBNID
	actual   *roster.RangeInfo
}

func (s *states) hasExpected() bool {
	return s.expected != nil
}

func (s *states) hasActual() bool {
	return s.actual != nil
}

func (r *Reconciler) nodeInfo(nInfo roster.NodeInfo) error {
	states := map[ranje.Ident]states{}

	// Collect the placement we *expect* the node to have.
	for _, pbnid := range r.ks.PlacementsByNodeID(nInfo.NodeID) {
		rID := pbnid.Range.Meta.Ident
		r := states[rID]
		r.expected = &pbnid
		states[rID] = r
	}

	// Collect the placements that the node reports that it has.
	for _, rInfo := range nInfo.Ranges {
		rID := rInfo.Meta.Ident
		r := states[rID]
		r.actual = &rInfo
		states[rID] = r
	}

	for rID, pair := range states {
		if pair.hasExpected() {
			if pair.hasActual() {
				if pair.expected.Placement.State != pair.actual.State.ToStatePlacement() {
					err := r.ks.RemoteState(rID, nInfo.NodeID, pair.actual.State.ToStatePlacement())
					if err != nil {
						log.Printf("error updating state: %v\n", err)
					}
				}
			} else {
				// expected range to be present, but it isn't!
				log.Printf("expected range %v on node %v (pos=%d), but it was missing\n", rID, nInfo.NodeID, pair.expected.Position)

				// Don't worry about it if the *next* placement is missing; we
				// probably asked after it was created here, but hasn't been
				// conveyed to the node yet.
				if pair.expected.Position == 0 {
					// Also do nothing if the placement is in Gone or Pending
					// state. We've probably created it controller-side but
					// haven't told the node about it yet.
					if s := pair.expected.Placement.State; s != ranje.SpGone && s != ranje.SpPending {
						err := r.ks.RemoteState(rID, nInfo.NodeID, ranje.SpGone)
						if err != nil {
							log.Printf("error updating state: %v\n", err)
						}
					}
				}
			}
		} else {
			if pair.hasActual() {
				// the node reports a placement that we didn't expect.
				log.Printf("got unexpected range %v on node %v\n", rID, nInfo.NodeID)
			} else {
				// Shouldn't be able to reach here. The above is buggy if so.
				panic("expected and actual state are both nil?")
			}
		}
	}

	return nil
}
