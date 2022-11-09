package util

import (
	"fmt"
	"log"
	"sync"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Deduper struct {

	// TODO: Move this into the Tick method; just pass it along.
	wg sync.WaitGroup

	// RPCs which have been sent (via orch.RPC) but not yet completed. Used to
	// avoid sending the same RPC redundantly every single tick. (Though RPCs
	// *can* be re-sent an arbitrary number of times. Rangelet will dedup them.)
	inFlight   map[string]struct{}
	inFlightMu sync.Mutex
}

func NewDeduper() Deduper {
	return Deduper{
		inFlight: map[string]struct{}{},
	}
}

func (a *Deduper) Wait() {
	a.wg.Wait()
}

func (a *Deduper) Exec(action api.Action, p *ranje.Placement, n *roster.Node, f func()) {
	key := fmt.Sprintf("%v:%s:%s", action, p.Range().Meta.Ident, n.Remote.Ident)

	a.inFlightMu.Lock()
	_, ok := a.inFlight[key]
	if !ok {
		a.inFlight[key] = struct{}{}
	}
	a.inFlightMu.Unlock()

	if ok {
		log.Printf("dropping in-flight command: %s", key)
		return
	}

	a.wg.Add(1)

	go func() {

		// TODO: Inject some client-side chaos here, too. RPCs complete very
		//       quickly locally, which doesn't test our in-flight thing well.
		f()

		a.inFlightMu.Lock()
		if _, ok := a.inFlight[key]; !ok {
			// Critical this works, because could drop all RPCs. Note that we
			// don't release the lock, so no more RPCs even if the panic is
			// caught, which it shouldn't be.
			panic(fmt.Sprintf("no record of in-flight command: %s", key))
		}
		log.Printf("command completed: %s", key)
		delete(a.inFlight, key)
		a.inFlightMu.Unlock()

		a.wg.Done()
	}()
}
