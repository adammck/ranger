package operations

import (
	"context"
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
	"golang.org/x/sync/errgroup"
)

func Join(ks *ranje.Keyspace, r1, r2 *ranje.Range, node *ranje.Node) {
	p1 := r1.MoveSrc()
	p2 := r2.MoveSrc()

	// Moves r1 and r2 into Joining state.
	// Starts dest in Pending state. (Like all ranges!)
	// Returns error if either of the ranges aren't ready, or if they're not adjacent.
	r3, err := ks.JoinTwo(r1, r2)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	fmt.Printf("Joining: %s, %s -> %s\n", r1, r2, r3)

	// 1. Take

	// TODO: Pass the context into Take, to cancel both together.
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error { return p1.Take() })
	g.Go(func() error { return p2.Take() })
	err = g.Wait()
	if err != nil {
		fmt.Printf("Join (Take) failed: %s\n", err.Error())
		return
	}

	// 2. Give

	r3.MustState(ranje.Placing)

	p3, err := ranje.NewPlacement(r3, node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return
	}

	_, err = p3.Give()
	if err != nil {
		fmt.Printf("Join (Give) failed: %s\n", err.Error())
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	// Wait for the placement to become Ready (which it might already be).
	err = p3.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Join (Fetch) failed: %s\n", err.Error())
		return
	}

	// 3. Drop

	g, _ = errgroup.WithContext(context.Background())
	g.Go(func() error { return p1.Drop() })
	g.Go(func() error { return p2.Drop() })
	err = g.Wait()
	if err != nil {
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		fmt.Printf("Join (Drop) failed: %s\n", err.Error())
		return
	}

	// 4. Serve

	err = p3.Serve()
	if err != nil {
		fmt.Printf("Join (Serve) failed: %s\n", err.Error())
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	r3.CompleteNextPlacement()
	r3.MustState(ranje.Ready)

	// 5. Cleanup

	for _, p := range []*ranje.DurablePlacement{p1, p2} {
		p.Forget()
	}

	for _, r := range []*ranje.Range{r1, r2} {

		// This happens implicitly in Range.ChildStateChanged.
		// TODO: Is this a good idea? Here would be more explicit.
		// r.MustState(ranje.Obsolete)

		// TODO: This part should probably be handled later by some kind of GC.
		err = ks.Discard(r)
		if err != nil {
			fmt.Printf("Join (Discard) failed: %s\n", err.Error())
		}
	}
}
