package operations

import (
	"context"
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
	"golang.org/x/sync/errgroup"
)

func Split(ks *ranje.Keyspace, r *ranje.Range, boundary ranje.Key, nLeft, nRight *ranje.Node) {
	src := r.MoveSrc()

	// Moves r into Splitting state
	// TODO: Rename MoveSrc! Clearly it's not just that.
	err := ks.DoSplit(r, boundary)
	if err != nil {
		fmt.Printf("DoSplit failed: %s\n", err.Error())
		return
	}

	// Only exactly two sides of the split for now
	rLeft, err := r.Child(0)
	if err != nil {
		fmt.Printf("DoSplit failed, getting left child: %s\n", err.Error())
		return
	}
	rRight, err := r.Child(1)
	if err != nil {
		fmt.Printf("DoSplit failed, getting right child: %s\n", err.Error())
		return
	}

	fmt.Printf("Splitting: %s -> %s, %s\n", r, rLeft, rRight)

	rLeft.MustState(ranje.Placing)
	pLeft, err := ranje.NewPlacement(rLeft, nLeft)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return
	}

	rRight.MustState(ranje.Placing)
	pRight, err := ranje.NewPlacement(rRight, nRight)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return
	}

	// 1. Take

	err = src.Take()
	if err != nil {
		fmt.Printf("Take failed: %s\n", err.Error())
		return
	}

	// 2. Give

	// TODO: Pass a context into Take, to cancel both together.
	g, _ := errgroup.WithContext(context.Background())
	for side, p := range map[string]*ranje.Placement{"left": pLeft, "right": pRight} {

		// Keep hold of current values for closure.
		// https://golang.org/doc/faq#closures_and_goroutines
		side := side
		p := p

		g.Go(func() error {

			// TODO: This doesn't work yet! Give doesn't include parents info.
			err = p.Give()
			if err != nil {
				return fmt.Errorf("give (%s) failed: %s", side, err.Error())
			}

			// Wait for the placement to become Ready (which it might already be).
			err = p.FetchWait()
			if err != nil {
				// TODO: Provide a more useful error here
				return fmt.Errorf("fetch (%s) failed: %s", side, err.Error())
			}

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		return
	}

	// 3. Drop

	err = src.Drop()
	if err != nil {
		fmt.Printf("Drop failed: %s\n", err.Error())
		return
	}

	// 4. Serve

	g, _ = errgroup.WithContext(context.Background())
	g.Go(func() error { return pLeft.Serve() })
	g.Go(func() error { return pRight.Serve() })
	err = g.Wait()
	if err != nil {
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		fmt.Printf("Serve (Drop) failed: %s\n", err.Error())
		return
	}

	rLeft.CompleteNextPlacement()
	rLeft.MustState(ranje.Ready)

	rRight.CompleteNextPlacement()
	rRight.MustState(ranje.Ready)

	// TODO: Should this happen via CompletePlacement, too?
	src.Forget()

	// This happens implicitly in Range.ChildStateChanged.
	// TODO: Is this a good idea? Here would be more explicit.
	// r.MustState(ranje.Obsolete)

	// TODO: Move this to some background GC routine in the balancer.
	err = ks.Discard(r)
	if err != nil {
		fmt.Printf("Discard failed: %s\n", err.Error())
	}
}
