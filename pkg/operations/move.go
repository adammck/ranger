package operations

import (
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
)

func Move(r *ranje.Range, node *ranje.Node) {
	var src *ranje.Placement

	// If the range is currently ready, it's currently places on some node.
	if r.State() == ranje.Ready {
		fmt.Printf("Moving: %s\n", r)
		r.MustState(ranje.Moving)
		src = r.MoveSrc()

	} else if r.State() == ranje.Quarantined || r.State() == ranje.Pending {
		fmt.Printf("Placing: %s\n", r)
		r.MustState(ranje.Placing)

	} else {
		panic(fmt.Sprintf("unexpectd range state?! %s", r.State()))
	}

	// TODO: If src and dest are the same node (i.e. the range is moving to the same node, we get stuck in Taken)
	// TODO: Could use an extra step here to clear the move with the dest node first.

	dest, err := ranje.NewPlacement(r, node)
	if err != nil {
		//return nil, fmt.Errorf("couldn't Give range; error creating placement: %s", err)
		// TODO: Do something less dumb than this.
		r.MustState(ranje.Ready)
		return
	}

	// 1. Take
	// (only if moving; skip if doing initial placement)

	if src != nil {
		err = src.Take()
		if err != nil {
			fmt.Printf("Take failed: %s\n", err.Error())
			r.MustState(ranje.Ready) // ???
			return
		}
	}

	// 2. Give

	err = dest.Give()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	// Wait for the placement to become Ready (which it might already be).

	if src != nil {
		err = dest.FetchWait()
		if err != nil {
			// TODO: Provide a more useful error here
			fmt.Printf("Fetch failed: %s\n", err.Error())
			return
		}
	}

	// 3. Drop
	// (only if moving; skip if doing initial placement)

	if src != nil {
		err = src.Drop()
		if err != nil {
			fmt.Printf("Drop failed: %s\n", err.Error())
			// No state change. Stay in Moving.
			// TODO: Repair the situation somehow.
			//r.MustState(ranje.MoveError)
			return
		}
	}

	// 4. Serve

	if src != nil {
		err = dest.Serve()
		if err != nil {
			fmt.Printf("Serve failed: %s\n", err.Error())
			// No state change. Stay in Moving.
			// TODO: Repair the situation somehow.
			//r.MustState(ranje.MoveError)
			return
		}
	}

	r.MustState(ranje.Ready)

	// 5. Cleanup

	if src != nil {
		src.Forget()
	}
}
