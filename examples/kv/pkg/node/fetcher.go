package node

/*
func fetchMany(dest ranje.Meta, parents []*pbr.Placement) {

	// Parse all the parents before spawning threads. This is fast and failure
	// indicates a bug more than a transient problem.
	rms := make([]*ranje.Meta, len(parents))
	for i, p := range parents {
		rms[i] = &rm
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mu := sync.Mutex{}

	// Fetch each source range in parallel.
	g, ctx := errgroup.WithContext(ctx)
	for i := range parents {

		// Ignore ranges which aren't currently assigned to any node. This kv
		// example doesn't have an external write log, so if it isn't placed,
		// there's nothing we can do with it.
		//
		// This includes ranges which are being placed for the first time, which
		// includes new split/join ranges! That isn't an error. Their state is
		// reassembled from their parents.
		if parents[i].Node == "" {
			//log.Printf("not fetching unplaced range: %s", rms[i].ident)
			continue
		}

		// lol, golang
		// https://golang.org/doc/faq#closures_and_goroutines
		i := i

		g.Go(func() error {
			return rd.fetchOne(ctx, &mu, dest, parents[i].Node, rms[i])
		})
	}

	if err := g.Wait(); err != nil {
		return err
		return
	}

	// Can't go straight into rsReady, because that allows writes. The source
	// node(s) are still serving reads, and if we start writing, they'll be
	// wrong. We can only serve reads until the assigner tells them to stop,
	// which will redirect all reads to us. Then we can start writing.
	rd.state = state.NsPrepared
}

func (rd *RangeData) fetchOne(ctx context.Context, mu *sync.Mutex, dest ranje.Meta, addr string, src *ranje.Meta) error {
	if addr == "" {
		log.Printf("FetchOne: %s with no addr", src.ident)
		return nil
	}

	log.Printf("FetchOne: %s from: %s", src.ident, addr)

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock())
	if err != nil {
		// TODO: Probably a bit excessive
		log.Fatalf("fail to dial: %v", err)
	}

	client := pbkv.NewKVClient(conn)

	res, err := client.Dump(ctx, &pbkv.DumpRequest{RangeIdent: uint64(src.ident)})
	if err != nil {
		log.Printf("FetchOne failed: %s from: %s: %s", src.ident, addr, err)

		return err
	}

	// TODO: Optimize loading by including range start and end in the Dump response. If they match, can skip filtering.

	c := 0
	s := 0
	mu.Lock()
	for _, pair := range res.Pairs {

		// TODO: Untangle []byte vs string mess
		if dest.Contains(pair.Key) {
			rd.data[string(pair.Key)] = pair.Value
			c += 1
		} else {
			s += 1
		}
	}
	mu.Unlock()
	log.Printf("Inserted %d keys from range %s via node %s (skipped %d)", c, src.ident, addr, s)

	return nil
}
*/
