package consul

import (
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
)

type Discoverer struct {
	consul *consulapi.Client
	srv    *grpc.Server
}

// TODO: Take a consul API client here, not a cfg.
func NewDiscoverer(cfg *consulapi.Config, srv *grpc.Server) (*Discoverer, error) {
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	d := &Discoverer{
		consul: client,
		srv:    srv,
	}

	return d, nil
}

type discoveryGetter struct {
	disc *Discoverer
	name string

	// stop is closed to signal that run should stop ticking and return.
	stop chan bool

	// running can be waited on to block until run is about to return. Wait on
	// this after closing stop to ensure that no more ticks will happen.
	running sync.WaitGroup

	// Remotes that we know about.
	remotes   map[string]api.Remote
	remotesMu sync.RWMutex

	// Functions to be called when new remotes are added and removed.
	add    func(api.Remote)
	remove func(api.Remote)
}

func (d *Discoverer) Discover(svcName string, add, remove func(api.Remote)) discovery.Getter {
	dg := &discoveryGetter{
		disc:    d,
		name:    svcName,
		stop:    make(chan bool),
		remotes: map[string]api.Remote{},
		add:     add,
		remove:  remove,
	}

	dg.running.Add(1)
	go dg.run()

	return dg
}

func (dg *discoveryGetter) tick() error {

	// Fetch all entries (remotes) for the service name.
	res, _, err := dg.disc.consul.Catalog().Service(dg.name, "", &consulapi.QueryOptions{})
	if err != nil {
		return err
	}

	seen := map[string]struct{}{}
	added := []api.Remote{}
	removed := []api.Remote{}

	dg.remotesMu.Lock()

	// Check every remote, see if it needs adding to our cache.
	for _, r := range res {
		svcID := r.ServiceID
		seen[svcID] = struct{}{}

		// Already known
		if _, ok := dg.remotes[svcID]; ok {
			continue
		}

		rem := api.Remote{
			Ident: svcID,
			Host:  r.Address, // https://github.com/hashicorp/consul/issues/2076
			Port:  r.ServicePort,
		}

		// New remote
		dg.remotes[svcID] = rem
		added = append(added, rem)
		//log.Printf("Added: %s", svcID)
	}

	// Remove any nodes which have gone from consul.
	for svcID, rem := range dg.remotes {
		if _, ok := seen[svcID]; !ok {
			delete(dg.remotes, svcID)
			removed = append(removed, rem)
			//log.Printf("Removing: %s", svcID)
		}
	}

	dg.remotesMu.Unlock()

	// Call add/remove callbacks outside of lock. But still synchronously inside
	// this function, so that we won't tick again until they return. Should keep
	// things linear (i.e. no remotes being removed before they're added).

	if dg.add != nil {
		for _, rem := range added {
			dg.add(rem)
		}
	}

	if dg.remove != nil {
		for _, rem := range removed {
			dg.remove(rem)
		}
	}

	return nil
}

func (dg *discoveryGetter) run() {
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			dg.tick()
		case <-dg.stop:
			ticker.Stop()
			dg.running.Done()
			return
		}
	}
}

func (dg *discoveryGetter) Get() ([]api.Remote, error) {
	dg.remotesMu.RLock()
	defer dg.remotesMu.RUnlock()

	res := make([]api.Remote, len(dg.remotes))
	i := 0
	for _, v := range dg.remotes {
		res[i] = v
		i += 1
	}

	return res, nil
}

// TODO: Could probably accomplish this with a cancellable context instead?
func (dg *discoveryGetter) Stop() error {

	// Signal run to return instead of tick again.
	close(dg.stop)

	// Block until any in-progress ticks are finished.
	dg.running.Wait()

	return nil
}
