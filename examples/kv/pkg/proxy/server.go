package proxy

import (
	"context"
	"log"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type proxyServer struct {
	pbkv.UnimplementedKVServer
	proxy *Proxy
}

func (ps *proxyServer) getClient(k string) (pbkv.KVClient, roster.Location, error) {
	loc := roster.Location{}

	states := []api.RemoteState{
		api.NsActive,
	}

	locations := ps.proxy.rost.LocateInState(api.Key(k), states)

	if len(locations) == 0 {
		return nil, loc, status.Errorf(codes.FailedPrecondition, "no nodes have key")
	}

	// Prefer the ready node.
	found := false
	for i := range locations {
		if locations[i].Info.State == api.NsActive {
			loc = locations[i]
			found = true
			break
		}
	}

	if !found {
		// No node was ready, so just pick the first.
		loc = locations[0]
	}

	client, ok := ps.proxy.clients[loc.Node]
	if !ok {
		return nil, loc, status.Errorf(codes.FailedPrecondition, "no client for node id %s?", loc.Node)
	}

	return client, loc, nil
}

func (ps *proxyServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	client, loc, err := ps.getClient(req.Key)
	if err != nil {
		return nil, err
	}

	res, err := client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	if err != nil {
		log.Printf("Error: %s (method=Get, key=%s, node=%s, state=%v)", err, req.Key, loc.Node, loc.Info.State)
	} else if ps.proxy.logReqs {
		log.Printf("Get: %s -> %s", req.Key, loc.Node)
	}

	return res, err
}

func (ps *proxyServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	var client pbkv.KVClient
	var res *pbkv.PutResponse
	var loc roster.Location
	var err error

	retries := 0
	maxRetries := 10

	for {

		client, loc, err = ps.getClient(req.Key)
		if err == nil {
			res, err = client.Put(ctx, req)
			if err == nil {
				// Success!
				break
			}
		}

		if retries >= maxRetries {
			break
		}

		retries += 1

		// TODO: Use a proper backoff lib here.
		// 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.28s, 2.56s, 5.12s
		d := time.Duration(((1<<retries)>>1)*10) * time.Millisecond

		// Sleep but respect cancellation.
		delay := time.NewTimer(d)
		select {
		case <-delay.C:
		case <-ctx.Done():
			if !delay.Stop() {
				<-delay.C
			}
		}
	}

	if err != nil {
		log.Printf("Error: %s (method=Put, key=%s, node=%s, state=%v)", err, req.Key, loc.Node, loc.Info.State)
	} else if ps.proxy.logReqs {
		log.Printf("Put: %s -> %s", req.Key, loc.Node)
	}

	return res, err
}
