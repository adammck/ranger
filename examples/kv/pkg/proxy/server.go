package proxy

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const retries = 5

type proxyServer struct {
	pbkv.UnimplementedKVServer
	proxy *Proxy
}

func (ps *proxyServer) getClient(k string, write bool) (pbkv.KVClient, roster.Location, error) {
	loc := roster.Location{}

	states := []state.RemoteState{
		state.NsReady,
	}

	if !write {
		// Reads are okay while the range is being moved, too.
		states = append(states, state.NsTaking, state.NsTaken, state.NsTakingError)
	}

	locations := ps.proxy.rost.LocateInState(ranje.Key(k), states)

	if len(locations) == 0 {
		return nil, loc, status.Error(codes.Unimplemented, "no nodes have key")
	}

	if len(locations) > 1 {
		return nil, loc, status.Error(codes.Unimplemented, "proxying when multiple nodes have range not implemented yet")
	}

	loc = locations[0]

	client, ok := ps.proxy.clients[loc.Node]
	if !ok {
		return nil, loc, status.Errorf(codes.FailedPrecondition, "no client for node id %s?", loc)
	}

	return client, loc, nil
}

func (ps *proxyServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	var res *pbkv.GetResponse
	var loc roster.Location
	var err error

	err = ps.Run(ctx, req.Key, false, func(c pbkv.KVClient, l roster.Location) error {
		res, err = c.Get(ctx, req)
		loc = l
		return err
	})

	if ps.proxy.logReqs {
		log.Printf("Get: %s -> %s (%v)%s", req.Key, loc.Node, loc.Info.State, errStr(err))
	}

	return res, err
}

func (ps *proxyServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	var res *pbkv.PutResponse
	var loc roster.Location
	var err error

	err = ps.Run(ctx, req.Key, false, func(c pbkv.KVClient, l roster.Location) error {
		res, err = c.Put(ctx, req)
		loc = l
		return err
	})

	if ps.proxy.logReqs {
		log.Printf("Put: %s -> %s (%v)%s", req.Key, loc.Node, loc.Info.State, errStr(err))
	}

	return res, err
}

func (ps *proxyServer) Run(ctx context.Context, k string, w bool, f func(c pbkv.KVClient, loc roster.Location) error) error {
	n := 0
	ms := 10

	for {
		client, loc, err := ps.getClient(k, w)
		if err == nil {
			err = f(client, loc)
			if err == nil {
				return nil
			}
		}

		if n >= retries {
			return err
		}

		// TODO: Use a proper backoff lib here.
		time.Sleep(time.Duration(ms) * time.Millisecond)

		n += 1
		ms = ms * powInt(n, 2)
	}
}

// ugh golang
func powInt(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}

func errStr(err error) string {
	if err == nil {
		return ""
	}

	return fmt.Sprintf("; err=%v", err)
}
