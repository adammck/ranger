package proxy

import (
	"context"
	"fmt"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type proxyServer struct {
	pbkv.UnimplementedKVServer
	proxy *Proxy
}

func (ps *proxyServer) getClient(k []byte) (pbkv.KVClient, error) {
	nids := ps.proxy.rost.Locate(ranje.Key(k))

	for _, nid := range nids {
		fmt.Printf("%s -> %s\n", k, nid)
	}

	if len(nids) == 0 {
		return nil, status.Error(codes.Unimplemented, "no nodes have key")
	}

	if len(nids) > 1 {
		return nil, status.Error(codes.Unimplemented, "proxying when multiple nodes have range not implemented yet")
	}

	nid := nids[0]
	client, ok := ps.proxy.clients[nid]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "no client for node id %s?", nid)
	}

	return client, nil
}

func (ps *proxyServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	client, err := ps.getClient(req.Key)
	if err != nil {
		return nil, err
	}

	return client.Get(ctx, req)
}

func (ps *proxyServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	client, err := ps.getClient(req.Key)
	if err != nil {
		return nil, err
	}

	return client.Put(ctx, req)
}
