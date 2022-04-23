package node

import (
	"context"
	"log"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	// Ensure that kvServer implements the KVServer interface
	var kvs *kvServer = nil
	var _ pbkv.KVServer = kvs
}

type kvServer struct {
	pbkv.UnimplementedKVServer
	node *Node
}

func (s *kvServer) Dump(ctx context.Context, req *pbkv.DumpRequest) (*pbkv.DumpResponse, error) {
	ident := ranje.Ident(req.RangeIdent)
	if ident == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range_ident")
	}

	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	rd, ok := s.node.data[ident]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range not found")
	}

	if rd.writes {
		return nil, status.Error(codes.FailedPrecondition, "can only dump ranges while writes are disabled")
	}

	res := &pbkv.DumpResponse{}
	for k, v := range rd.data {
		res.Pairs = append(res.Pairs, &pbkv.Pair{Key: []byte(k), Value: v})
	}

	log.Printf("Dumped: %s", ident)
	return res, nil
}

func (s *kvServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.rglt.Find(ranje.Key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	v, ok := rd.data[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such key")
	}

	if s.node.logReqs {
		log.Printf("get %q", k)
	}

	return &pbkv.GetResponse{
		Value: v,
	}, nil
}

func (s *kvServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.rglt.Find(ranje.Key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	if !rd.writes {
		return nil, status.Error(codes.FailedPrecondition, "can only PUT to ranges unless writes are enabled")
	}

	if req.Value == nil {
		delete(rd.data, k)
	} else {
		rd.data[k] = req.Value
	}

	if s.node.logReqs {
		log.Printf("put %q", k)
	}

	return &pbkv.PutResponse{}, nil
}
