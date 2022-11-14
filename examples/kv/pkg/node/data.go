package node

import (
	"context"
	"log"
	"sync/atomic"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvServer struct {
	pbkv.UnimplementedKVServer
	node *Node
}

func init() {
	// Ensure that kvServer implements the KVServer interface
	var kvs *kvServer = nil
	var _ pbkv.KVServer = kvs
}

// Get reads a single value by its key.
//
// Returns Aborted if the key is not in any of the ranges assigned to this node.
// This should not occur under normal circumstances. It means that the caller is
// confused about which range is assigned to which node. Most likely, the range
// has moved very recently and the caller hasn't heard about it yet.
//
// Returns NotFound If the key is not found but is within a valid range.
func (s *kvServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	rID, ok := s.node.rglt.Find(api.Key(k))
	if !ok {
		return nil, status.Error(codes.Aborted, "no such range")
	}

	s.node.rangesMu.RLock()
	r, ok := s.node.ranges[rID]
	s.node.rangesMu.RUnlock()
	if !ok {
		panic("rangelet found unknown range!")
	}

	r.dataMu.RLock()
	v, ok := r.data[k]
	r.dataMu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "no such key")
	}

	if s.node.logReqs {
		// TODO: Also log errors.
		log.Printf("Get: %q", k)
	}

	return &pbkv.GetResponse{
		Value: v,
	}, nil
}

// Put writes a single value by its key.
//
// Returns Aborted if the key is not in any of the assigned ranges, like Get.
//
// Returns FailedPrecondition if an appropriate range is assigned but read-only.
// This is normal, and occurs while a range is being moved away from the node.
// The caller will have to wait until the range is available elsewhere. (Unless
// the move fails, then it may become writable here!)
func (s *kvServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	rID, ok := s.node.rglt.Find(api.Key(k))
	if !ok {
		return nil, status.Error(codes.Aborted, "no such range")
	}

	s.node.rangesMu.RLock()
	r, ok := s.node.ranges[rID]
	s.node.rangesMu.RUnlock()
	if !ok {
		panic("rangelet found unknown range!")
	}

	if atomic.LoadUint32(&r.writable) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "can't PUT to read-only range")
	}

	if req.Value == nil {
		r.dataMu.Lock()
		delete(r.data, k)
		r.dataMu.Unlock()
	} else {
		r.dataMu.Lock()
		r.data[k] = req.Value
		r.dataMu.Unlock()
	}

	if s.node.logReqs {
		// TODO: Also log errors.
		log.Printf("Put: %q", k)
	}

	return &pbkv.PutResponse{}, nil
}

// Dump is called by other nodes during range moves, splits, and joins, to fetch
// data currently stores on this node.
func (s *kvServer) Dump(ctx context.Context, req *pbkv.DumpRequest) (*pbkv.DumpResponse, error) {
	ident := api.RangeID(req.RangeIdent)
	if ident == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range_ident")
	}

	s.node.rangesMu.RLock()
	r, ok := s.node.ranges[ident]
	s.node.rangesMu.RUnlock()
	if !ok {
		return nil, status.Error(codes.Aborted, "range not found")
	}

	if atomic.LoadUint32(&r.writable) == 1 {
		return nil, status.Error(codes.FailedPrecondition, "can't dump wriable range")
	}

	res := &pbkv.DumpResponse{}

	func() {
		r.dataMu.RLock()
		defer r.dataMu.RUnlock()
		for k, v := range r.data {
			res.Pairs = append(res.Pairs, &pbkv.Pair{Key: k, Value: v})
		}
	}()

	log.Printf("Dumped: %s", ident)
	return res, nil
}
