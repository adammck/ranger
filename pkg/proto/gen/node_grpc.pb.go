// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.23.2
// source: node.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	Node_Prepare_FullMethodName    = "/ranger.Node/Prepare"
	Node_Activate_FullMethodName   = "/ranger.Node/Activate"
	Node_Deactivate_FullMethodName = "/ranger.Node/Deactivate"
	Node_Drop_FullMethodName       = "/ranger.Node/Drop"
	Node_Info_FullMethodName       = "/ranger.Node/Info"
	Node_Ranges_FullMethodName     = "/ranger.Node/Ranges"
)

// NodeClient is the client API for Node service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type NodeClient interface {
	Prepare(ctx context.Context, in *PrepareRequest, opts ...grpc.CallOption) (*PrepareResponse, error)
	Activate(ctx context.Context, in *ServeRequest, opts ...grpc.CallOption) (*ServeResponse, error)
	Deactivate(ctx context.Context, in *DeactivateRequest, opts ...grpc.CallOption) (*DeactivateResponse, error)
	Drop(ctx context.Context, in *DropRequest, opts ...grpc.CallOption) (*DropResponse, error)
	// Controller wants to know the state of the node, including its ranges, and
	// (optionally) may extend the expiry of some ranges.
	// Proxy shouldn't call this; use Ranges instead.
	// TODO: Rename this now that it is not just fetching info.
	Info(ctx context.Context, in *InfoRequest, opts ...grpc.CallOption) (*InfoResponse, error)
	// Proxy wants to know what it can forward to this node.
	// Controller shouldn't call this; use Info instead.
	Ranges(ctx context.Context, in *RangesRequest, opts ...grpc.CallOption) (Node_RangesClient, error)
}

type nodeClient struct {
	cc grpc.ClientConnInterface
}

func NewNodeClient(cc grpc.ClientConnInterface) NodeClient {
	return &nodeClient{cc}
}

func (c *nodeClient) Prepare(ctx context.Context, in *PrepareRequest, opts ...grpc.CallOption) (*PrepareResponse, error) {
	out := new(PrepareResponse)
	err := c.cc.Invoke(ctx, Node_Prepare_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Activate(ctx context.Context, in *ServeRequest, opts ...grpc.CallOption) (*ServeResponse, error) {
	out := new(ServeResponse)
	err := c.cc.Invoke(ctx, Node_Activate_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Deactivate(ctx context.Context, in *DeactivateRequest, opts ...grpc.CallOption) (*DeactivateResponse, error) {
	out := new(DeactivateResponse)
	err := c.cc.Invoke(ctx, Node_Deactivate_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Drop(ctx context.Context, in *DropRequest, opts ...grpc.CallOption) (*DropResponse, error) {
	out := new(DropResponse)
	err := c.cc.Invoke(ctx, Node_Drop_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Info(ctx context.Context, in *InfoRequest, opts ...grpc.CallOption) (*InfoResponse, error) {
	out := new(InfoResponse)
	err := c.cc.Invoke(ctx, Node_Info_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Ranges(ctx context.Context, in *RangesRequest, opts ...grpc.CallOption) (Node_RangesClient, error) {
	stream, err := c.cc.NewStream(ctx, &Node_ServiceDesc.Streams[0], Node_Ranges_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &nodeRangesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Node_RangesClient interface {
	Recv() (*RangesResponse, error)
	grpc.ClientStream
}

type nodeRangesClient struct {
	grpc.ClientStream
}

func (x *nodeRangesClient) Recv() (*RangesResponse, error) {
	m := new(RangesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// NodeServer is the server API for Node service.
// All implementations must embed UnimplementedNodeServer
// for forward compatibility
type NodeServer interface {
	Prepare(context.Context, *PrepareRequest) (*PrepareResponse, error)
	Activate(context.Context, *ServeRequest) (*ServeResponse, error)
	Deactivate(context.Context, *DeactivateRequest) (*DeactivateResponse, error)
	Drop(context.Context, *DropRequest) (*DropResponse, error)
	// Controller wants to know the state of the node, including its ranges, and
	// (optionally) may extend the expiry of some ranges.
	// Proxy shouldn't call this; use Ranges instead.
	// TODO: Rename this now that it is not just fetching info.
	Info(context.Context, *InfoRequest) (*InfoResponse, error)
	// Proxy wants to know what it can forward to this node.
	// Controller shouldn't call this; use Info instead.
	Ranges(*RangesRequest, Node_RangesServer) error
	mustEmbedUnimplementedNodeServer()
}

// UnimplementedNodeServer must be embedded to have forward compatible implementations.
type UnimplementedNodeServer struct {
}

func (UnimplementedNodeServer) Prepare(context.Context, *PrepareRequest) (*PrepareResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Prepare not implemented")
}
func (UnimplementedNodeServer) Activate(context.Context, *ServeRequest) (*ServeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Activate not implemented")
}
func (UnimplementedNodeServer) Deactivate(context.Context, *DeactivateRequest) (*DeactivateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Deactivate not implemented")
}
func (UnimplementedNodeServer) Drop(context.Context, *DropRequest) (*DropResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Drop not implemented")
}
func (UnimplementedNodeServer) Info(context.Context, *InfoRequest) (*InfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Info not implemented")
}
func (UnimplementedNodeServer) Ranges(*RangesRequest, Node_RangesServer) error {
	return status.Errorf(codes.Unimplemented, "method Ranges not implemented")
}
func (UnimplementedNodeServer) mustEmbedUnimplementedNodeServer() {}

// UnsafeNodeServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NodeServer will
// result in compilation errors.
type UnsafeNodeServer interface {
	mustEmbedUnimplementedNodeServer()
}

func RegisterNodeServer(s grpc.ServiceRegistrar, srv NodeServer) {
	s.RegisterService(&Node_ServiceDesc, srv)
}

func _Node_Prepare_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PrepareRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Prepare(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Node_Prepare_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Prepare(ctx, req.(*PrepareRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Activate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Activate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Node_Activate_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Activate(ctx, req.(*ServeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Deactivate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeactivateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Deactivate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Node_Deactivate_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Deactivate(ctx, req.(*DeactivateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Drop_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DropRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Drop(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Node_Drop_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Drop(ctx, req.(*DropRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Info_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Info(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Node_Info_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Info(ctx, req.(*InfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Ranges_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RangesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(NodeServer).Ranges(m, &nodeRangesServer{stream})
}

type Node_RangesServer interface {
	Send(*RangesResponse) error
	grpc.ServerStream
}

type nodeRangesServer struct {
	grpc.ServerStream
}

func (x *nodeRangesServer) Send(m *RangesResponse) error {
	return x.ServerStream.SendMsg(m)
}

// Node_ServiceDesc is the grpc.ServiceDesc for Node service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Node_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ranger.Node",
	HandlerType: (*NodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Prepare",
			Handler:    _Node_Prepare_Handler,
		},
		{
			MethodName: "Activate",
			Handler:    _Node_Activate_Handler,
		},
		{
			MethodName: "Deactivate",
			Handler:    _Node_Deactivate_Handler,
		},
		{
			MethodName: "Drop",
			Handler:    _Node_Drop_Handler,
		},
		{
			MethodName: "Info",
			Handler:    _Node_Info_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Ranges",
			Handler:       _Node_Ranges_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "node.proto",
}
