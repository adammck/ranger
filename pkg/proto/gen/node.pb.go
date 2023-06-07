// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.23.2
// source: node.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Parent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Range *RangeMeta `protobuf:"bytes,1,opt,name=range,proto3" json:"range,omitempty"`
	// Range IDs in here may not appear in the PrepareRequest, because at some point
	// the history is pruned.
	Parent []uint64 `protobuf:"varint,2,rep,packed,name=parent,proto3" json:"parent,omitempty"`
	// TODO: This should probably be two fields, host and port? Or node ident?
	Placements []*Placement `protobuf:"bytes,3,rep,name=placements,proto3" json:"placements,omitempty"`
}

func (x *Parent) Reset() {
	*x = Parent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Parent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Parent) ProtoMessage() {}

func (x *Parent) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Parent.ProtoReflect.Descriptor instead.
func (*Parent) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{0}
}

func (x *Parent) GetRange() *RangeMeta {
	if x != nil {
		return x.Range
	}
	return nil
}

func (x *Parent) GetParent() []uint64 {
	if x != nil {
		return x.Parent
	}
	return nil
}

func (x *Parent) GetPlacements() []*Placement {
	if x != nil {
		return x.Placements
	}
	return nil
}

type PrepareRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Range *RangeMeta `protobuf:"bytes,1,opt,name=range,proto3" json:"range,omitempty"`
	// The range(s) which this range was created from, and the nodes where they
	// can currently be found. This is empty is the range is brand new. Nodes may
	// use this info to restore the current state of the range when accepting it.
	// TODO: Need nested parents here?
	Parents []*Parent `protobuf:"bytes,3,rep,name=parents,proto3" json:"parents,omitempty"`
}

func (x *PrepareRequest) Reset() {
	*x = PrepareRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PrepareRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PrepareRequest) ProtoMessage() {}

func (x *PrepareRequest) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PrepareRequest.ProtoReflect.Descriptor instead.
func (*PrepareRequest) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{1}
}

func (x *PrepareRequest) GetRange() *RangeMeta {
	if x != nil {
		return x.Range
	}
	return nil
}

func (x *PrepareRequest) GetParents() []*Parent {
	if x != nil {
		return x.Parents
	}
	return nil
}

type PrepareResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// TODO: Return just the state instead, like ServeResponse.
	RangeInfo *RangeInfo `protobuf:"bytes,1,opt,name=range_info,json=rangeInfo,proto3" json:"range_info,omitempty"`
}

func (x *PrepareResponse) Reset() {
	*x = PrepareResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PrepareResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PrepareResponse) ProtoMessage() {}

func (x *PrepareResponse) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PrepareResponse.ProtoReflect.Descriptor instead.
func (*PrepareResponse) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{2}
}

func (x *PrepareResponse) GetRangeInfo() *RangeInfo {
	if x != nil {
		return x.RangeInfo
	}
	return nil
}

type ServeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Range uint64 `protobuf:"varint,1,opt,name=range,proto3" json:"range,omitempty"`
	Force bool   `protobuf:"varint,2,opt,name=force,proto3" json:"force,omitempty"`
}

func (x *ServeRequest) Reset() {
	*x = ServeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ServeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServeRequest) ProtoMessage() {}

func (x *ServeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServeRequest.ProtoReflect.Descriptor instead.
func (*ServeRequest) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{3}
}

func (x *ServeRequest) GetRange() uint64 {
	if x != nil {
		return x.Range
	}
	return 0
}

func (x *ServeRequest) GetForce() bool {
	if x != nil {
		return x.Force
	}
	return false
}

type ServeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	State RangeNodeState `protobuf:"varint,1,opt,name=state,proto3,enum=ranger.RangeNodeState" json:"state,omitempty"`
}

func (x *ServeResponse) Reset() {
	*x = ServeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ServeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServeResponse) ProtoMessage() {}

func (x *ServeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServeResponse.ProtoReflect.Descriptor instead.
func (*ServeResponse) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{4}
}

func (x *ServeResponse) GetState() RangeNodeState {
	if x != nil {
		return x.State
	}
	return RangeNodeState_UNKNOWN
}

type DeactivateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Range uint64 `protobuf:"varint,1,opt,name=range,proto3" json:"range,omitempty"`
}

func (x *DeactivateRequest) Reset() {
	*x = DeactivateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeactivateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeactivateRequest) ProtoMessage() {}

func (x *DeactivateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeactivateRequest.ProtoReflect.Descriptor instead.
func (*DeactivateRequest) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{5}
}

func (x *DeactivateRequest) GetRange() uint64 {
	if x != nil {
		return x.Range
	}
	return 0
}

type DeactivateResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	State RangeNodeState `protobuf:"varint,1,opt,name=state,proto3,enum=ranger.RangeNodeState" json:"state,omitempty"`
}

func (x *DeactivateResponse) Reset() {
	*x = DeactivateResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeactivateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeactivateResponse) ProtoMessage() {}

func (x *DeactivateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeactivateResponse.ProtoReflect.Descriptor instead.
func (*DeactivateResponse) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{6}
}

func (x *DeactivateResponse) GetState() RangeNodeState {
	if x != nil {
		return x.State
	}
	return RangeNodeState_UNKNOWN
}

type DropRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Range uint64 `protobuf:"varint,1,opt,name=range,proto3" json:"range,omitempty"`
	Force bool   `protobuf:"varint,2,opt,name=force,proto3" json:"force,omitempty"`
}

func (x *DropRequest) Reset() {
	*x = DropRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DropRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DropRequest) ProtoMessage() {}

func (x *DropRequest) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DropRequest.ProtoReflect.Descriptor instead.
func (*DropRequest) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{7}
}

func (x *DropRequest) GetRange() uint64 {
	if x != nil {
		return x.Range
	}
	return 0
}

func (x *DropRequest) GetForce() bool {
	if x != nil {
		return x.Force
	}
	return false
}

type DropResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	State RangeNodeState `protobuf:"varint,1,opt,name=state,proto3,enum=ranger.RangeNodeState" json:"state,omitempty"`
}

func (x *DropResponse) Reset() {
	*x = DropResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DropResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DropResponse) ProtoMessage() {}

func (x *DropResponse) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DropResponse.ProtoReflect.Descriptor instead.
func (*DropResponse) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{8}
}

func (x *DropResponse) GetState() RangeNodeState {
	if x != nil {
		return x.State
	}
	return RangeNodeState_UNKNOWN
}

type InfoRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *InfoRequest) Reset() {
	*x = InfoRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InfoRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InfoRequest) ProtoMessage() {}

func (x *InfoRequest) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InfoRequest.ProtoReflect.Descriptor instead.
func (*InfoRequest) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{9}
}

type InfoResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ranges []*RangeInfo `protobuf:"bytes,1,rep,name=ranges,proto3" json:"ranges,omitempty"`
	// The nod wants the controller to remove all ranges from it. Probably because
	// it wants to shut down gracefully.
	WantDrain bool `protobuf:"varint,2,opt,name=wantDrain,proto3" json:"wantDrain,omitempty"`
}

func (x *InfoResponse) Reset() {
	*x = InfoResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InfoResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InfoResponse) ProtoMessage() {}

func (x *InfoResponse) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InfoResponse.ProtoReflect.Descriptor instead.
func (*InfoResponse) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{10}
}

func (x *InfoResponse) GetRanges() []*RangeInfo {
	if x != nil {
		return x.Ranges
	}
	return nil
}

func (x *InfoResponse) GetWantDrain() bool {
	if x != nil {
		return x.WantDrain
	}
	return false
}

type RangesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *RangesRequest) Reset() {
	*x = RangesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RangesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RangesRequest) ProtoMessage() {}

func (x *RangesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RangesRequest.ProtoReflect.Descriptor instead.
func (*RangesRequest) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{11}
}

type RangesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Meta  *RangeMeta     `protobuf:"bytes,1,opt,name=meta,proto3" json:"meta,omitempty"`
	State RangeNodeState `protobuf:"varint,2,opt,name=state,proto3,enum=ranger.RangeNodeState" json:"state,omitempty"`
}

func (x *RangesResponse) Reset() {
	*x = RangesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_node_proto_msgTypes[12]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RangesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RangesResponse) ProtoMessage() {}

func (x *RangesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_node_proto_msgTypes[12]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RangesResponse.ProtoReflect.Descriptor instead.
func (*RangesResponse) Descriptor() ([]byte, []int) {
	return file_node_proto_rawDescGZIP(), []int{12}
}

func (x *RangesResponse) GetMeta() *RangeMeta {
	if x != nil {
		return x.Meta
	}
	return nil
}

func (x *RangesResponse) GetState() RangeNodeState {
	if x != nil {
		return x.State
	}
	return RangeNodeState_UNKNOWN
}

var File_node_proto protoreflect.FileDescriptor

var file_node_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x72, 0x61,
	0x6e, 0x67, 0x65, 0x72, 0x1a, 0x0b, 0x72, 0x61, 0x6e, 0x6a, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x7c, 0x0a, 0x06, 0x50, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x12, 0x27, 0x0a, 0x05, 0x72,
	0x61, 0x6e, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x72, 0x61, 0x6e,
	0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x05, 0x72,
	0x61, 0x6e, 0x67, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x04, 0x52, 0x06, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x12, 0x31, 0x0a, 0x0a,
	0x70, 0x6c, 0x61, 0x63, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x11, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x50, 0x6c, 0x61, 0x63, 0x65, 0x6d,
	0x65, 0x6e, 0x74, 0x52, 0x0a, 0x70, 0x6c, 0x61, 0x63, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x22,
	0x63, 0x0a, 0x0e, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x27, 0x0a, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x11, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x4d,
	0x65, 0x74, 0x61, 0x52, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x28, 0x0a, 0x07, 0x70, 0x61,
	0x72, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x72, 0x61,
	0x6e, 0x67, 0x65, 0x72, 0x2e, 0x50, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x52, 0x07, 0x70, 0x61, 0x72,
	0x65, 0x6e, 0x74, 0x73, 0x22, 0x43, 0x0a, 0x0f, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x30, 0x0a, 0x0a, 0x72, 0x61, 0x6e, 0x67, 0x65,
	0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x72, 0x61,
	0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x09,
	0x72, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x22, 0x3a, 0x0a, 0x0c, 0x53, 0x65, 0x72,
	0x76, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x72, 0x61, 0x6e,
	0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x12,
	0x14, 0x0a, 0x05, 0x66, 0x6f, 0x72, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05,
	0x66, 0x6f, 0x72, 0x63, 0x65, 0x22, 0x3d, 0x0a, 0x0d, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2c, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52,
	0x61, 0x6e, 0x67, 0x65, 0x4e, 0x6f, 0x64, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73,
	0x74, 0x61, 0x74, 0x65, 0x22, 0x29, 0x0a, 0x11, 0x44, 0x65, 0x61, 0x63, 0x74, 0x69, 0x76, 0x61,
	0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x72, 0x61, 0x6e,
	0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x22,
	0x42, 0x0a, 0x12, 0x44, 0x65, 0x61, 0x63, 0x74, 0x69, 0x76, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2c, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61,
	0x6e, 0x67, 0x65, 0x4e, 0x6f, 0x64, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x22, 0x39, 0x0a, 0x0b, 0x44, 0x72, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x66, 0x6f, 0x72, 0x63,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x66, 0x6f, 0x72, 0x63, 0x65, 0x22, 0x3c,
	0x0a, 0x0c, 0x44, 0x72, 0x6f, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2c,
	0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e,
	0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x4e, 0x6f, 0x64, 0x65,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0x0d, 0x0a, 0x0b,
	0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x57, 0x0a, 0x0c, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x29, 0x0a, 0x06, 0x72,
	0x61, 0x6e, 0x67, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x72, 0x61,
	0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x06,
	0x72, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x77, 0x61, 0x6e, 0x74, 0x44, 0x72,
	0x61, 0x69, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x77, 0x61, 0x6e, 0x74, 0x44,
	0x72, 0x61, 0x69, 0x6e, 0x22, 0x0f, 0x0a, 0x0d, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x65, 0x0a, 0x0e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x25, 0x0a, 0x04, 0x6d, 0x65, 0x74, 0x61, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52,
	0x61, 0x6e, 0x67, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x04, 0x6d, 0x65, 0x74, 0x61, 0x12, 0x2c,
	0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e,
	0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x4e, 0x6f, 0x64, 0x65,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x32, 0xed, 0x02, 0x0a,
	0x04, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x3c, 0x0a, 0x07, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65,
	0x12, 0x16, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65,
	0x72, 0x2e, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x00, 0x12, 0x39, 0x0a, 0x08, 0x41, 0x63, 0x74, 0x69, 0x76, 0x61, 0x74, 0x65, 0x12,
	0x14, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x53,
	0x65, 0x72, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x45,
	0x0a, 0x0a, 0x44, 0x65, 0x61, 0x63, 0x74, 0x69, 0x76, 0x61, 0x74, 0x65, 0x12, 0x19, 0x2e, 0x72,
	0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x44, 0x65, 0x61, 0x63, 0x74, 0x69, 0x76, 0x61, 0x74, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1a, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72,
	0x2e, 0x44, 0x65, 0x61, 0x63, 0x74, 0x69, 0x76, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x33, 0x0a, 0x04, 0x44, 0x72, 0x6f, 0x70, 0x12, 0x13, 0x2e,
	0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x44, 0x72, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x14, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x44, 0x72, 0x6f, 0x70,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x33, 0x0a, 0x04, 0x49, 0x6e,
	0x66, 0x6f, 0x12, 0x13, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x49, 0x6e, 0x66, 0x6f,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72,
	0x2e, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x3b, 0x0a, 0x06, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x12, 0x15, 0x2e, 0x72, 0x61, 0x6e, 0x67,
	0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x16, 0x2e, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x42, 0x25, 0x5a, 0x23,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x64, 0x61, 0x6d, 0x6d,
	0x63, 0x6b, 0x2f, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x72, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_node_proto_rawDescOnce sync.Once
	file_node_proto_rawDescData = file_node_proto_rawDesc
)

func file_node_proto_rawDescGZIP() []byte {
	file_node_proto_rawDescOnce.Do(func() {
		file_node_proto_rawDescData = protoimpl.X.CompressGZIP(file_node_proto_rawDescData)
	})
	return file_node_proto_rawDescData
}

var file_node_proto_msgTypes = make([]protoimpl.MessageInfo, 13)
var file_node_proto_goTypes = []interface{}{
	(*Parent)(nil),             // 0: ranger.Parent
	(*PrepareRequest)(nil),     // 1: ranger.PrepareRequest
	(*PrepareResponse)(nil),    // 2: ranger.PrepareResponse
	(*ServeRequest)(nil),       // 3: ranger.ServeRequest
	(*ServeResponse)(nil),      // 4: ranger.ServeResponse
	(*DeactivateRequest)(nil),  // 5: ranger.DeactivateRequest
	(*DeactivateResponse)(nil), // 6: ranger.DeactivateResponse
	(*DropRequest)(nil),        // 7: ranger.DropRequest
	(*DropResponse)(nil),       // 8: ranger.DropResponse
	(*InfoRequest)(nil),        // 9: ranger.InfoRequest
	(*InfoResponse)(nil),       // 10: ranger.InfoResponse
	(*RangesRequest)(nil),      // 11: ranger.RangesRequest
	(*RangesResponse)(nil),     // 12: ranger.RangesResponse
	(*RangeMeta)(nil),          // 13: ranger.RangeMeta
	(*Placement)(nil),          // 14: ranger.Placement
	(*RangeInfo)(nil),          // 15: ranger.RangeInfo
	(RangeNodeState)(0),        // 16: ranger.RangeNodeState
}
var file_node_proto_depIdxs = []int32{
	13, // 0: ranger.Parent.range:type_name -> ranger.RangeMeta
	14, // 1: ranger.Parent.placements:type_name -> ranger.Placement
	13, // 2: ranger.PrepareRequest.range:type_name -> ranger.RangeMeta
	0,  // 3: ranger.PrepareRequest.parents:type_name -> ranger.Parent
	15, // 4: ranger.PrepareResponse.range_info:type_name -> ranger.RangeInfo
	16, // 5: ranger.ServeResponse.state:type_name -> ranger.RangeNodeState
	16, // 6: ranger.DeactivateResponse.state:type_name -> ranger.RangeNodeState
	16, // 7: ranger.DropResponse.state:type_name -> ranger.RangeNodeState
	15, // 8: ranger.InfoResponse.ranges:type_name -> ranger.RangeInfo
	13, // 9: ranger.RangesResponse.meta:type_name -> ranger.RangeMeta
	16, // 10: ranger.RangesResponse.state:type_name -> ranger.RangeNodeState
	1,  // 11: ranger.Node.Prepare:input_type -> ranger.PrepareRequest
	3,  // 12: ranger.Node.Activate:input_type -> ranger.ServeRequest
	5,  // 13: ranger.Node.Deactivate:input_type -> ranger.DeactivateRequest
	7,  // 14: ranger.Node.Drop:input_type -> ranger.DropRequest
	9,  // 15: ranger.Node.Info:input_type -> ranger.InfoRequest
	11, // 16: ranger.Node.Ranges:input_type -> ranger.RangesRequest
	2,  // 17: ranger.Node.Prepare:output_type -> ranger.PrepareResponse
	4,  // 18: ranger.Node.Activate:output_type -> ranger.ServeResponse
	6,  // 19: ranger.Node.Deactivate:output_type -> ranger.DeactivateResponse
	8,  // 20: ranger.Node.Drop:output_type -> ranger.DropResponse
	10, // 21: ranger.Node.Info:output_type -> ranger.InfoResponse
	12, // 22: ranger.Node.Ranges:output_type -> ranger.RangesResponse
	17, // [17:23] is the sub-list for method output_type
	11, // [11:17] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_node_proto_init() }
func file_node_proto_init() {
	if File_node_proto != nil {
		return
	}
	file_ranje_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_node_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Parent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PrepareRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PrepareResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ServeRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ServeResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeactivateRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeactivateResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DropRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DropResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InfoRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InfoResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RangesRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_node_proto_msgTypes[12].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RangesResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_node_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   13,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_node_proto_goTypes,
		DependencyIndexes: file_node_proto_depIdxs,
		MessageInfos:      file_node_proto_msgTypes,
	}.Build()
	File_node_proto = out.File
	file_node_proto_rawDesc = nil
	file_node_proto_goTypes = nil
	file_node_proto_depIdxs = nil
}
