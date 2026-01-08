// Copyright (c) 2024 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

package grpc

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

// StreamRequest is the request message for streaming stdout/stderr.
type StreamRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ContainerId string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	ExecId      string `protobuf:"bytes,2,opt,name=exec_id,json=execId,proto3" json:"exec_id,omitempty"`
}

func (x *StreamRequest) Reset() {
	*x = StreamRequest{}
}

func (x *StreamRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamRequest) ProtoMessage() {}

func (x *StreamRequest) ProtoReflect() protoreflect.Message {
	return nil // Simplified - not used for marshaling
}

func (x *StreamRequest) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

func (x *StreamRequest) GetExecId() string {
	if x != nil {
		return x.ExecId
	}
	return ""
}

// StreamResponse is the response message for streaming stdout/stderr.
type StreamResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *StreamResponse) Reset() {
	*x = StreamResponse{}
}

func (x *StreamResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamResponse) ProtoMessage() {}

func (x *StreamResponse) ProtoReflect() protoreflect.Message {
	return nil // Simplified - not used for marshaling
}

func (x *StreamResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}
