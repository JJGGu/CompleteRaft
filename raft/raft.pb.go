// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.14.0
// source: raft.proto

package raft

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type RequestVoteArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term         int64 `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"`
	CandidateId  int32 `protobuf:"varint,2,opt,name=CandidateId,proto3" json:"CandidateId,omitempty"`
	LastLogIndex int64 `protobuf:"zigzag64,3,opt,name=LastLogIndex,proto3" json:"LastLogIndex,omitempty"`
	LastLogTerm  int64 `protobuf:"zigzag64,4,opt,name=LastLogTerm,proto3" json:"LastLogTerm,omitempty"`
}

func (x *RequestVoteArgs) Reset() {
	*x = RequestVoteArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestVoteArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteArgs) ProtoMessage() {}

func (x *RequestVoteArgs) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteArgs.ProtoReflect.Descriptor instead.
func (*RequestVoteArgs) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{0}
}

func (x *RequestVoteArgs) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteArgs) GetCandidateId() int32 {
	if x != nil {
		return x.CandidateId
	}
	return 0
}

func (x *RequestVoteArgs) GetLastLogIndex() int64 {
	if x != nil {
		return x.LastLogIndex
	}
	return 0
}

func (x *RequestVoteArgs) GetLastLogTerm() int64 {
	if x != nil {
		return x.LastLogTerm
	}
	return 0
}

// 返回信息
type RequestVoteReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term        int64 `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"`             // 当前任期，便于candidate更新自己的任期号
	VoteGranted bool  `protobuf:"varint,2,opt,name=VoteGranted,proto3" json:"VoteGranted,omitempty"` // 是否赢得选票
}

func (x *RequestVoteReply) Reset() {
	*x = RequestVoteReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestVoteReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteReply) ProtoMessage() {}

func (x *RequestVoteReply) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteReply.ProtoReflect.Descriptor instead.
func (*RequestVoteReply) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{1}
}

func (x *RequestVoteReply) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteReply) GetVoteGranted() bool {
	if x != nil {
		return x.VoteGranted
	}
	return false
}

type AppendEntriesArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term         int64       `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"`
	LeaderId     int32       `protobuf:"varint,2,opt,name=LeaderId,proto3" json:"LeaderId,omitempty"`
	PrevLogIndex int64       `protobuf:"zigzag64,3,opt,name=PrevLogIndex,proto3" json:"PrevLogIndex,omitempty"` // 新日志前面的一个日志索引
	PrevLogTerm  int64       `protobuf:"zigzag64,4,opt,name=PrevLogTerm,proto3" json:"PrevLogTerm,omitempty"`   // 新日志前面的一个日志的任期号
	LeaderCommit int64       `protobuf:"zigzag64,5,opt,name=LeaderCommit,proto3" json:"LeaderCommit,omitempty"` // 领导者的已知已提交的最高的日志条目的索引
	Entries      []*LogEntry `protobuf:"bytes,6,rep,name=Entries,proto3" json:"Entries,omitempty"`              // 需要被保存的日志条目（被当做心跳使用是 则日志条目内容为空；为了提高效率可能一次性发送多个）
}

func (x *AppendEntriesArgs) Reset() {
	*x = AppendEntriesArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesArgs) ProtoMessage() {}

func (x *AppendEntriesArgs) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesArgs.ProtoReflect.Descriptor instead.
func (*AppendEntriesArgs) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{2}
}

func (x *AppendEntriesArgs) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesArgs) GetLeaderId() int32 {
	if x != nil {
		return x.LeaderId
	}
	return 0
}

func (x *AppendEntriesArgs) GetPrevLogIndex() int64 {
	if x != nil {
		return x.PrevLogIndex
	}
	return 0
}

func (x *AppendEntriesArgs) GetPrevLogTerm() int64 {
	if x != nil {
		return x.PrevLogTerm
	}
	return 0
}

func (x *AppendEntriesArgs) GetLeaderCommit() int64 {
	if x != nil {
		return x.LeaderCommit
	}
	return 0
}

func (x *AppendEntriesArgs) GetEntries() []*LogEntry {
	if x != nil {
		return x.Entries
	}
	return nil
}

type AppendEntriesReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term          int64 `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"`                   // 返回的follower的任期，用于Leader更新自己的任期
	Success       bool  `protobuf:"varint,2,opt,name=Success,proto3" json:"Success,omitempty"`               // 是否追加成功
	ConflictTerm  int64 `protobuf:"zigzag64,3,opt,name=ConflictTerm,proto3" json:"ConflictTerm,omitempty"`   // 如果参数中的PrevLogTerm 与 FOLLOWER自己的LastLogTerm不匹配，则将ConflictTerm设为logs[prevLogIndex].Term，便于快速同步
	ConflictIndex int64 `protobuf:"zigzag64,4,opt,name=ConflictIndex,proto3" json:"ConflictIndex,omitempty"` // 同上
}

func (x *AppendEntriesReply) Reset() {
	*x = AppendEntriesReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesReply) ProtoMessage() {}

func (x *AppendEntriesReply) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesReply.ProtoReflect.Descriptor instead.
func (*AppendEntriesReply) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{3}
}

func (x *AppendEntriesReply) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesReply) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *AppendEntriesReply) GetConflictTerm() int64 {
	if x != nil {
		return x.ConflictTerm
	}
	return 0
}

func (x *AppendEntriesReply) GetConflictIndex() int64 {
	if x != nil {
		return x.ConflictIndex
	}
	return 0
}

type InstallSnapshotArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term              int64  `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"`                           // leader's term
	LeaderId          int32  `protobuf:"varint,2,opt,name=LeaderId,proto3" json:"LeaderId,omitempty"`                     // leader's id
	LastIncludedIndex int64  `protobuf:"zigzag64,3,opt,name=LastIncludedIndex,proto3" json:"LastIncludedIndex,omitempty"` // 已经快照的最后一个log index
	LastIncludedTerm  int64  `protobuf:"zigzag64,4,opt,name=LastIncludedTerm,proto3" json:"LastIncludedTerm,omitempty"`   // 已经快照的最后一个log term
	Data              []byte `protobuf:"bytes,5,opt,name=Data,proto3" json:"Data,omitempty"`                              // raw bytes of the snapshot chunk, starting at offset
}

func (x *InstallSnapshotArgs) Reset() {
	*x = InstallSnapshotArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InstallSnapshotArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstallSnapshotArgs) ProtoMessage() {}

func (x *InstallSnapshotArgs) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InstallSnapshotArgs.ProtoReflect.Descriptor instead.
func (*InstallSnapshotArgs) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{4}
}

func (x *InstallSnapshotArgs) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *InstallSnapshotArgs) GetLeaderId() int32 {
	if x != nil {
		return x.LeaderId
	}
	return 0
}

func (x *InstallSnapshotArgs) GetLastIncludedIndex() int64 {
	if x != nil {
		return x.LastIncludedIndex
	}
	return 0
}

func (x *InstallSnapshotArgs) GetLastIncludedTerm() int64 {
	if x != nil {
		return x.LastIncludedTerm
	}
	return 0
}

func (x *InstallSnapshotArgs) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type InstallSnapshotReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term int64 `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"` // 返回的follower的任期，用于Leader更新自己的任期
}

func (x *InstallSnapshotReply) Reset() {
	*x = InstallSnapshotReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InstallSnapshotReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstallSnapshotReply) ProtoMessage() {}

func (x *InstallSnapshotReply) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InstallSnapshotReply.ProtoReflect.Descriptor instead.
func (*InstallSnapshotReply) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{5}
}

func (x *InstallSnapshotReply) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

type LogEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term    int64  `protobuf:"zigzag64,1,opt,name=Term,proto3" json:"Term,omitempty"`    // 当前Entry的任期
	Command []byte `protobuf:"bytes,2,opt,name=Command,proto3" json:"Command,omitempty"` // 具体要执行的指令
}

func (x *LogEntry) Reset() {
	*x = LogEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntry) ProtoMessage() {}

func (x *LogEntry) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogEntry.ProtoReflect.Descriptor instead.
func (*LogEntry) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{6}
}

func (x *LogEntry) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *LogEntry) GetCommand() []byte {
	if x != nil {
		return x.Command
	}
	return nil
}

// 用于持久化节点状态
type State struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CurrentTerm       int64       `protobuf:"zigzag64,1,opt,name=CurrentTerm,proto3" json:"CurrentTerm,omitempty"`
	VotedFor          int32       `protobuf:"varint,2,opt,name=VotedFor,proto3" json:"VotedFor,omitempty"`
	LastIncludedIndex int64       `protobuf:"zigzag64,3,opt,name=LastIncludedIndex,proto3" json:"LastIncludedIndex,omitempty"`
	LastIncludedTerm  int64       `protobuf:"zigzag64,4,opt,name=LastIncludedTerm,proto3" json:"LastIncludedTerm,omitempty"`
	Logs              []*LogEntry `protobuf:"bytes,5,rep,name=Logs,proto3" json:"Logs,omitempty"`
}

func (x *State) Reset() {
	*x = State{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *State) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*State) ProtoMessage() {}

func (x *State) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use State.ProtoReflect.Descriptor instead.
func (*State) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{7}
}

func (x *State) GetCurrentTerm() int64 {
	if x != nil {
		return x.CurrentTerm
	}
	return 0
}

func (x *State) GetVotedFor() int32 {
	if x != nil {
		return x.VotedFor
	}
	return 0
}

func (x *State) GetLastIncludedIndex() int64 {
	if x != nil {
		return x.LastIncludedIndex
	}
	return 0
}

func (x *State) GetLastIncludedTerm() int64 {
	if x != nil {
		return x.LastIncludedTerm
	}
	return 0
}

func (x *State) GetLogs() []*LogEntry {
	if x != nil {
		return x.Logs
	}
	return nil
}

var File_raft_proto protoreflect.FileDescriptor

var file_raft_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x72, 0x61,
	0x66, 0x74, 0x22, 0x8d, 0x01, 0x0a, 0x0f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f,
	0x74, 0x65, 0x41, 0x72, 0x67, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x12, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b, 0x43, 0x61,
	0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x0b, 0x43, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0c,
	0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x12, 0x52, 0x0c, 0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78,
	0x12, 0x20, 0x0a, 0x0b, 0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x12, 0x52, 0x0b, 0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x54, 0x65,
	0x72, 0x6d, 0x22, 0x48, 0x0a, 0x10, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74,
	0x65, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x12, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b, 0x56, 0x6f,
	0x74, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x0b, 0x56, 0x6f, 0x74, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x22, 0xd7, 0x01, 0x0a,
	0x11, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x41, 0x72,
	0x67, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x12,
	0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x1a, 0x0a, 0x08, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x49, 0x64, 0x12, 0x22, 0x0a, 0x0c, 0x50, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64,
	0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x12, 0x52, 0x0c, 0x50, 0x72, 0x65, 0x76, 0x4c, 0x6f,
	0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x20, 0x0a, 0x0b, 0x50, 0x72, 0x65, 0x76, 0x4c, 0x6f,
	0x67, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28, 0x12, 0x52, 0x0b, 0x50, 0x72, 0x65,
	0x76, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x22, 0x0a, 0x0c, 0x4c, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x12, 0x52, 0x0c,
	0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x12, 0x28, 0x0a, 0x07,
	0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e,
	0x72, 0x61, 0x66, 0x74, 0x2e, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x45,
	0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x22, 0x8c, 0x01, 0x0a, 0x12, 0x41, 0x70, 0x70, 0x65, 0x6e,
	0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x12, 0x0a,
	0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x12, 0x52, 0x04, 0x54, 0x65, 0x72,
	0x6d, 0x12, 0x18, 0x0a, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x08, 0x52, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x12, 0x22, 0x0a, 0x0c, 0x43,
	0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x12, 0x52, 0x0c, 0x43, 0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74, 0x54, 0x65, 0x72, 0x6d, 0x12,
	0x24, 0x0a, 0x0d, 0x43, 0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x12, 0x52, 0x0d, 0x43, 0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74,
	0x49, 0x6e, 0x64, 0x65, 0x78, 0x22, 0xb3, 0x01, 0x0a, 0x13, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6c,
	0x6c, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x41, 0x72, 0x67, 0x73, 0x12, 0x12, 0x0a,
	0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x12, 0x52, 0x04, 0x54, 0x65, 0x72,
	0x6d, 0x12, 0x1a, 0x0a, 0x08, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x08, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x12, 0x2c, 0x0a,
	0x11, 0x4c, 0x61, 0x73, 0x74, 0x49, 0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x64, 0x49, 0x6e, 0x64,
	0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x12, 0x52, 0x11, 0x4c, 0x61, 0x73, 0x74, 0x49, 0x6e,
	0x63, 0x6c, 0x75, 0x64, 0x65, 0x64, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x2a, 0x0a, 0x10, 0x4c,
	0x61, 0x73, 0x74, 0x49, 0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x64, 0x54, 0x65, 0x72, 0x6d, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x12, 0x52, 0x10, 0x4c, 0x61, 0x73, 0x74, 0x49, 0x6e, 0x63, 0x6c, 0x75,
	0x64, 0x65, 0x64, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x12, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x44, 0x61, 0x74, 0x61, 0x22, 0x2a, 0x0a, 0x14, 0x49,
	0x6e, 0x73, 0x74, 0x61, 0x6c, 0x6c, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65,
	0x70, 0x6c, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x12, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x22, 0x38, 0x0a, 0x08, 0x4c, 0x6f, 0x67, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x12, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x18, 0x0a, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61,
	0x6e, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e,
	0x64, 0x22, 0xc3, 0x01, 0x0a, 0x05, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x43,
	0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x12,
	0x52, 0x0b, 0x43, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x1a, 0x0a,
	0x08, 0x56, 0x6f, 0x74, 0x65, 0x64, 0x46, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x08, 0x56, 0x6f, 0x74, 0x65, 0x64, 0x46, 0x6f, 0x72, 0x12, 0x2c, 0x0a, 0x11, 0x4c, 0x61, 0x73,
	0x74, 0x49, 0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x64, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x12, 0x52, 0x11, 0x4c, 0x61, 0x73, 0x74, 0x49, 0x6e, 0x63, 0x6c, 0x75, 0x64,
	0x65, 0x64, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x2a, 0x0a, 0x10, 0x4c, 0x61, 0x73, 0x74, 0x49,
	0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x64, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x12, 0x52, 0x10, 0x4c, 0x61, 0x73, 0x74, 0x49, 0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x64, 0x54,
	0x65, 0x72, 0x6d, 0x12, 0x22, 0x0a, 0x04, 0x4c, 0x6f, 0x67, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x0e, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x52, 0x04, 0x4c, 0x6f, 0x67, 0x73, 0x32, 0xd8, 0x01, 0x0a, 0x04, 0x52, 0x61, 0x66, 0x74,
	0x12, 0x3e, 0x0a, 0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x12,
	0x15, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f,
	0x74, 0x65, 0x41, 0x72, 0x67, 0x73, 0x1a, 0x16, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00,
	0x12, 0x44, 0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65,
	0x73, 0x12, 0x17, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45,
	0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x41, 0x72, 0x67, 0x73, 0x1a, 0x18, 0x2e, 0x72, 0x61, 0x66,
	0x74, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52,
	0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x12, 0x4a, 0x0a, 0x0f, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6c,
	0x6c, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x12, 0x19, 0x2e, 0x72, 0x61, 0x66, 0x74,
	0x2e, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6c, 0x6c, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74,
	0x41, 0x72, 0x67, 0x73, 0x1a, 0x1a, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x49, 0x6e, 0x73, 0x74,
	0x61, 0x6c, 0x6c, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79,
	0x22, 0x00, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_raft_proto_rawDescOnce sync.Once
	file_raft_proto_rawDescData = file_raft_proto_rawDesc
)

func file_raft_proto_rawDescGZIP() []byte {
	file_raft_proto_rawDescOnce.Do(func() {
		file_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_raft_proto_rawDescData)
	})
	return file_raft_proto_rawDescData
}

var file_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_raft_proto_goTypes = []interface{}{
	(*RequestVoteArgs)(nil),      // 0: raft.RequestVoteArgs
	(*RequestVoteReply)(nil),     // 1: raft.RequestVoteReply
	(*AppendEntriesArgs)(nil),    // 2: raft.AppendEntriesArgs
	(*AppendEntriesReply)(nil),   // 3: raft.AppendEntriesReply
	(*InstallSnapshotArgs)(nil),  // 4: raft.InstallSnapshotArgs
	(*InstallSnapshotReply)(nil), // 5: raft.InstallSnapshotReply
	(*LogEntry)(nil),             // 6: raft.LogEntry
	(*State)(nil),                // 7: raft.State
}
var file_raft_proto_depIdxs = []int32{
	6, // 0: raft.AppendEntriesArgs.Entries:type_name -> raft.LogEntry
	6, // 1: raft.State.Logs:type_name -> raft.LogEntry
	0, // 2: raft.Raft.RequestVote:input_type -> raft.RequestVoteArgs
	2, // 3: raft.Raft.AppendEntries:input_type -> raft.AppendEntriesArgs
	4, // 4: raft.Raft.InstallSnapshot:input_type -> raft.InstallSnapshotArgs
	1, // 5: raft.Raft.RequestVote:output_type -> raft.RequestVoteReply
	3, // 6: raft.Raft.AppendEntries:output_type -> raft.AppendEntriesReply
	5, // 7: raft.Raft.InstallSnapshot:output_type -> raft.InstallSnapshotReply
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_raft_proto_init() }
func file_raft_proto_init() {
	if File_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_raft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestVoteArgs); i {
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
		file_raft_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestVoteReply); i {
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
		file_raft_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesArgs); i {
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
		file_raft_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesReply); i {
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
		file_raft_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InstallSnapshotArgs); i {
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
		file_raft_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InstallSnapshotReply); i {
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
		file_raft_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogEntry); i {
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
		file_raft_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*State); i {
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
			RawDescriptor: file_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_raft_proto_goTypes,
		DependencyIndexes: file_raft_proto_depIdxs,
		MessageInfos:      file_raft_proto_msgTypes,
	}.Build()
	File_raft_proto = out.File
	file_raft_proto_rawDesc = nil
	file_raft_proto_goTypes = nil
	file_raft_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// RaftClient is the client API for Raft service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type RaftClient interface {
	// 投票选举RPC
	RequestVote(ctx context.Context, in *RequestVoteArgs, opts ...grpc.CallOption) (*RequestVoteReply, error)
	// 日志追加RPC
	AppendEntries(ctx context.Context, in *AppendEntriesArgs, opts ...grpc.CallOption) (*AppendEntriesReply, error)
	// 快照RPC
	InstallSnapshot(ctx context.Context, in *InstallSnapshotArgs, opts ...grpc.CallOption) (*InstallSnapshotReply, error)
}

type raftClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftClient(cc grpc.ClientConnInterface) RaftClient {
	return &raftClient{cc}
}

func (c *raftClient) RequestVote(ctx context.Context, in *RequestVoteArgs, opts ...grpc.CallOption) (*RequestVoteReply, error) {
	out := new(RequestVoteReply)
	err := c.cc.Invoke(ctx, "/raft.Raft/RequestVote", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) AppendEntries(ctx context.Context, in *AppendEntriesArgs, opts ...grpc.CallOption) (*AppendEntriesReply, error) {
	out := new(AppendEntriesReply)
	err := c.cc.Invoke(ctx, "/raft.Raft/AppendEntries", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) InstallSnapshot(ctx context.Context, in *InstallSnapshotArgs, opts ...grpc.CallOption) (*InstallSnapshotReply, error) {
	out := new(InstallSnapshotReply)
	err := c.cc.Invoke(ctx, "/raft.Raft/InstallSnapshot", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServer is the server API for Raft service.
type RaftServer interface {
	// 投票选举RPC
	RequestVote(context.Context, *RequestVoteArgs) (*RequestVoteReply, error)
	// 日志追加RPC
	AppendEntries(context.Context, *AppendEntriesArgs) (*AppendEntriesReply, error)
	// 快照RPC
	InstallSnapshot(context.Context, *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// UnimplementedRaftServer can be embedded to have forward compatible implementations.
type UnimplementedRaftServer struct {
}

func (*UnimplementedRaftServer) RequestVote(context.Context, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (*UnimplementedRaftServer) AppendEntries(context.Context, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (*UnimplementedRaftServer) InstallSnapshot(context.Context, *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}

func RegisterRaftServer(s *grpc.Server, srv RaftServer) {
	s.RegisterService(&_Raft_serviceDesc, srv)
}

func _Raft_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raft.Raft/RequestVote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).RequestVote(ctx, req.(*RequestVoteArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raft.Raft/AppendEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).AppendEntries(ctx, req.(*AppendEntriesArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_InstallSnapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InstallSnapshotArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).InstallSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raft.Raft/InstallSnapshot",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).InstallSnapshot(ctx, req.(*InstallSnapshotArgs))
	}
	return interceptor(ctx, in, info, handler)
}

var _Raft_serviceDesc = grpc.ServiceDesc{
	ServiceName: "raft.Raft",
	HandlerType: (*RaftServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    _Raft_RequestVote_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _Raft_AppendEntries_Handler,
		},
		{
			MethodName: "InstallSnapshot",
			Handler:    _Raft_InstallSnapshot_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raft.proto",
}
