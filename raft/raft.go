package raft

import (
	"sync"
	"time"
)

const (
	LEADER = "Leader"
	CANDIDATE = "Candidate"
	FOLLOWER = "Follower"
	// 心跳时间
	HeartbeatTerm = 100 * time.Millisecond
	// 选举超时时间 10ms - 500ms
	ElectionTimeoutBase = 400 * time.Millisecond
	ElectionTimeoutExtra = 100
	RpcCallTimeout = HeartbeatTerm
)

// 选举超时定时器
type electionTimer struct {
	d time.Duration
	t *time.Timer
}

// 接收可以apply到该节点的信息
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int64
}

// Raft 节点
// 每台服务器都有运行，只是根据不同的身份，接收/执行不同的RPC
type Raft struct {
	// 这个节点的index（每个节点都有自己的唯一index）
	me int32
	// 所有节点的RPC client 用于相互之间的通信
	peers []RaftClient
	// 所有节点的地址 ip:port，初始化指定
	addrList []string
	// 该节点的状态, Leader/Candidate/Follower
	state string
	// 超时选举定时器
	timer *electionTimer
	// channel 用于接收 ApplyMsg
	applyChan chan ApplyMsg
	// 当前节点是否停止
	stopped bool
	// 条件变量 用于触发 AppendEntries RPC
	conditions []*sync.Cond

	// 当前节点的term，从0开始
	currentTerm int64
	// 给哪个candidate投票
	votedFor int32
	// 当前节点存储的所有log
	logs []*LogEntry
	// 已经committed的日志索引位置
	commitIndex int64
	// 最后一个apply的log index
	lastApplied int64
	// 要发给每一个节点的下一个log index
	nextIndex []int64
	// 对于所有节点当前已经匹配的最高的log index
	matchIndex []int64
	// 上一次快照的位置，包含该index
	lastIncludedIndex int64
	// 上一次快照的term
	lastIncludedTerm int64
	// 网络是否断开，用于测试，false代表网络链接断开
	networkDrop bool
	// 用于测试网络不可靠，true代表网络连接不可靠
	networkUnreliable bool
	// RPC server implementation
	UnimplementedRaftServer
}