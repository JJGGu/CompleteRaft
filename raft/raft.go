package raft

import "time"

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

// Raft 节点
// 每台服务器都有运行，只是根据不同的身份，接收/执行不同的RPC
type Raft struct {

}