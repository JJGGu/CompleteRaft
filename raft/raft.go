package raft

import (
	"context"
	"math"
	"math/rand"
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
	StayTerm = -1
	VoteNil = -1
	TermNil = -1
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
	mu sync.RWMutex
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
	// 被快照取代的最后的条目在日志中的索引值
	lastIncludedIndex int64
	// 该条目的任期号
	lastIncludedTerm int64
	// 网络是否断开，用于测试，false代表网络链接断开
	networkDrop bool
	// 用于测试网络不可靠，true代表网络连接不可靠
	networkUnreliable bool
	// RPC server implementation
	UnimplementedRaftServer
}

//  RequestVote RPC args：传入的Candidate相关信息
func (rf *Raft) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 初始化reply
	reply := &RequestVoteReply{
		Term: rf.currentTerm,
		VoteGranted: false,
	}
	// 1. term 校验
	// 如果candidate的term比当前服务器的节点小的话，返回初始化的reply
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	// 转化状态，term同步，并将voteFor设置为-1
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.convertToFollower(args.Term, VoteNil)
	}

	// 2. 检查是否已经给别人投过票
	if rf.votedFor != VoteNil && rf.votedFor != args.CandidateId {
		return reply, nil
	}

	// 3. 检查candidate的日志与当前服务器节点最后日志任期和索引是否匹配。必须满足candidate是有最新的logIndexTerm和LogIndex的节点
	lastIndex, lastTerm := rf.lastIndex(), rf.lastTerm()
	// candidate最后一个日志的term小于当前节点
	if lastTerm > args.LastLogIndex {
		return reply, nil
	}
	// 任期相同，但是日志索引比当前节点小
	if lastTerm == args.LastLogIndex && lastIndex > args.LastLogIndex {
		return reply, nil
	}
	// 没有上述三种情况，则给candidate投票
	reply.VoteGranted = true
	rf.convertToFollower(args.Term, args.CandidateId)
	rf.resetElectTimer()

	return reply, nil
}

// AppendEntries RPC  args: 传入的Leader的日志的相关信息
func (rf *Raft) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 初始化reply
	reply := &AppendEntriesReply{
		Term: rf.currentTerm,
		Success: false,
		ConflictIndex: 0,
		ConflictTerm: 0,
	}
	// 检查Term
	// 如果Leader的Term比当前节点的Term大，则说明Leader已经不是最新的了，直接返回默认reply
	if args.Term < rf.currentTerm {
		return reply,nil
	}
	// 如果Leader的Term比当前节点的Term大，则重置当前节点的Term
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		// 调整Term
		rf.convertToFollower(args.Term, VoteNil)
	}
	// 重置选举超时
	rf.resetElectTimer()

	// 如果当前节点的最后一个日志与Leader的PrevLogIndex不匹配，则需要记录该节点的最后一个logindex返回给Leader，Leader会重新发送从那个日志之后的日志
	lastIndex := rf.lastIndex()
	if lastIndex < args.PrevLogIndex {
		reply.ConflictTerm = TermNil
		reply.ConflictIndex = lastIndex + 1
		return reply, nil
	}

	// 检查从Leader传输过来的
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := int64(0)
	// 如果Leader的Prevlogindex >= 当前节点的最后一个快照处的logIndex， 则将prevLogTerm置位该节点的preLogIndex处的Term
	if prevLogIndex >= rf.lastIncludedIndex {
		prevLogTerm = rf.getLog(prevLogIndex).Term
	}
	// 如果Leader的prevLogTerm与该节点处的preLogIndex处的Term不同
	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		// 根据term找出该节点中第一个冲突的logIndex（该LogIndex之前的日志都匹配）
		for i, e := range rf.logs {
			if e.Term == prevLogTerm {
				reply.ConflictIndex = int64(i) + rf.lastIncludedIndex
			}
		}
	}

	// 日志追加
	for i, e := range args.Entries {
		// 要追加到的日志的Index
		j := prevLogIndex + int64(1 + i)
		if j <= lastIndex {
			// 有相同的日志情况
			if rf.getLog(j).Term == e.Term {
				continue
			}
			// 如果出现了不匹配的日志，则将其后的删除后再替换为参数中的日志
			rf.logs = rf.logs[:rf.subIndex(j)]
		}
		// 追加日志
		rf.logs = append(rf.logs, args.Entries[i:]...)
		//_ = rf.persistState()
		break
	}

	// 提交Apply，通过applyCh发送提交
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int64(math.Max(float64(args.LeaderCommit), float64(rf.lastIndex())))
		rf.apply()
	}
	//rf.convertToFollower(args.Term, args.LeaderId)
	reply.Success = true
	return reply, nil
}

// InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(ctx context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &InstallSnapshotReply{
		Term: rf.currentTerm,
	}

	// 同样的操作，检查term，重置定时器
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.convertToFollower(args.Term, VoteNil)
	}
	rf.resetElectTimer()

	// 如果Leader的快照的最后Index小于等于当前节点的快照的LastIndex,则直接返回
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return reply, nil
	}

	// 如果leader快照位置小于当前节点的当前日志长度，则进行截取，保留之后的
	if args.LastIncludedIndex < rf.lastIndex() {
		rf.logs = rf.logs[args.LastIncludedIndex - rf.lastIncludedIndex:]
	} else {
		// 当前节点的最后的日志index比Leader的快照最后index小则丢弃自己的所有日志,使用快照的日志
		rf.logs = []*LogEntry{}
		rf.logs = append(rf.logs, &LogEntry{
			Term: args.LastIncludedIndex,
			Command: nil,
		})
	}

	// 更新Index
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	//_ = rf.persistStatesAndSnapshot(args.Data)
	rf.commitIndex = int64(math.Max(float64(rf.commitIndex), float64(args.LastIncludedIndex)))
	rf.lastApplied = int64(math.Max(float64(rf.lastApplied), float64(args.LastIncludedIndex)))

	// 发出通知
	rf.applyChan <- ApplyMsg{
		CommandValid: false, // false代表是snapshot
		CommandIndex: -1,
		Command:      args.Data,
	}
	return reply, nil
}

// 测试RPC
func (rf *Raft) sendRPC(peer int, args interface{}, reply interface{}) bool {
	ctx := context.Background()
	switch args.(type) {
	case *RequestVoteArgs:
		args, reply := args.(*RequestVoteArgs), reply.(*RequestVoteReply)
		resp, err := rf.peers[peer].RequestVote(ctx, args)
		if err != nil {
			return false
		}
		reply.VoteGranted = resp.VoteGranted
		reply.Term = resp.Term
		return true
	case *AppendEntriesArgs:
		args, reply := args.(*AppendEntriesArgs), reply.(*AppendEntriesReply)
		resp, err := rf.peers[peer].AppendEntries(ctx, args)
		if err != nil {
			//log.Printf("[Raft] raft peer%v send AppendEntries RPC error: %s\n", rf.me, err)
			return false
		}
		reply.Term = resp.Term
		reply.Success = resp.Success
		reply.ConflictIndex = resp.ConflictIndex
		reply.ConflictTerm = resp.ConflictTerm
		return true
	case *InstallSnapshotArgs:
		args, reply := args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply)
		resp, err := rf.peers[peer].InstallSnapshot(ctx, args)
		if err != nil {
			//log.Printf("[Raft] raft peer%v send InstallSnapshot RPC error: %s\n", rf.me, err)
			return false
		}
		reply.Term = resp.Term
		return true
	}
	return false
}

// 重设选举定时器
func (rf *Raft) resetElectTimer() {
	rf.timer.d = randElectTime()
	rf.timer.t.Reset(rf.timer.d)
}
// 生成随机超时时间
func randElectTime() time.Duration {
	extra := time.Duration(rand.Intn(ElectionTimeoutExtra)*5) * time.Millisecond
	return ElectionTimeoutBase + extra
}

// 转换状态并投票 持久化
func (rf *Raft) convertToFollower(term int64, peer int32) {
	rf.state = FOLLOWER
	if term != StayTerm {
		rf.currentTerm = term
	}
	rf.votedFor = peer
//	_ = rf.persistState()
}

// 将指令apply
func (rf *Raft) apply() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command: rf.getLog(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
	}
}


// index of logs may be changed for snapshot
func (rf *Raft) getLog(i int64) LogEntry {
	return *rf.logs[i-rf.lastIncludedIndex]
}

func (rf *Raft) subIndex(i int64) int64 {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) lastIndex() int64 {
	return rf.lastIncludedIndex + int64(len(rf.logs)) - 1
}

func (rf *Raft) lastTerm() int64 {
	return rf.logs[len(rf.logs)-1].Term
}