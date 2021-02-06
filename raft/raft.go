package raft

import (
	"context"
	"github.com/golang/protobuf/proto"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LEADER = "Leader"
	CANDIDATE = "Candidate"
	FOLLOWER = "Follower"
	// 心跳时间
	HeartbeatTime = 100 * time.Millisecond
	// 选举超时时间 10ms - 500ms
	ElectionTimeoutBase = 400 * time.Millisecond
	ElectionTimeoutExtra = 100
	RpcCallTimeout = HeartbeatTime
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
	// 持久化
	persist *Persist
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

// 节点重新上线（新建）
func NewRaftPeer(me int32, persist *Persist, applyChan chan ApplyMsg, clients []RaftClient) *Raft {
	// 初始化raft节点
	rf := &Raft{
		me: me,
		mu: sync.RWMutex{},
		state: FOLLOWER,
		applyChan: applyChan,
		stopped: false,
		conditions: make([]*sync.Cond, len(clients)),
		persist: persist,
		currentTerm: 0,
		votedFor: VoteNil,
		logs: make([]*LogEntry, 0),
		nextIndex: make([]int64, len(clients)),
		matchIndex: make([]int64, len(clients)),
		peers: clients,
	}
	// 重置Logs
	rf.logs = append(rf.logs, &LogEntry{
		Term: 0,
		Command: nil,
	})
	// 初始化条件变量
	for i := 0; i < len(clients); i++ {
		rf.conditions[i] = sync.NewCond(&rf.mu)
	}
	// 初始化选举超时时间
	timeout := randElectTime()
	rf.timer = &electionTimer{
		d: timeout,
		t: time.NewTimer(timeout),
	}
	// 读取持久化信息
	_ = rf.readPersistedState()
	// 重置commitIndex和ApplyIndex
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	// 初始化nextIndex
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	log.Printf("[Raft] raft peer%v start successfully!\n", me)
	// 开始运行
	go rf.runPeer()

	return rf
}

func (rf *Raft) runPeer() {
	// 持续监听，如果该节点选举超时，开始拉票
	for {
		select {
		case <-rf.timer.t.C:
			rf.run()
		}
	}
}

func (rf *Raft) run() {
	log.Printf("[Raft] raft peer%v become candidate, start request vote!\n", rf.me)
	rf.mu.Lock()
	// 开始新的term进行拉选票
	rf.currentTerm++
	// 改为Candidate状态
	rf.state = CANDIDATE
	// 先将票投给自己
	rf.votedFor = rf.me
	// 重置定时器
	rf.resetElectTimer()
	_ = rf.persistState()
	// 构造RequestVote请求
	index := rf.lastIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: index,                 //自己日志的最新index
		LastLogTerm:  rf.getLog(index).Term, //最新日志的Term
	}
	rf.mu.Unlock()
	// 用于接收每一个peer的response
	replyChan := make(chan RequestVoteReply, len(rf.peers))
	// 用于等待所有RPC完成，同步信号
	var wg sync.WaitGroup
	// 开始给每一个peer发送RequestVote RPC
	for i := range rf.peers {
		// 跳过自己
		if int32(i) == rf.me {
			continue
		}
		wg.Add(1)
		// 开启协程发送rpc
		go func(peer int) {
			defer wg.Done()
			var reply RequestVoteReply
			respChan := make(chan struct{})
			go func() {
				if rf.sendRPC(peer, &args, &reply) {
					respChan <- struct{}{}
				}
			}()
			select {
			// RPC超时
			case <-time.After(RpcCallTimeout):
				return
			case <-respChan:
				// 收到响应
				replyChan <- reply
			}
		}(i)
	}
	// 等待所有RPC完成
	go func() {
		wg.Wait()
		// 关闭channel停止接收数据
		close(replyChan)
	}()
	// 统计replyChan中的reply，收集选票，如果得票超过半数则成为leader，votes初始化为1是因为自己投给自己的一票
	votes, majority := int32(1), int32(len(rf.peers)/2+1)
	for rep := range replyChan { // 当上面goroutine完成后会关闭channel，从而退出循环
		rf.mu.Lock()
		// 如果对方term更大则自己应转为follower
		if rep.Term > rf.currentTerm {
			rf.convertToFollower(rep.Term, VoteNil)
			rf.mu.Unlock()
			return
		}
		// 检查是否仍处于Candidate状态，或者当前 term是否已经更新
		if rf.state != CANDIDATE || rep.Term < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 收集选票，注意原子性
		if rep.VoteGranted {
			atomic.AddInt32(&votes, 1)
		}
		// 如果选票超过半数，转为leader，不用再等待其他选票了
		if atomic.LoadInt32(&votes) >= majority {
			rf.state = LEADER
			rf.mu.Unlock()
			log.Printf("[Raft] raft peer%v become Leader successfully！\n", rf.me)
			// 成为leader后开始给其他Peer追加日志并发出心跳，结束该选举goroutine
			go rf.startAppendEntries()
			go rf.startHeartbeat()
			return
		}
		rf.mu.Unlock()
	}
	// 如果选举失败，转为follower，等待下一次超时选举
	rf.mu.Lock()
	rf.convertToFollower(StayTerm, VoteNil)
	rf.mu.Unlock()
}

// 心跳
func (rf *Raft) startHeartbeat() {
	// 每隔HeartbeatTime传输信息
	ch := time.Tick(HeartbeatTime)
	for {
		// 确保只有leader才有资格发心跳，否则退出
		if !rf.isRunningLeader() {
			return
		}
		// 向其他Peer发送心跳
		for i := range rf.peers {
			if int32(i) == rf.me {
				// 注意发出心跳的同时应重置定时器，否则会导致新一轮的选举
				rf.mu.Lock()
				rf.resetElectTimer()
				rf.mu.Unlock()
				continue
			}
			// 通知其他Peer 心跳到达
			rf.conditions[i].Broadcast()
		}
		// 等待下一次心跳时间到来
		<-ch
	}
}

// 日志追加
func (rf *Raft) startAppendEntries() {
	for i := range rf.peers {
		// 跳过自己
		if int32(i) == rf.me {
			continue
		}
		// 开启新的goroutine来发送AppendEntries RPC
		go func(peer int) {
			for {
				// 只有leader才有资格发送该RPC
				if !rf.isRunningLeader() {
					return
				}
				rf.mu.Lock()
				// 等待心跳或新的日志到来，心跳和日志共用该RPC
				rf.conditions[peer].Wait()

				// 同步到最新的日志，如果是心跳的话就没有日志可发了
				next := rf.nextIndex[peer]

				// 如果下一个需发送的log落后于上一次快照的位置，说明落后太多，可能断开重连了，此时直接发送快照，加快同步
				if next <= rf.lastIncludedIndex {
					log.Printf("next: %v, lastIncludedIndex: %v", next, rf.lastIncludedIndex)
					rf.startInstallSnapshot(peer)
					continue
				}
				// 构造AppendEntries RPC 参数
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				// 检查是否有日志未发送
				if next < rf.logLength() {
					// 上次同步的日志的位置
					args.PrevLogIndex = next - 1
					args.PrevLogTerm = rf.getLog(next - 1).Term
					// 加入需同步的日志
					args.Entries = append(args.Entries, rf.logs[rf.subIndex(next):]...)
				}
				rf.mu.Unlock()

				var reply AppendEntriesReply
				respCh := make(chan struct{})
				// 开启一个新的goroutine发送RPC
				go func() {
					//if rf.sendAppendEntries(peer, &args, &reply) {
					if rf.sendRPC(peer, &args, &reply) {
						respCh <- struct{}{}
					}
				}()
				// 等待RPC发送结果
				select {
				case <-time.After(RpcCallTimeout):
					// 超时会返回重新发送
					continue
				case <-respCh:
					close(respCh)
				}

				// 如果日志位置匹配失败，也就是出现了冲突
				if !reply.Success {
					rf.mu.Lock()
					// 检查term
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term, VoteNil)
						rf.mu.Unlock()
						return
					}
					//检查自己的状态
					if rf.state != LEADER || reply.Term < rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					// 根据冲突的index，回退，发送最后一条匹配的日志位置后的全部日志
					if reply.ConflictIndex > 0 {
						rf.mu.Lock()
						firstConflict := reply.ConflictIndex
						if reply.ConflictTerm != TermNil {
							lastIdx := rf.lastIndex()
							// 找到匹配的term中的最后一条日志，之后发送后面的全部term的日志
							for i := rf.addIndex(0); i <= lastIdx; i++ {
								if rf.getLog(i).Term != reply.ConflictTerm {
									continue
								}
								for i <= lastIdx && rf.getLog(i).Term == reply.ConflictTerm {
									i++
								}
								// 匹配的term中的最后一条日志
								firstConflict = i
								break
							}
						}
						// 更新下一次待发送的位置
						rf.nextIndex[peer] = firstConflict
						rf.mu.Unlock()
					}
					// 跳过，开始重新发送
					continue
				}

				// 如果追加成功，更新index
				if len(args.Entries) > 0 {
					rf.mu.Lock()
					// 更新index
					rf.matchIndex[peer] = args.PrevLogIndex + int64(len(args.Entries))
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.updateCommitIndex()
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// 快照
func (rf *Raft) startInstallSnapshot(peer int) {
	// 确保只有leader才可以发送snapshot
	if rf.state != LEADER || rf.stopped {
		rf.mu.Unlock()
		return
	}

	// 读取快找，构造发送请求
	data, _ := rf.persist.ReadSnapshot()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              data,
	}
	rf.mu.Unlock()
	log.Printf("[Raft] raft peer%v start to send InstallSnapshot RPC\n", rf.me)

	// 发送RPC并等待返回结果
	var reply InstallSnapshotReply
	respCh := make(chan struct{})
	go func() {
		//if rf.sendInstallSnapshot(peer, &args, &reply){
		if rf.sendRPC(peer, &args, &reply) {
			respCh <- struct{}{}
		}
	}()
	select {
	case <-time.After(RpcCallTimeout):
		return
	case <-respCh:
		close(respCh)
	}

	// 处理返回结果
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 检查term
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term, VoteNil)
		return
	}
	// 检查自己的状态
	if rf.state != LEADER || reply.Term < rf.currentTerm { // curTerm changed already
		return
	}

	// 更新index
	rf.matchIndex[peer] = args.LastIncludedIndex
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
}



// 当半数Peer追加了日志，则可以commit
func (rf *Raft) updateCommitIndex() {
	n := len(rf.peers)
	matched := make([]int64, n)
	for i := range matched {
		matched[i] = rf.matchIndex[i]
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i] > matched[j]
	})
	// 半数以上已经append，则更新commitIndex
	if N := matched[n/2]; N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.apply()
	}
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

// 读取当前Peer的状态，返回当前term以及是否为leader
func (rf *Raft) GetState() (int64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == LEADER
}

// 关闭
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.stopped = true
	rf.mu.Unlock()
}

func (rf *Raft) logLength() int64 {
	return rf.lastIndex() + 1
}

func (rf *Raft) addIndex(i int64) int64 {
	return rf.lastIncludedIndex + i
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
// 检查raft是否启动以及是否是leader
func (rf *Raft) isRunningLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == LEADER && !rf.stopped
}
// 节点信息持久化 存储到持久化文件中
func (rf *Raft) persistState() error {
	pb := &State{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Logs:              rf.logs,
	}
	// serialize peer's state by proto
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	// persist data to file
	return rf.persist.SaveRaftState(data)
}

// 读取持久化文件
func (rf *Raft) readPersistedState() error {
	var pb State
	// read state from persisted file
	data, err := rf.persist.ReadRaftState()
	if err != nil {
		return err
	}
	// proto unmarshal data
	if err = proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	// set raft peer's state
	rf.currentTerm = pb.CurrentTerm
	rf.votedFor = pb.VotedFor
	rf.lastIncludedIndex = pb.LastIncludedIndex
	rf.lastIncludedTerm = pb.LastIncludedTerm
	rf.logs = pb.Logs
	return nil
}