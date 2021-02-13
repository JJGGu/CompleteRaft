package raft

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

const ElectionTimeout = time.Second

// 测试初始化后的正常情况下的选举
func TestInitialElection(t *testing.T) {
	//addrList := []string{"127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003"}
	addrList := []string{"127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003", "127.0.0.1:50004", "127.0.0.1:50005"}

	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(len(addrList))
	cfg.begin("-----START TESTING------")
	// 测试是否选举出一个LEADER
	leader := cfg.checkOneLeader()
	fmt.Println("LEADER_ID:", leader)

	// 测试heartbeat, 经过一段时间后任期是否相同
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	fmt.Println("term1:", term1)

	time.Sleep(5 * ElectionTimeout)
	term2 := cfg.checkTerms()
	fmt.Println("term2:", term2)

	// 打印相关测试信息
	cfg.end()
}

func TestReElection(t *testing.T) {
	addrList := []string{"127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003"}
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(len(addrList))
	cfg.begin("-----START TESTING------")

	// 网络断开前的leader
	leader1 := cfg.checkOneLeader()
	fmt.Println("Leader1: " , leader1)

	// 测试该leader断开连接后是否新的leader被选举出来
	fmt.Println("LEADER断开连接")
	cfg.disconnect(leader1)
	leader2 := cfg.checkOneLeader()
	fmt.Println("Leader2: ", leader2)

	//原leader重新加入，不应该影响当前的leader
	fmt.Println("LEADER重新连接")
	cfg.connect(leader1)
	leader3 := cfg.checkOneLeader()
	fmt.Println("Leader3: ", leader3)

	// 断开两个server后，不应该有leader被选举出来
	fmt.Println("断开两个Server")
	cfg.disconnect(leader3)
	cfg.disconnect((leader3 + 1) % len(addrList))
	time.Sleep(2 * ElectionTimeout)
	cfg.checkNoLeader()

	//如果重新加入了一个server，应该选举出一个leader
	fmt.Println("重新加入一个")
	cfg.connect((leader2 + 1) % len(addrList))
	cfg.checkOneLeader()

	cfg.end()
}

func TestAppendEntries(t *testing.T)  {
	addrList := []string{"127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003", "127.0.0.1:50004", "127.0.0.1:50005"}

	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(len(addrList))
	cfg.begin("-----START TESTING------")


}

type config struct {
	mu         sync.Mutex
	t          *testing.T
	n          int
	rafts      []*Raft
	applyErr   []string
	connected  []bool
	addrList   []string
	saved      []*Persist
	logs       []map[int]int //server已经提交的日志的copy
	startTime  time.Time
	t0         time.Time // 初始时间
	cmds0      int       // 初始日志数
	maxIndex   int       // 最大的entry的index
	maxIndex0  int       // 初始的最大entry的index
	longDelays bool
	clients    []RaftClient
	listens    []net.Listener
	servers    []*grpc.Server
}

func makeConfig(t *testing.T, addrList []string) *config {
	// 设置可并发使用的最大CPU数目
	runtime.GOMAXPROCS(4)
	n := len(addrList)
	// 初始化config
	cfg := &config{
		mu:         sync.Mutex{},
		t:          t,
		n:          n,
		rafts:      make([]*Raft, n),
		applyErr:   make([]string, n),
		connected:  make([]bool, n),
		saved:      make([]*Persist, n),
		logs:       make([]map[int]int, n),
		startTime:  time.Now(),
		longDelays: true,
		addrList:   make([]string, n),
		listens:    make([]net.Listener, n),
		servers:    make([]*grpc.Server, n),
	}
	copy(cfg.addrList, addrList)
	clients := make([]RaftClient, len(cfg.addrList))
	for i := range cfg.addrList {
		conn, err := grpc.Dial(cfg.addrList[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("start conn error")
		}
		clients[i] = NewRaftClient(conn)
	}
	cfg.clients = clients

	// 创建raft servers
	for i := 0; i < n; i++ {
		cfg.logs[i] = make(map[int]int)
		cfg.start(i)
	}

	// 连接server
	for i := 0; i < n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// 创建一个raft server
func (c *config) start(i int) {
	// 如果已经有了server，先关闭它
	c.crash(i)
	// 创建persist
	c.mu.Lock()
	if c.saved[i] != nil {
		persist, _ := c.saved[i].Copy(i)
		c.saved[i] = persist
	} else {
		c.saved[i] = NewPersist(i)
	}
	c.mu.Unlock()
	// 监听来自raft的消息，代表已经committed过的消息
	applyChan := make(chan ApplyMsg)
	go func() {
		for m := range applyChan {
			errMsg := ""
			if !m.CommandValid {
				// 忽略该类型的消息
			} else if v, err := strconv.Atoi(string(m.Command)); err == nil {
				c.mu.Lock()
				for j := 0; j < len(c.logs); j++ {
					// 如果日志内容对不上
					if old, oldOk := c.logs[j][int(m.CommandIndex)]; oldOk && old != v {
						errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v", m.CommandIndex, i, v, j, old)
					}
				}
				_, prevOk := c.logs[i][int(m.CommandIndex)-1]
				c.logs[i][int(m.CommandIndex)] = v
				if int(m.CommandIndex) > c.maxIndex {
					c.maxIndex = int(m.CommandIndex)
				}
				c.mu.Unlock()

				// 如果顺序对不上
				if m.CommandIndex > 1 && !prevOk {
					errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			} else {
				// 类型错误
				errMsg = fmt.Sprintf("committed command %v is not an int", m.Command)
			}

			if errMsg != "" {
				log.Fatalf("apply error: %v\n", errMsg)
			}
		}
	}()

	// 创建raft Peer
	rf := NewRaftPeer(int32(i), c.saved[i], applyChan, c.clients)
	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()

	// 注册grpc server
	// 注册gRPC server，创建raft
	addr := c.addrList[i]
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	c.listens[i] = listen
	log.Printf("listen %s successfully!", addr)
	s := grpc.NewServer()
	RegisterRaftServer(s, rf)
	c.servers[i] = s
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
}

// 关闭一个raft server，并保存它的状态
func (c *config) crash(i int) {
	// 终止连接
	c.disconnect(i)
	if c.servers[i] != nil {
		c.servers[i].Stop()
		c.listens[i].Close()
	}

	//复制一个persist，不影响原来的persist
	if c.saved[i] != nil {
		cc, _ := c.saved[i].Copy(i)
		c.mu.Lock()
		c.saved[i] = cc
		c.mu.Unlock()
	}
	// 终止raft
	rf := c.rafts[i]
	if rf != nil {
		rf.Kill()
		c.mu.Lock()
		c.rafts[i] = nil
		c.mu.Unlock()
	}
	//保存原来的state
	if c.saved[i] != nil {
		raftLog, _ := c.saved[i].ReadSnapshot()
		c.mu.Lock()
		c.saved[i] = NewPersist(i)
		c.mu.Unlock()
		c.saved[i].SaveRaftState(raftLog)
	}
}

// 建立连接
func (c *config) connect(i int) {
	c.connected[i] = true
	// 建立向内和向外的连接
	c.rafts[i].networkDrop = false
}

// 断开连接
func (c *config) disconnect(i int) {
	c.connected[i] = false
	// 断开向内和向外的连接，true表示网络不可用
	if c.rafts[i] != nil {
		c.rafts[i].networkDrop = true
	}
}

// 启用不可靠网络
func (c *config) unreliableConnect(i int) {
	if c.rafts[i] != nil {
		c.rafts[i].networkUnreliable = true
	}
}

func (c *config) reliableConnect(i int) {
	if c.rafts[i] != nil {
		c.rafts[i].networkUnreliable = false
	}
}

// 执行清理工作
func (c *config) cleanup() {
	// 终止每一个raft server
	for i := 0; i < len(c.rafts); i++ {
		if c.rafts[i] != nil {
			c.rafts[i].Kill()
		}
	}
	// 检查是否超时
	c.checkTimeout()
}

// 检查是否超时，限制超时时间为2分钟
func (c *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !c.t.Failed() && time.Since(c.startTime) > 120*time.Second {
		c.t.Fatal("test took longer than 120 seconds")
	}
}

//开始一个test
func (c *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	c.t0 = time.Now()
	c.cmds0 = 0
	c.maxIndex0 = c.maxIndex
}

// 检查是否只有一个leader，re-election的情况下重试多次
func (c *config) checkOneLeader() int {
	for iter := 0; iter < 10; iter++ {
		ms := 450 + rand.Int63()%100
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// 遍历每一个server，记录其term是否为leader
		leaders := make(map[int64][]int)
		for i := 0; i < c.n; i++ {
			if c.connected[i] {
				if term, leader := c.rafts[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := int64(-1)
		for term, leaderList := range leaders {
			// 如果某个term出现了不止一个leader，报错
			if len(leaderList) > 1 {
				c.t.Fatalf("term %d has %d (>1) leaders", term, len(leaderList))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		// 返回最后一个term的leader
		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	// 如果没有选出leader，报错
	c.t.Fatalf("expected one leader, get none")
	return -1
}

// 检查是否没有leader
func (c *config) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			_, isLeader := c.rafts[i].GetState()
			if isLeader {
				c.t.Fatalf("expected no leader, but %v claims to be leader", i)
			}
		}
	}
}

// 检查是否每一个raft节点的任期是否相同
func (c *config) checkTerms() int64 {
	term := int64(-1)
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			xterm, _ := c.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				c.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

func cleanStateFile(n int) {
	for i := 0; i < n; i++ {
		_ = os.Remove("state" + strconv.Itoa(i))
		_ = os.Remove("state" + strconv.Itoa(i) + "_copy")
	}
}

//检查多少个server认为一个日志committed
func (c *config) nCommitted(index int) (int, interface{}) {
	count := 0
	cmd := -1
	for i := 0; i < len(c.rafts); i++ {
		if c.applyErr[i] != "" {
			c.t.Fatal(c.applyErr[i])
		}
		// 查看对应的Peer的日志，如果存在指定index，则计数++
		c.mu.Lock()
		cmd1, ok := c.logs[i][index]
		c.mu.Unlock()
		if ok {
			// 如果日志内容不一致，报错
			if count > 0 && cmd != cmd1 {
				c.t.Fatalf("committed value do not match: index %v, %v, %v\n", index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}
// 执行一次完整的agreement。一开始可能会选错leader，提交失败后会进行重试，失败超过10s会结束
//
func (c *config) one(cmd int, expectedServers int, retry bool) int64 {
	t0 := time.Now()
	starts := 0
	// 执行时间超所10秒就返回
	for time.Since(t0).Seconds() < 10 {
		// 尝试所有的server，可能其中之一是leader
		index := int64(-1)
		for si := 0; si < c.n; si++ {
			starts = (starts + 1) % c.n
			var rf *Raft
			c.mu.Lock()
			if c.connected[starts] {
				rf = c.rafts[starts]
			}
			c.mu.Unlock()
			if rf != nil {
				// 如果是leader且提交成功了
				index1, _, ok := rf.Submit([]byte(strconv.Itoa(cmd)))
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// 如果已经有leader并提交了该cmd，等待agreement
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				// 寻找提交记录
				nd, cmd1 := c.nCommitted(int(index))
				if nd > 0 && nd >= expectedServers {
					if cmd2, ok := cmd1.(int); ok && cmd2 == cmd {
						// 是我们提交的command
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			// 是否重试
			if retry == false {
				log.Println(index)
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}
//  end a test -- 能够到达这里说明通过了测试，打印相关信息
func (c *config) end() {
	c.checkTimeout()
	if c.t.Failed() == false {
		c.mu.Lock()
		t := time.Since(c.t0).Seconds()    // 测试所用的时间
		peerNum := c.n                     // peer数
		cmdNum := c.maxIndex - c.maxIndex0 // 执行的cmd 日志数目
		c.mu.Unlock()

		fmt.Printf("--- Passed ---")
		fmt.Printf("time:  %4.1f  peerNum: %d  cmdNum: %4d\n", t, peerNum, cmdNum)
	}
}
