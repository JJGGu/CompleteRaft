package raft

import (
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

const (
	StateFile = "state"
	SnapshotFile = "snapshot"
)

// 用于持久化节点信息和快照
type Persist struct {
	mu sync.RWMutex
	peerId int
	stateFile string
	snapshotFile string
}

func NewPersist(peerId int) *Persist {
	return &Persist{
		mu:           sync.RWMutex{},
		peerId:       peerId,
		stateFile:    StateFile + strconv.Itoa(peerId),
		snapshotFile: SnapshotFile + strconv.Itoa(peerId),
	}
}

// 保存状态
func (ps *Persist) SaveRaftState(state []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ioutil.WriteFile(ps.stateFile, state, 0770)
}

// 保存snapshot
func (ps *Persist) SaveStateAndSnapshot(state, snapshot []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	err := ioutil.WriteFile(ps.stateFile, state, 0700)
	err = ioutil.WriteFile(ps.snapshotFile, snapshot, 0700)
	return err
}

// 读取持久化文件
func (ps *Persist) ReadRaftState() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ioutil.ReadFile(ps.stateFile)
}

// 读取快照持久化文件
func (ps *Persist) ReadSnapshot() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ioutil.ReadFile(ps.snapshotFile)
}

// 计算大小
func (ps *Persist) ReadStateSize() (int, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	data, err := ioutil.ReadFile(ps.stateFile)
	if err != nil {
		return -1, err
	}
	return len(data), nil
}
func (ps *Persist) ReadSnapshotSize() (int, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	data, err := ioutil.ReadFile(ps.snapshotFile)
	if err != nil {
		return -1, err
	}
	return len(data), nil
}

// 文件拷贝
func (ps *Persist) Copy(i int) (*Persist, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	stateFileCopy, snapshotFileCopy := ps.stateFile+"_copy", ps.snapshotFile+"_copy"
	err := copyFile(ps.stateFile, stateFileCopy)
	err = copyFile(ps.snapshotFile, snapshotFileCopy)
	if err != nil {
		return nil, err
	}

	p := &Persist{
		mu:           sync.RWMutex{},
		peerId:       i,
		stateFile:    StateFile + strconv.Itoa(i),
		snapshotFile: SnapshotFile + strconv.Itoa(i),
	}
	return p, nil
}

func copyFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}