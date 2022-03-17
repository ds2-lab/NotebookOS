// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package smr

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

type StateValueCallback func([]byte)
type ShouldLogNodeCallback func(*LogNode) bool
type WriteCallback func(WriteCloser)

type LogNodeConfig struct {
	onChange       StateValueCallback
	onRestore      StateValueCallback
	shouldSnapshot ShouldLogNodeCallback
	getSnapshot    WriteCallback
}

func NewConfig() *LogNodeConfig {
	return &LogNodeConfig{}
}

func (conf *LogNodeConfig) WithChangeCallback(cb StateValueCallback) *LogNodeConfig {
	conf.onChange = cb
	return conf
}

func (conf *LogNodeConfig) WithRestoreCallback(cb StateValueCallback) *LogNodeConfig {
	conf.onRestore = cb
	return conf
}

func (conf *LogNodeConfig) WithShouldSnapshotCallback(cb ShouldLogNodeCallback) *LogNodeConfig {
	conf.shouldSnapshot = cb
	return conf
}

func (conf *LogNodeConfig) WithSnapshotCallback(cb WriteCallback) *LogNodeConfig {
	conf.getSnapshot = cb
	return conf
}

// A SyncLog backed by raft
type LogNode struct {
	proposeC    chan []byte            // proposed serialized messages
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	errorC      chan error             // errors from raft session

	id      int      // client ID for raft session
	peers   []string // raft peer URLs
	join    bool     // node is joining an existing cluster
	waldir  string   // path to WAL directory
	snapdir string   // path to snapshot directory

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64
	num_changes   uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	// Bridges
	config *LogNodeConfig

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

// NewLogNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewLogNode(store_path string, id int, peers []string, join bool) *LogNode {
	node := &LogNode{
		proposeC:    make(chan []byte),
		confChangeC: make(chan raftpb.ConfChange),
		errorC:      make(chan error),
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      path.Join(store_path, fmt.Sprintf("dnlog-%d", id)),
		snapdir:     path.Join(store_path, fmt.Sprintf("dnlog-%d-snap", id)),
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	return node
}

func (node *LogNode) NumChanges() int {
	return int(node.num_changes)
}

func (node *LogNode) Start(config *LogNodeConfig) {
	node.config = config
	go node.start()
}

//  Append the difference of the value of specified key to the synchronization queue.
func (node *LogNode) Append(val []byte) {
	node.proposeC <- val
}

func (node *LogNode) AddNode(id int, addr string) {
	node.confChangeC <- raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(id),
		Context: []byte(addr),
	}
}

func (node *LogNode) RemoveNode(id int) {
	node.confChangeC <- raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: uint64(id),
	}
}

func (node *LogNode) WaitToClose() error {
	return <-node.errorC
}

func (node *LogNode) Close() error {
	close(node.proposeC)
	close(node.confChangeC)
	return node.WaitToClose()
}

func (node *LogNode) start() {
	if !fileutil.Exist(node.snapdir) {
		if err := os.Mkdir(node.snapdir, 0750); err != nil {
			log.Fatalf("LogNode: cannot create dir for snapshot (%v)", err)
		}
	}
	node.snapshotter = snap.New(zap.NewExample(), node.snapdir)

	oldwal := wal.Exist(node.waldir)
	node.wal = node.replayWAL()

	// signal replay has finished
	node.snapshotterReady <- node.snapshotter

	rpeers := make([]raft.Peer, len(node.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(node.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   node.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal || node.join {
		node.node = raft.RestartNode(c)
	} else {
		node.node = raft.StartNode(c, rpeers)
	}

	node.transport = &rafthttp.Transport{
		Logger:      node.logger,
		ID:          types.ID(node.id),
		ClusterID:   0x1000,
		Raft:        node,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(node.id)),
		ErrorC:      make(chan error),
	}

	node.transport.Start()
	for i := range node.peers {
		if i+1 != node.id {
			node.transport.AddPeer(types.ID(i+1), []string{node.peers[i]})
		}
	}

	go node.serveRaft()
	go node.serveChannels()
}

func (node *LogNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := node.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := node.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return node.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (node *LogNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > node.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, node.appliedIndex)
	}
	if node.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[node.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (node *LogNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([][]byte, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			data = append(data, ents[i].Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			node.confState = *node.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					node.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(node.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				node.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{})
		go func() {
			for _, d := range data {
				node.config.onChange(d)
			}
			close(applyDoneC)
		}()

		node.num_changes += uint64(len(data))
	}

	// after commit, update appliedIndex
	node.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (node *LogNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(node.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(node.logger, node.waldir)
		if err != nil {
			log.Fatalf("LogNode: error listing snapshots (%v)", err)
		}
		snapshot, err := node.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("LogNode: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (node *LogNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(node.waldir) {
		if err := os.Mkdir(node.waldir, 0750); err != nil {
			log.Fatalf("LogNode: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), node.waldir, nil)
		log.Printf("Created wal at %s: %v, %v", node.waldir, w, err)
		if err != nil {
			log.Fatalf("LogNode: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), node.waldir, walsnap)
	if err != nil {
		log.Fatalf("LogNode: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (node *LogNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", node.id)
	snapshot := node.loadSnapshot()
	// Restore kernal from snapshot
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		if node.config.onRestore == nil {
			log.Fatalf("no RestoreCallback configured on start.")
		}
		node.config.onRestore(snapshot.Data)
	}

	w := node.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	node.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		node.raftStorage.ApplySnapshot(*snapshot)
	}
	node.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	log.Printf("Read %d entries on replay wal: %v", len(ents), ents)
	node.raftStorage.Append(ents)

	return w
}

func (node *LogNode) writeError(err error) {
	node.stopHTTP()
	node.errorC <- err
	close(node.errorC)
	node.node.Stop()
}

// stop closes http, closes all channels, and stops raft.
func (node *LogNode) stop() {
	node.stopHTTP()
	close(node.errorC)
	node.node.Stop()
}

func (node *LogNode) stopHTTP() {
	node.transport.Stop()
	close(node.httpstopc)
	<-node.httpdonec
}

func (node *LogNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", node.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", node.snapshotIndex)

	if snapshotToSave.Metadata.Index <= node.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, node.appliedIndex)
	}

	// Restore kernal from snapshot
	if node.config.onRestore == nil {
		log.Fatalf("no RestoreCallback configured on start.")
	}
	node.config.onRestore(snapshotToSave.Data)

	node.confState = snapshotToSave.Metadata.ConfState
	node.snapshotIndex = snapshotToSave.Metadata.Index
	node.appliedIndex = snapshotToSave.Metadata.Index
	node.num_changes = 0
}

var snapshotCatchUpEntriesN uint64 = 1000

func (node *LogNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if node.config.getSnapshot == nil || // No snapshotCallback
		(node.config.shouldSnapshot == nil && node.appliedIndex-node.snapshotIndex <= node.snapCount) || // No shouldSnapshot and failed default test.
		(node.config.shouldSnapshot != nil && (node.num_changes == 0 || !node.config.shouldSnapshot(node))) { // Failed shouldSnapshot test
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-node.stopc:
			return
		}
	}

	// create pipeline and write
	reader, writer := io.Pipe()
	go node.config.getSnapshot(&writeCloserWrapper{writer: writer})

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", node.appliedIndex, node.snapshotIndex)
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Panic(err)
	}
	snap, err := node.raftStorage.CreateSnapshot(node.appliedIndex, &node.confState, data)
	if err != nil {
		panic(err)
	}
	if err := node.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := node.appliedIndex
	if node.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = node.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := node.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	node.snapshotIndex = node.appliedIndex
	node.num_changes = 0
}

func (node *LogNode) serveChannels() {
	snap, err := node.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	node.confState = snap.Metadata.ConfState
	node.snapshotIndex = snap.Metadata.Index
	node.appliedIndex = snap.Metadata.Index

	defer node.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for node.proposeC != nil && node.confChangeC != nil {
			select {
			case val, ok := <-node.proposeC:
				if !ok {
					node.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					node.node.Propose(context.TODO(), val)
				}

			case cc, ok := <-node.confChangeC:
				if !ok {
					node.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					node.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(node.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			node.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-node.node.Ready():
			// log.Printf("ready snapshot: %v", rd.Snapshot)
			// log.Printf("ready entris: %v", rd.Entries)
			// log.Printf("ready message: %v", rd.Messages)
			// log.Printf("ready commited: %v", rd.CommittedEntries)
			node.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				node.saveSnap(rd.Snapshot)
				node.raftStorage.ApplySnapshot(rd.Snapshot)
				node.publishSnapshot(rd.Snapshot)
			}
			node.raftStorage.Append(rd.Entries)
			node.transport.Send(node.processMessages(rd.Messages))
			applyDoneC, ok := node.publishEntries(node.entriesToApply(rd.CommittedEntries))
			if !ok {
				node.stop()
				return
			}
			node.maybeTriggerSnapshot(applyDoneC)
			node.node.Advance()

		case err := <-node.transport.ErrorC:
			node.writeError(err)
			return

		case <-node.stopc:
			node.stop()
			return
		}
	}
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (node *LogNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = node.confState
		}
	}
	return ms
}

func (node *LogNode) serveRaft() {
	url, err := url.Parse(node.peers[node.id-1])
	if err != nil {
		log.Fatalf("LogNode: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, node.httpstopc)
	if err != nil {
		log.Fatalf("LogNode: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: node.transport.Handler()}).Serve(ln)
	select {
	case <-node.httpstopc:
	default:
		log.Fatalf("LogNode: Failed to serve rafthttp (%v)", err)
	}
	close(node.httpdonec)
}

func (node *LogNode) Process(ctx context.Context, m raftpb.Message) error {
	return node.node.Step(ctx, m)
}
func (node *LogNode) IsIDRemoved(id uint64) bool  { return false }
func (node *LogNode) ReportUnreachable(id uint64) { node.node.ReportUnreachable(id) }
func (node *LogNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	node.node.ReportSnapshot(id, status)
}
