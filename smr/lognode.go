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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/google/uuid"
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

var (
	ProposalDeadline = 1 * time.Minute
	ErrEOF           = io.EOF.Error() // For python module to check if io.EOF is returned
)

type StateValueCallback func(ReadCloser, int, string) string
type StatesValueCallback func(ReadCloser, int) string
type ShouldLogNodeCallback func(*LogNode) bool
type WriteCallback func(WriteCloser) string
type ResolveCallback func(interface{})

func returnError(cbRet string) error {
	if len(cbRet) == 0 {
		return nil
	} else {
		return errors.New(cbRet)
	}
}

type LogNodeConfig struct {
	ElectionTick  int
	HeartbeatTick int

	onChange       StateValueCallback
	onRestore      StatesValueCallback
	shouldSnapshot ShouldLogNodeCallback
	getSnapshot    WriteCallback
}

func NewConfig() *LogNodeConfig {
	return &LogNodeConfig{
		ElectionTick:  10,
		HeartbeatTick: 1,
	}
}

func (conf *LogNodeConfig) WithChangeCallback(cb StateValueCallback) *LogNodeConfig {
	conf.onChange = cb
	return conf
}

func (conf *LogNodeConfig) WithRestoreCallback(cb StatesValueCallback) *LogNodeConfig {
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

type commit struct {
	data       [][]byte
	applyDoneC chan<- struct{}
}

// A SyncLog backed by raft
type LogNode struct {
	proposeC    chan *proposalContext  // proposed serialized messages
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan *commit
	errorC      chan error // errors from raft session

	id      int      // client ID for raft session
	peers   []string // raft peer URLs
	join    bool     // node is joining an existing cluster
	waldir  string   // path to WAL directory
	snapdir string   // path to snapshot directory

	confState        raftpb.ConfState
	snapshotIndex    uint64
	appliedIndex     uint64
	num_changes      uint64
	denied_changes   uint64
	proposalPadding  int
	proposalRegistry map[string]*proposalContext

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
		proposeC:         make(chan *proposalContext),
		confChangeC:      make(chan raftpb.ConfChange),
		commitC:          make(chan *commit),
		errorC:           make(chan error),
		id:               id,
		peers:            peers,
		join:             join,
		waldir:           path.Join(store_path, fmt.Sprintf("dnlog-%d", id)),
		snapdir:          path.Join(store_path, fmt.Sprintf("dnlog-%d-snap", id)),
		snapCount:        defaultSnapshotCount,
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		num_changes:      0,
		denied_changes:   0,
		proposalRegistry: make(map[string]*proposalContext),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	testId, _ := node.generateProposal(nil, 0)
	node.proposalPadding = len(testId)
	return node
}

func (node *LogNode) NumChanges() int {
	return int(node.num_changes)
}

func (node *LogNode) Start(config *LogNodeConfig) {
	node.config = config
	go node.start()
}

func (node *LogNode) StartAndWait(config *LogNodeConfig) {
	node.Start(config)
	node.WaitToClose()
}

//  Append the difference of the value of specified key to the synchronization queue.
func (node *LogNode) Propose(val Bytes, resolve ResolveCallback, msg string) {
	go node.propose(val.Bytes(), resolve, msg)
}

func (node *LogNode) propose(val []byte, resolve ResolveCallback, msg string) {
	_, ctx := node.generateProposal(val, ProposalDeadline)
	node.logger.Debug("Appending value", zap.String("key", msg), zap.String("id", ctx.Id))
	node.proposeC <- ctx
	// Wait for committed or retry
	for !node.waitProposal(ctx) {
		node.logger.Debug("Retry appending value", zap.String("key", msg), zap.String("id", ctx.Id))
		ctx = ctx.Reset(ProposalDeadline)
		node.proposeC <- ctx
	}
	node.logger.Debug("Value appended", zap.String("key", msg), zap.String("id", ctx.Id))
	if resolve != nil {
		resolve(msg)
	}
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
			node.logFatalf("LogNode: cannot create dir for snapshot (%v)", err)
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
		ElectionTick:              node.config.ElectionTick,
		HeartbeatTick:             node.config.HeartbeatTick,
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
	node.serveChannels()
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
		node.logFatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, node.appliedIndex)
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
		select {
		case node.commitC <- &commit{data, applyDoneC}:
		case <-node.stopc:
			return nil, false
		}

		// after commit, update num_changes
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
			node.logFatalf("LogNode: error listing snapshots (%v)", err)
		}
		snapshot, err := node.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			node.logFatalf("LogNode: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (node *LogNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(node.waldir) {
		if err := os.Mkdir(node.waldir, 0750); err != nil {
			node.logFatalf("LogNode: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), node.waldir, nil)
		if err != nil {
			node.logFatalf("LogNode: create wal error (%v)", err)
		}
		w.Close()
		node.logger.Info("Created wal direcotry", zap.String("uri", node.waldir))
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(zap.NewExample(), node.waldir, walsnap)
	if err != nil {
		node.logFatalf("LogNode: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (node *LogNode) replayWAL() *wal.WAL {
	snapshot := node.loadSnapshot()
	// Restore kernal from snapshot
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		if node.config.onRestore == nil {
			node.logFatalf("no RestoreCallback configured on start.")
		}
		if err := returnError(node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshot.Data)}, len(snapshot.Data))); err != nil {
			node.logFatalf("LogNode: failed to restore states (%v)", err)
		}
	}

	w := node.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		node.logFatalf("LogNode: failed to read WAL (%v)", err)
	}
	node.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		node.raftStorage.ApplySnapshot(*snapshot)
	}
	node.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
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

	node.logger.Info("publishing snapshot", zap.Uint64("index", node.snapshotIndex))
	defer node.logger.Info("finished publishing snapshot", zap.Uint64("index", node.snapshotIndex))

	if snapshotToSave.Metadata.Index <= node.appliedIndex {
		node.logFatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, node.appliedIndex)
	}

	// Restore kernal from snapshot
	if node.config.onRestore == nil {
		node.logFatalf("no RestoreCallback configured on start.")
	}
	node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshotToSave.Data)}, len(snapshotToSave.Data))

	node.confState = snapshotToSave.Metadata.ConfState
	node.snapshotIndex = snapshotToSave.Metadata.Index
	node.appliedIndex = snapshotToSave.Metadata.Index
	node.num_changes = 0
}

var snapshotCatchUpEntriesN uint64 = 1000

func (node *LogNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if node.config.getSnapshot == nil || // No snapshotCallback
		(node.config.shouldSnapshot == nil && node.appliedIndex-node.snapshotIndex <= node.snapCount) { // No shouldSnapshot and failed default test.
		return
	} else if node.config.shouldSnapshot != nil && (node.num_changes == node.denied_changes || !node.config.shouldSnapshot(node)) { // Failed shouldSnapshot test
		// Update denied_changes to avoid triggering snapshot too frequently.
		node.denied_changes = node.num_changes
		return
	}

	node.logger.Debug("should snapshot passed")

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-node.stopc:
			return
		}
	}

	// Double confirm the snapshot decision. (e.g. No more values to be proposed before snapshotting)
	if node.config.shouldSnapshot != nil && !node.config.shouldSnapshot(node) {
		return
	}

	// create pipeline and write
	reader, writer := io.Pipe()
	go func() {
		if err := returnError(node.config.getSnapshot(&writerWrapper{writer: writer})); err != nil {
			panic(err)
		}
	}()

	node.logger.Info("start snapshot", zap.Uint64("applied index", node.appliedIndex), zap.Uint64("last index", node.snapshotIndex))
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

	node.logger.Info("compacted log", zap.Uint64("index", compactIndex))
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
			case ctx, ok := <-node.proposeC:
				if !ok {
					node.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					node.node.Propose(ctx, ctx.Proposal)
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

	// apply commits to state machine
	go func() {
		for {
			select {
			case commit := <-node.commitC:
				for _, d := range commit.data {
					realData, ctx := node.doneProposal(d)
					id := ""
					if ctx != nil {
						id = ctx.Id
						ctx.Trigger(func() {
							if err := returnError(node.config.onChange(&readerWrapper{reader: bytes.NewBuffer(realData)}, len(realData), id)); err != nil {
								node.logFatalf("LogNode: Error on replay state (%v)", err)
							}
							ctx.Cancel()
						})
					} else {
						if err := returnError(node.config.onChange(&readerWrapper{reader: bytes.NewBuffer(realData)}, len(realData), id)); err != nil {
							node.logFatalf("LogNode: Error on replay state (%v)", err)
						}
					}
				}
				close(commit.applyDoneC)
			case <-node.stopc:
				return
			}
		}
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			node.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-node.node.Ready():
			// node.logger.Info("ready", zap.Int("entries", len(rd.CommittedEntries)), zap.Uint64("commit", rd.HardState.Commit))
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
		node.logFatalf("LogNode: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, node.httpstopc)
	if err != nil {
		node.logFatalf("LogNode: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: node.transport.Handler()}).Serve(ln)
	select {
	case <-node.httpstopc:
	default:
		node.logFatalf("LogNode: Failed to serve rafthttp (%v)", err)
	}
	close(node.httpdonec)
}

func (node *LogNode) generateProposal(val []byte, timeout time.Duration) ([]byte, *proposalContext) {
	id := uuid.New().String()
	if len(val) == 0 {
		return []byte(id), nil
	}
	ret := val
	if cap(ret) < len(val)+len(id) {
		ret = make([]byte, 0, len(val)+len(id))
		copy(ret[:len(val)], val)
	}
	copy(ret[len(val):len(val)+len(id)], []byte(id))
	ret = ret[:len(val)+len(id)]
	ctx := ProposalContext(id, ret, timeout)
	node.registerProposal(ctx)
	return ret, ctx
}

func (node *LogNode) registerProposal(ctx *proposalContext) {
	node.proposalRegistry[ctx.Id] = ctx
}

func (node *LogNode) doneProposal(val []byte) ([]byte, *proposalContext) {
	id := val[len(val)-node.proposalPadding:]
	ret := val[:len(val)-node.proposalPadding]
	if ctx, ok := node.proposalRegistry[string(id)]; ok {
		delete(node.proposalRegistry, ctx.Id)
		return ret, ctx
	}
	return ret, nil
}

func (node *LogNode) waitProposal(ctx *proposalContext) bool {
	for {
		select {
		case cb := <-ctx.Callbacks():
			cb()
		case <-ctx.Done():
			return ctx.Err() == context.Canceled
		}
	}
}

func (node *LogNode) logFatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
	node.Close()
}

func (node *LogNode) Process(ctx context.Context, m raftpb.Message) error {
	return node.node.Step(ctx, m)
}
func (node *LogNode) IsIDRemoved(id uint64) bool  { return false }
func (node *LogNode) ReportUnreachable(id uint64) { node.node.ReportUnreachable(id) }
func (node *LogNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	node.node.ReportSnapshot(id, status)
}
