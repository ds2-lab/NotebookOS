// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
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
	"go.uber.org/zap/zapcore"
)

var (
	ProposalDeadline = 1 * time.Minute
	ErrClosed        = errors.New("node closed")
	ErrEOF           = io.EOF.Error() // For python module to check if io.EOF is returned
)

type StateValueCallback func(ReadCloser, int, string) string
type StatesValueCallback func(ReadCloser, int) string
type ShouldLogNodeCallback func(*LogNode) bool
type WriteCallback func(WriteCloser) string
type ResolveCallback func(interface{}, string)

func fromCError(cbRet string) error {
	if len(cbRet) == 0 {
		return nil
	} else {
		return errors.New(cbRet)
	}
}

func toCError(err error) string {
	if err == nil {
		return ""
	} else {
		return err.Error()
	}
}

type LogNodeConfig struct {
	ElectionTick  int
	HeartbeatTick int
	Debug         bool

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
	proposeC    chan *proposalContext   // proposed serialized messages
	confChangeC chan *confChangeContext // proposed cluster config changes
	commitC     chan *commit
	errorC      chan error // errors from raft session

	hdfsClient *hdfs.Client // HDFS client for reading/writing the data directory during migrations.

	id    int            // Client ID for raft session
	peers map[int]string // Raft peer URLs. For now, just used during start. ID of Nth peer is N+1.
	join  bool           // Node is joining an existing cluster

	waldir              string // Path to WAL directory
	snapdir             string // Path to snapshot directory
	data_dir            string // The raft data directory.
	hdfs_data_directory string // The location of the backed-up data directory within HDFS. If it is the empty string, then it is invalid.

	confState        raftpb.ConfState
	snapshotIndex    uint64
	appliedIndex     uint64
	num_changes      uint64
	denied_changes   uint64
	proposalPadding  int
	proposalRegistry hashmap.BaseHashMap[string, smrContext]

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      LogSnapshotter
	snapshotterReady chan LogSnapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	started   bool
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
//
// The store_path is used as the actual data directory.
// hdfs_data_directory is (possibly) the path to the data directory within HDFS, meaning
// we were migrated and our data directory was written to HDFS so that we could retrieve it.
func NewLogNode(store_path string, id int, hdfsHostname string, hdfs_data_directory string, peerAddresses []string, peerIDs []int, join bool) *LogNode {
	if len(peerAddresses) != len(peerIDs) {
		log.Fatalf("Received unequal number of peer addresses (%d) and peer node IDs (%d). They must be equal.\n", len(peerAddresses), len(peerIDs))
	}

	fmt.Printf("Creating a new LogNode.\n")

	node := &LogNode{
		proposeC:            make(chan *proposalContext),
		confChangeC:         make(chan *confChangeContext),
		commitC:             make(chan *commit),
		errorC:              make(chan error, 1),
		id:                  id,
		join:                join,
		snapCount:           defaultSnapshotCount,
		stopc:               make(chan struct{}),
		httpstopc:           make(chan struct{}),
		httpdonec:           make(chan struct{}),
		num_changes:         0,
		denied_changes:      0,
		proposalRegistry:    hashmap.NewConcurrentMap[smrContext](32),
		logger:              zap.NewExample(), // zap.NewExample(zap.IncreaseLevel(zapcore.DebugLevel)),
		snapshotterReady:    make(chan LogSnapshotter, 1),
		data_dir:            store_path,
		hdfs_data_directory: hdfs_data_directory,
		// rest of structure populated after WAL replay
	}
	if store_path != "" {
		node.waldir = path.Join(store_path, fmt.Sprintf("dnlog-%d", id))
		node.snapdir = path.Join(store_path, fmt.Sprintf("dnlog-%d-snap", id))

		node.logger.Info(fmt.Sprintf("LogNode %d WAL directory: \"%s\"", id, node.waldir))
		node.logger.Info(fmt.Sprintf("LogNode %d Snapshot directory: \"%s\"", id, node.snapdir))
	}
	testId, _ := node.patchPropose(nil)
	node.proposalPadding = len(testId)

	node.peers = make(map[int]string, len(peerAddresses))
	for i := 0; i < len(peerAddresses); i++ {
		peer_addr := peerAddresses[i]
		peer_id := peerIDs[i]

		node.logger.Info("Discovered peer.", zap.String("peer_address", peer_addr), zap.Int("peer_id", peer_id))
		node.peers[peer_id] = peer_addr
	}

	node.logger.Info(fmt.Sprintf("Connecting to HDFS at \"%s\"", hdfsHostname), zap.String("hostname", hdfsHostname))

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{hdfsHostname},
		User:      "jovyan",
		// Temporary work-around to deal with Kubernetes networking issues with HDFS.
		// The HDFS NameNode returns the IP for the client to use to connect to the DataNode for reading/writing file blocks.
		// At least for development/testing, I am using a local Kubernetes cluster and a local HDFS deployment.
		// So, the HDFS NameNode returns the local IP address. But since Kubernetes Pods have their own local host, they cannot use this to connect to the HDFS DataNode.
		DatanodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			port := strings.Split(address, ":")[1]                       // Get the port that the DataNode is using. Discard the IP address.
			modified_address := fmt.Sprintf("%s:%s", "172.17.0.1", port) // Return the IP address that will enable the local k8s Pods to find the local DataNode.
			node.logger.Info(fmt.Sprintf("Dialing HDFS DataNode. Original address \"%s\". Modified address: %s.\n", address, modified_address), zap.String("original_address", address), zap.String("modified_address", modified_address))
			conn, err := (&net.Dialer{}).DialContext(ctx, network, modified_address)
			if err != nil {
				return nil, err
			}

			return conn, nil
		},
	})
	if err != nil {
		node.logger.Error("Failed to create HDFS client.", zap.String("hdfsHostname", hdfsHostname))
		node.logger.Panic("Failed to create HDFS client.", zap.String("hdfsHostname", hdfsHostname))
	}

	node.logger.Info(fmt.Sprintf("Successfully connected to HDFS at \"%s\"", hdfsHostname), zap.String("hostname", hdfsHostname))
	node.hdfsClient = hdfsClient

	// TODO(Ben): Read the data directory from HDFS.
	if hdfs_data_directory != "" {
		err := node.ReadDataDirectoryFromHDFS()

		if err != nil {
			return nil
		}

		node.logger.Info("Successfully read data directory from HDFS to local storage.")
	}

	return node
}

func (node *LogNode) NumChanges() int {
	return int(node.num_changes)
}

func (node *LogNode) Start(config *LogNodeConfig) {
	node.config = config
	if !config.Debug {
		node.logger = node.logger.WithOptions(zap.IncreaseLevel(zapcore.InfoLevel))
	}
	go node.start()
}

func (node *LogNode) StartAndWait(config *LogNodeConfig) {
	node.Start(config)
	node.WaitToClose()
}

// Append the difference of the value of specified key to the synchronization queue.
func (node *LogNode) Propose(val Bytes, resolve ResolveCallback, msg string) {
	_, ctx := node.generateProposal(val.Bytes(), ProposalDeadline)
	go node.propose(ctx, node.sendProposal, resolve, msg)
}

// func (node *LogNode) propose(val []byte, resolve ResolveCallback, msg string) {
// 	_, ctx := node.generateProposal(val, ProposalDeadline)
// 	node.logger.Info("Appending value", zap.String("key", msg), zap.String("id", ctx.ID()))
// 	if err := node.sendProposal(ctx); err != nil {
// 		resolve(msg, toCError(err))
// 		return
// 	}
// 	// Wait for committed or retry
// 	for !node.waitProposal(ctx) {
// 		node.logger.Info("Retry appending value", zap.String("key", msg), zap.String("id", ctx.ID()))
// 		ctx.Reset(ProposalDeadline)
// 		if err := node.sendProposal(ctx); err != nil {
// 			resolve(msg, toCError(err))
// 			return
// 		}
// 	}
// 	node.logger.Info("Value appended", zap.String("key", msg), zap.String("id", ctx.ID()))
// 	if resolve != nil {
// 		resolve(msg, toCError(nil))
// 	}
// }

func (node *LogNode) AddNode(id int, addr string, resolve ResolveCallback) {
	log.Printf("Proposing the addition of node %d at %s\n", id, addr)
	ctx := node.generateConfChange(&raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(id),
		Context: []byte(addr),
	}, ProposalDeadline)
	go node.propose(ctx, node.manageNode, resolve, "add node")
}

func (node *LogNode) RemoveNode(id int, resolve ResolveCallback) {
	log.Printf("Proposing the removal of node %d\n", id)
	ctx := node.generateConfChange(&raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: uint64(id),
	}, ProposalDeadline)
	go node.propose(ctx, node.manageNode, resolve, "remove node")
}

func (node *LogNode) UpdateNode(id int, addr string, resolve ResolveCallback) {
	log.Printf("Proposing an update for node %d to be at address %s\n", id, addr)
	ctx := node.generateConfChange(&raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(id),
		Context: []byte(addr),
	}, ProposalDeadline)
	go node.propose(ctx, node.manageNode, resolve, "update node")
}

func (node *LogNode) propose(ctx smrContext, proposer func(smrContext) error, resolve ResolveCallback, msg string) {
	if resolve == nil {
		resolve = node.defaultResolveCallback
	}

	node.logger.Info("Appending value", zap.String("key", msg), zap.String("id", ctx.ID()))
	if err := proposer(ctx); err != nil {
		resolve(msg, toCError(err))
		return
	}
	// Wait for committed or retry
	for !node.waitProposal(ctx) {
		node.logger.Info("Retry appending value", zap.String("key", msg), zap.String("id", ctx.ID()))
		ctx.Reset(ProposalDeadline)
		if err := proposer(ctx); err != nil {
			resolve(msg, toCError(err))
			return
		}
	}
	node.logger.Info("Value appended", zap.String("key", msg), zap.String("id", ctx.ID()))
	if msg == "add node" {
		log.Printf("Value appended: \"add node\" (id: %s)\n", ctx.ID())
	} else if msg == "remove node" {
		log.Printf("Value appended: \"remove node\" (id: %s)\n", ctx.ID())
	}
	if resolve != nil {
		resolve(msg, toCError(nil))
	}
}

func (node *LogNode) manageNode(ctx smrContext) error {
	select {
	case node.confChangeC <- ctx.(*confChangeContext):
		return nil
	case <-node.stopc:
		return ErrClosed
	}
}

func (node *LogNode) WaitToClose() (lastErr error) {
	// Save local variables to avoid node.errorC being set to Nil.
	errorC := node.errorC
	for errorC != nil {
		err, ok := <-errorC
		if !ok || err == nil {
			return
		} else {
			lastErr = err
		}
	}
	return
}

func (node *LogNode) Close() error {
	node.close()

	// Wait for the goroutine to stop.
	return node.WaitToClose()
}

func (node *LogNode) close() {
	node.logger.Info("Closing nodes...")
	// Clear node channels
	proposeC, confChangeC := node.proposeC, node.confChangeC
	node.proposeC, node.confChangeC = nil, nil

	if !node.started {
		close(node.stopc)
	} else {
		// Signal the routine that depends on input channels to stop.
		// Will trigger the close(stopc).
		if proposeC != nil {
			select {
			case proposeC <- nil:
				// Handler routing is still active
				if confChangeC != nil {
					confChangeC <- nil
					confChangeC = nil
				}
			default:
			}
		}
		if confChangeC != nil {
			select {
			case confChangeC <- nil:
			default:
			}
		}
	}
}

func (node *LogNode) start() {
	if node.isSnapEnabled() {
		if !fileutil.Exist(node.snapdir) {
			if err := os.Mkdir(node.snapdir, 0750); err != nil {
				node.logFatalf("LogNode: cannot create dir for snapshot (%v)", err)
				return
			}
		}
		node.snapshotter = snap.New(zap.NewExample(), node.snapdir)
	}

	oldwal := false
	node.raftStorage = raft.NewMemoryStorage()
	if node.isWALEnabled() {
		oldwal = wal.Exist(node.waldir)
		node.logger.Info(fmt.Sprintf("WAL is enabled. Old WAL available: %v.", node.waldir))
		node.wal = node.replayWAL()
		if node.wal == nil {
			return
		}
	}

	// signal replay has finished
	node.snapshotterReady <- node.snapshotter

	rpeers := make([]raft.Peer, 0, len(node.peers))
	for id, peer := range node.peers {
		if peer != "" {
			rpeers = append(rpeers, raft.Peer{ID: uint64(id)})
		}
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
	for id, peer := range node.peers {
		// Allow holes in the peer list
		if peer != "" && id != node.id {
			node.transport.AddPeer(types.ID(id), []string{peer})
		}
	}

	go node.serveRaft()
	node.serveChannels()
}

func (node *LogNode) isSnapEnabled() bool {
	return node.snapdir != ""
}

func (node *LogNode) saveSnap(snap raftpb.Snapshot) error {
	if node.snapshotter == nil || node.wal == nil {
		return nil
	}

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
		return nil
	}
	if node.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[node.appliedIndex-firstIdx+1:]
	}
	return nents
}

// Read the data directory for this Raft node back from HDFS to local storage.
//
// This assumes the HDFS path and the local path are identical.
func (node *LogNode) ReadDataDirectoryFromHDFS() error {
	node.logger.Info(fmt.Sprintf("Walking the HDFS data directory \"%s\"", node.hdfs_data_directory), zap.String("directory", node.hdfs_data_directory))
	walk_err := node.hdfsClient.Walk(node.hdfs_data_directory, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			node.logger.Info(fmt.Sprintf("Found remote directory \"%s\"", path), zap.String("directory", path))
			err := os.MkdirAll(path, os.FileMode(int(0777)))
			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.logger.Error(fmt.Sprintf("Exception encountered while trying to create local directory \"%s\": %v", path, err), zap.String("directory", path), zap.Error(err))
				return err
			}

			node.logger.Info(fmt.Sprintf("Successfully created local directory \"%s\"", path), zap.String("directory", path))
			// Convert the remote HDFS path to a local path based on the persistent store ID.
		} else {
			node.logger.Info(fmt.Sprintf("Found remote file \"%s\"", path), zap.String("file", path))
			err := node.hdfsClient.CopyToLocal(path, path)

			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.logger.Error(fmt.Sprintf("Exception encountered while trying to copy remote-to-local for file \"%s\": %v", path, err), zap.String("file", path), zap.Error(err))
				return err
			}

			node.logger.Info(fmt.Sprintf("Successfully copied remote HDFS file to local file system: \"%s\"", path), zap.String("file", path))
		}

		return nil
	})

	if walk_err != nil {
		node.logger.Error(fmt.Sprintf("Exception encountered while trying to create HDFS directory \"%s\"): %v", node.data_dir, walk_err), zap.Error(walk_err))
		return walk_err
	}

	return nil
}

// Write the data directory for this Raft node from local storage to HDFS.
func (node *LogNode) WriteDataDirectoryToHDFS(resolve ResolveCallback) {
	// Walk through the entire etcd-raft data directory, copying each file one-at-a-time to HDFS.
	walkdir_err := filepath.WalkDir(node.data_dir, func(path string, d os.DirEntry, err_arg error) error {
		// Note: the first entry found is the base directory passed to filepath.WalkDir (node.data_dir in this case).
		if d.IsDir() {
			node.logger.Info(fmt.Sprintf("Found local directory \"%s\"", path), zap.String("directory", path))
			err := node.hdfsClient.MkdirAll(path, os.FileMode(int(0777)))
			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.logger.Error(fmt.Sprintf("Exception encountered while trying to create HDFS directory \"%s\": %v", path, err), zap.String("directory", path), zap.Error(err))
				return err
			}

			node.logger.Info(fmt.Sprintf("Successfully created HDFS directory \"%s\"", path), zap.String("directory", path))
		} else {
			node.logger.Info(fmt.Sprintf("Found local file \"%s\"", path), zap.String("file", path))
			err := node.hdfsClient.CopyToRemote(path, path)

			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.logger.Error(fmt.Sprintf("Exception encountered while trying to copy local-to-remote for file \"%s\": %v", path, err), zap.String("file", path), zap.Error(err))
				return err
			}

			node.logger.Info(fmt.Sprintf("Successfully copied local file to HDFS: \"%s\"", path), zap.String("file", path))
		}
		return nil
	})

	if walkdir_err != nil {
		resolve(fmt.Sprintf("Exception encountered while trying to create HDFS directory \"%s\"): %v", node.data_dir, walkdir_err), toCError(walkdir_err))
		return
	}

	if resolve != nil {
		resolve(node.data_dir, toCError(nil))
	}
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

			node.logger.Info(fmt.Sprintf("incoming conf change: %v", &cc), zap.Int32("type", int32(cc.Type)), zap.Int("context length", len(cc.Context)))

			// Call immidiately to restore valid cc.Context
			ctx := node.doneConfChange(&cc)

			node.confState = *node.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					node.logger.Info(fmt.Sprintf("Adding a node to the cluster: node %d", cc.NodeID), zap.Uint64("node_id", cc.NodeID))
					log.Println("Adding a node to the cluster: ", cc.NodeID)
					node.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(node.id) {
					node.logger.Info("I've been removed from the cluster! Shutting down.")
					log.Println("I've been removed from the cluster! Shutting down.")
					if ctx != nil {
						ctx.Cancel()
					}
					return nil, false
				}
				node.transport.RemovePeer(types.ID(cc.NodeID))
			case raftpb.ConfChangeUpdateNode:
				if len(cc.Context) > 0 {
					node.logger.Info(fmt.Sprintf("Updating an existing node within cluster: node %d", cc.NodeID), zap.Uint64("node_id", cc.NodeID))
					log.Println("Updating an existing node within cluster: ", cc.NodeID)
					node.transport.UpdatePeer(types.ID(cc.NodeID), []string{string(cc.Context)})
					node.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			}

			if ctx != nil {
				ctx.Cancel()
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
	if node.snapshotter != nil && node.isWALEnabled() && wal.Exist(node.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(node.logger, node.waldir)
		if err != nil {
			node.logWarnf("LogNode: error listing snapshots (%v)", err)
			return nil
		}
		snapshot, err := node.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			node.logWarnf("LogNode: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return nil
}

func (node *LogNode) isWALEnabled() bool {
	return node.waldir != ""
}

// openWAL returns a WAL ready for reading.
func (node *LogNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(node.waldir) {
		if err := os.Mkdir(node.waldir, 0750); err != nil {
			node.logFatalf("LogNode: cannot create dir for wal (%v)", err)
			return nil
		}

		w, err := wal.Create(zap.NewExample(), node.waldir, nil)
		if err != nil {
			node.logFatalf("LogNode: create wal error (%v)", err)
			return nil
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
			return nil
		}
		if err := fromCError(node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshot.Data)}, len(snapshot.Data))); err != nil {
			node.logFatalf("LogNode: failed to restore states (%v)", err)
			return nil
		}
	}

	w := node.openWAL(snapshot)
	if w == nil {
		return w
	}
	_, st, ents, err := w.ReadAll()
	if err != nil {
		node.logFatalf("LogNode: failed to read WAL (%v)", err)
		return nil
	}
	if snapshot != nil {
		node.raftStorage.ApplySnapshot(*snapshot)
	}
	node.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	node.raftStorage.Append(ents)

	return w
}

func (node *LogNode) writeError(err error) {
	select {
	case node.errorC <- err: // node.errorC will be set to nil immediately after closed.
		return
	default:
	}

	// Block or closed?
	select {
	case _, ok := <-node.errorC:
		if !ok {
			return // errorC closed
		}
	default:
		return // errorC closed and set nil
	}

	// Retry or abandon
	select {
	case node.errorC <- err:
	default:
	}
}

// stop closes http, closes all channels, and stops raft.
func (node *LogNode) stopServing() {
	node.stopHTTP()
	node.node.Stop()
	close(node.errorC)
	node.errorC = nil
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
		return
	}

	// Restore kernal from snapshot
	if node.config.onRestore == nil {
		node.logFatalf("no RestoreCallback configured on start.")
		return
	}
	node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshotToSave.Data)}, len(snapshotToSave.Data))

	node.confState = snapshotToSave.Metadata.ConfState
	node.snapshotIndex = snapshotToSave.Metadata.Index
	node.appliedIndex = snapshotToSave.Metadata.Index
	node.num_changes = 0
}

var snapshotCatchUpEntriesN uint64 = 1000

func (node *LogNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if !node.isSnapEnabled() ||
		node.config.getSnapshot == nil || // No snapshotCallback
		(node.config.shouldSnapshot == nil && node.appliedIndex-node.snapshotIndex <= node.snapCount) { // No shouldSnapshot and failed default test.
		return
	} else if node.config.shouldSnapshot != nil && (node.num_changes == node.denied_changes || !node.config.shouldSnapshot(node)) { // Failed shouldSnapshot test
		// Update denied_changes to avoid triggering snapshot too frequently.
		node.denied_changes = node.num_changes
		return
	}

	node.logger.Info("should snapshot passed")

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
		if err := fromCError(node.config.getSnapshot(&writerWrapper{writer: writer})); err != nil {
			writer.CloseWithError(err)
		}
	}()

	node.logger.Info("start snapshot", zap.Uint64("applied index", node.appliedIndex), zap.Uint64("last index", node.snapshotIndex))
	data, err := io.ReadAll(reader)
	if err != nil {
		node.panic(err)
		return
	}
	snap, err := node.raftStorage.CreateSnapshot(node.appliedIndex, &node.confState, data)
	if err != nil {
		node.panic(err)
		return
	}
	if err := node.saveSnap(snap); err != nil {
		node.panic(err)
		return
	}

	compactIndex := node.appliedIndex
	if node.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = node.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := node.raftStorage.Compact(compactIndex); err != nil {
		node.panic(err)
		return
	}

	node.logger.Info("compacted log", zap.Uint64("index", compactIndex))
	node.snapshotIndex = node.appliedIndex
	node.num_changes = 0
}

func (node *LogNode) serveChannels() {
	snap, err := node.raftStorage.Snapshot()
	if err != nil {
		node.panic(err)
		return
	}
	node.confState = snap.Metadata.ConfState
	node.snapshotIndex = snap.Metadata.Index
	node.appliedIndex = snap.Metadata.Index

	if node.wal != nil {
		defer node.wal.Close()
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		// Save local channel variables to avoid closing node channels.
		proposeC := node.proposeC
		confChangeC := node.confChangeC
		for proposeC != nil || confChangeC != nil {
			select {
			case ctx, ok := <-proposeC:
				if !ok || ctx == nil {
					proposeC = nil // Clear local channel.
				} else {
					// blocks until accepted by raft state machine
					node.logger.Info("Proposing something.")
					node.node.Propose(ctx, ctx.Proposal)
				}

			case cc, ok := <-confChangeC:
				if !ok || cc == nil {
					confChangeC = nil // Clear local channel.
				} else {
					confChangeCount++
					cc.ConfChange.ID = confChangeCount
					node.logger.Info("Proposing configuration change: %s", zap.String("conf-change", cc.ConfChange.String()))
					node.node.ProposeConfChange(context.TODO(), *cc.ConfChange)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(node.stopc)
	}()
	node.started = true

	// apply commits to state machine
	go func() {
		for {
			select {
			case commit := <-node.commitC:
				for _, d := range commit.data {
					realData, ctx := node.doneProposal(d)
					id := ""
					if ctx != nil {
						id = ctx.ID()
					}
					if err := fromCError(node.config.onChange(&readerWrapper{reader: bytes.NewBuffer(realData)}, len(realData), id)); err != nil {
						node.logFatalf("LogNode: Error on replay state (%v)", err)
					}
					if ctx != nil {
						ctx.Cancel()
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
			if node.wal != nil {
				node.wal.Save(rd.HardState, rd.Entries)
			}
			if node.snapshotter != nil && !raft.IsEmptySnap(rd.Snapshot) {
				node.saveSnap(rd.Snapshot)
				node.raftStorage.ApplySnapshot(rd.Snapshot)
				node.publishSnapshot(rd.Snapshot)
			}
			node.raftStorage.Append(rd.Entries)
			node.transport.Send(node.processMessages(rd.Messages))
			applyDoneC, ok := node.publishEntries(node.entriesToApply(rd.CommittedEntries))
			if !ok {
				node.close()
				break
			}
			node.maybeTriggerSnapshot(applyDoneC)
			node.node.Advance()

		case err := <-node.transport.ErrorC:
			// Write errors and close.
			node.writeError(err)
			node.close()

		case <-node.stopc:
			node.stopServing()
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
	defer close(node.httpdonec)

	url, err := url.Parse(node.peers[node.id])
	if err != nil {
		node.logFatalf("LogNode: Failed parsing URL (%v)", err)
		return
	}

	ln, err := newStoppableListener(url.Host, node.httpstopc)
	if err != nil {
		node.logFatalf("LogNode: Failed to listen rafthttp (%v)", err)
		return
	}

	err = (&http.Server{Handler: node.transport.Handler()}).Serve(ln)
	select {
	case <-node.httpstopc:
	default:
		node.logFatalf("LogNode: Failed to serve rafthttp (%v)", err)
	}
}

func (node *LogNode) patchPropose(val []byte) (ret []byte, id string) {
	uuid := uuid.New()
	binary, _ := uuid.MarshalBinary()
	id = uuid.String()
	if len(val) == 0 {
		ret = binary
	} else {
		ret = append(val, binary...)
	}
	return
}

func (node *LogNode) generateProposal(val []byte, timeout time.Duration) ([]byte, *proposalContext) {
	ret, id := node.patchPropose(val)
	ctx := NewProposalContext(id, ret, timeout)
	node.registerProposal(ctx)
	return ret, ctx
}

func (node *LogNode) generateConfChange(cc *raftpb.ConfChange, timeout time.Duration) *confChangeContext {
	var id string
	cc.Context, id = node.patchPropose(cc.Context)
	ctx := NewConfChangeContext(id, cc, timeout)
	node.registerProposal(ctx)
	return ctx
}

func (node *LogNode) registerProposal(proposal smrContext) {
	node.proposalRegistry.Store(proposal.ID(), proposal)
}

func (node *LogNode) sendProposal(proposal smrContext) error {
	// Save local channel for thread-safe access.
	select {
	case node.proposeC <- proposal.(*proposalContext):
		return nil
	case <-node.stopc:
		return ErrClosed
	}
}

func (node *LogNode) doneProposal(val []byte) ([]byte, smrContext) {
	if len(val) < node.proposalPadding {
		return val, nil
	}

	binary := val[len(val)-node.proposalPadding:]
	var id uuid.UUID
	id.UnmarshalBinary(binary)

	ret := val[:len(val)-node.proposalPadding]
	ctx, _ := node.proposalRegistry.LoadAndDelete(id.String())
	return ret, ctx
}

func (node *LogNode) doneConfChange(cc *raftpb.ConfChange) (ctx smrContext) {
	cc.Context, ctx = node.doneProposal(cc.Context)
	return ctx
}

func (node *LogNode) waitProposal(ctx smrContext) bool {
	<-ctx.Done()
	return ctx.Err() == context.Canceled
}

func (node *LogNode) logWarnf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (node *LogNode) logFatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
	node.writeError(fmt.Errorf(format, args...))
	node.close()
}

func (node *LogNode) panic(err error) {
	log.Println(err)
	node.writeError(err)
	node.close()
}

func (node *LogNode) defaultResolveCallback(interface{}, string) {
	// Ignore
}

func (node *LogNode) Process(ctx context.Context, m raftpb.Message) error {
	return node.node.Step(ctx, m)
}
func (node *LogNode) IsIDRemoved(id uint64) bool  { return false }
func (node *LogNode) ReportUnreachable(id uint64) { node.node.ReportUnreachable(id) }
func (node *LogNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	node.node.ReportSnapshot(id, status)
}
