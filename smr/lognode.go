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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
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
	"golang.org/x/exp/rand"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	SerializedStateFile    string = "serialized_state.json"
	NewSerializedStateFile string = "serialized_state_new.json"
)

var (
	ProposalDeadline   = 1 * time.Minute
	ErrClosed          = errors.New("node closed")
	ErrEOF             = io.EOF.Error() // For python module to check if io.EOF is returned
	ErrHdfsClientIsNil = errors.New("hdfs client is nil; cannot close it")
	sig                = make(chan os.Signal, 1)
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

func (conf *LogNodeConfig) String() string {
	out, err := json.Marshal(conf)
	if err != nil {
		panic(err)
	}

	return string(out)
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
	peers map[int]string // Raft peer URLs. For now, just used during start. ID of Nth peer is N+1. Each address should be prefixed by "http://"
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

	// This field will be populated by ReadDataDirectoryFromHDFS
	// if there is a serialized state file to be read.
	serialized_state_bytes []byte

	logger        *zap.Logger
	sugaredLogger *zap.SugaredLogger

	debug_port int // Http debug server
}

var defaultSnapshotCount uint64 = 10000

func PrintTestMessage() {
	fmt.Printf("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec auctor quam vel sapien porta, rutrum facilisis ex scelerisque. Vestibulum bibendum luctus ullamcorper. Nunc mattis magna ut sapien ornare posuere. Mauris lorem massa, molestie sodales consequat a, pellentesque a urna. Maecenas consequat nibh vel dolor ultricies, vitae malesuada mauris sollicitudin. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed placerat tellus et enim mattis volutpat. Mauris rhoncus mollis justo vel feugiat. Integer finibus aliquet erat ac porta.\n")
	fmt.Printf("Proin cursus id nibh a semper. Donec eget augue aliquam, efficitur nisl vitae, auctor diam. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed tempus dui vel eros efficitur scelerisque. Pellentesque scelerisque leo nibh, et congue justo viverra ut. Suspendisse id dui vitae lacus tincidunt congue. Nam tempus est elementum consectetur tincidunt.\n")
	fmt.Printf("Ut sit amet justo et risus porta aliquet. Donec id quam ligula. Etiam in purus maximus, aliquet leo sit amet, blandit lacus. Vivamus in euismod ligula. Phasellus pellentesque dapibus faucibus. Curabitur vel tellus a lorem convallis iaculis. Sed molestie gravida felis eu ultrices. Suspendisse consequat sed turpis ac aliquet.\n")
	fmt.Printf("Maecenas facilisis nulla sit amet volutpat luctus. Aenean maximus a diam aliquet bibendum. Nunc ut tellus vel felis congue luctus ut sit amet erat. Cras scelerisque, felis in posuere mollis, purus nunc ultrices odio, sit amet euismod sem lorem vel massa. Sed turpis neque, mattis sit amet scelerisque eu, bibendum hendrerit magna. Phasellus efficitur lacinia euismod. Proin faucibus dignissim elementum. Interdum et malesuada fames ac ante ipsum primis in faucibus. Phasellus dolor eros, finibus sit amet nisl pellentesque, sollicitudin fermentum nisl. Pellentesque sollicitudin leo velit, et tempus tellus tempor quis. Mauris ut diam ut orci imperdiet faucibus.\n")
	fmt.Printf("Aliquam accumsan ut tortor id cursus. Donec tincidunt ullamcorper ligula sed finibus. Maecenas ac turpis a dui placerat eleifend. Aenean suscipit ut turpis sit amet feugiat. Maecenas porta commodo sapien non tempus. Curabitur bibendum fermentum libero vel dapibus. Maecenas vitae tellus in massa aliquet lacinia. Fusce dictum mi tortor, sit amet vestibulum lectus suscipit suscipit. Pellentesque metus nisi, sodales quis semper eu, iaculis at velit.\n")
}

// NewLogNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
//
// The store_path is used as the actual data directory.
// hdfs_data_directory is (possibly) the path to the data directory within HDFS, meaning
// we were migrated and our data directory was written to HDFS so that we could retrieve it.
func NewLogNode(store_path string, id int, hdfsHostname string, hdfs_data_directory string, peerAddresses []string, peerIDs []int, join bool, debug_port int) *LogNode {
	fmt.Fprintf(os.Stderr, "Creating a new LogNode.\n")

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)

	go func() {
		s := <-sig

		fmt.Printf("Received signal: %v\n", s)
		fmt.Fprintf(os.Stderr, "Received signal: %v.\n", s)
	}()

	if len(peerAddresses) != len(peerIDs) {
		fmt.Fprintf(os.Stderr, "[ERROR] Received unequal number of peer addresses (%d) and peer node IDs (%d). They must be equal.\n", len(peerAddresses), len(peerIDs))
		return nil
	}

	fmt.Fprintf(os.Stderr, "Checking validity of hdfs hostname.\n")

	if len(hdfsHostname) == 0 {
		fmt.Fprintf(os.Stderr, "[ERROR] Cannot connect to HDFS; no hostname received.")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Creating LogNode struct now.\n")

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
		snapshotterReady:    make(chan LogSnapshotter, 1),
		data_dir:            store_path,
		hdfs_data_directory: hdfs_data_directory,
		debug_port:          debug_port,
		// rest of structure populated after WAL replay
	}

	fmt.Fprintf(os.Stderr, "Created LogNode struct Creating zap loggers now.\n")

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to create Zap Development logger because: %v\n", err)
		return nil
	}
	node.logger = logger
	node.sugaredLogger = logger.Sugar()

	if node.sugaredLogger == nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to create sugared version of Zap development logger.")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Created LogNode Zap loggers. Parsing store path now (store_path=%s)\n", store_path)

	if store_path != "" {
		node.waldir = path.Join(store_path, fmt.Sprintf("dnlog-%d", id))
		node.snapdir = path.Join(store_path, fmt.Sprintf("dnlog-%d-snap", id))

		node.sugaredLogger.Infof("LogNode %d WAL directory: '%s'", id, node.waldir)
		node.sugaredLogger.Infof("LogNode %d Snapshot directory: '%s'", id, node.snapdir)

		fmt.Printf("LogNode %d WAL directory: '%s'", id, node.waldir)
		fmt.Printf("LogNode %d Snapshot directory: '%s'\n", id, node.snapdir)
	}

	fmt.Fprintf(os.Stderr, "Calling patchPropose now.\n")
	testId, _ := node.patchPropose(nil)
	node.proposalPadding = len(testId)

	fmt.Fprintf(os.Stderr, "Populating list of peers now.\n")
	node.peers = make(map[int]string, len(peerAddresses))
	for i := 0; i < len(peerAddresses); i++ {
		peer_addr := peerAddresses[i]
		peer_id := peerIDs[i]

		node.logger.Info("Discovered peer.", zap.String("peer_address", peer_addr), zap.Int("peer_id", peer_id))
		fmt.Printf("Discovered peer %d: %s.\n", peer_id, peer_addr)
		node.peers[peer_id] = peer_addr
	}

	node.sugaredLogger.Infof("Connecting to HDFS at '%s'", hdfsHostname)
	fmt.Printf("Connecting to HDFS at '%s'\n", hdfsHostname)

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{hdfsHostname},
		User:      "jovyan",
		NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext(ctx, network, address)
			if err != nil {
				node.sugaredLogger.Errorf("Failed to dial HDFS DataNode at address '%s' because: %v", address, err)
				return nil, err
			}
			return conn, nil
		},
		// Temporary work-around to deal with Kubernetes networking issues with HDFS.
		// The HDFS NameNode returns the IP for the client to use to connect to the DataNode for reading/writing file blocks.
		// At least for development/testing, I am using a local Kubernetes cluster and a local HDFS deployment.
		// So, the HDFS NameNode returns the local IP address. But since Kubernetes Pods have their own local host, they cannot use this to connect to the HDFS DataNode.
		DatanodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			port := strings.Split(address, ":")[1]                       // Get the port that the DataNode is using. Discard the IP address.
			modified_address := fmt.Sprintf("%s:%s", "172.17.0.1", port) // Return the IP address that will enable the local k8s Pods to find the local DataNode.
			node.sugaredLogger.Infof("Dialing HDFS DataNode. Original address: '%s'. Modified address: %s.\n", address, modified_address)

			childCtx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext(childCtx, network, modified_address)
			if err != nil {
				node.sugaredLogger.Errorf("Failed to dial HDFS DataNode at address '%s' because: %v", modified_address, err)
				return nil, err
			}

			return conn, nil
		},
	})

	if err != nil {
		node.logger.Error("Failed to create HDFS client.", zap.String("hdfsHostname", hdfsHostname), zap.Error(err))
		// log.Fatalf("Failed to create HDFS client (addr=%s) because: %v\n", hdfsHostname, err)
		return nil
	} else {
		node.sugaredLogger.Infof("Successfully connected to HDFS at '%s'", hdfsHostname)
		fmt.Printf("Successfully connected to HDFS at '%s'\n", hdfsHostname)
		node.hdfsClient = hdfsClient
	}

	// TODO(Ben): Read the data directory from HDFS.
	if hdfs_data_directory != "" {
		node.logger.Info(fmt.Sprintf("Reading data directory from HDFS now: %s.", hdfs_data_directory), zap.String("hdfs_data_directory", hdfs_data_directory), zap.String("data_directory", node.data_dir))

		if hdfs_data_directory != node.data_dir {
			node.logger.Error("The HDFS data directory and the local data directory must be the same; they are not.", zap.String("hdfs_data_directory", hdfs_data_directory), zap.String("data_directory", node.data_dir))
			return nil
		}

		// TODO: Make configurable, or make this interval longer to support larger recoveries.
		// Alternatively, have the Goroutine that's reading the data from HDFS periodically indicate that it is still alive/making progress,
		// and as long as that is happening, we continue waiting, with a timeout such that no progress after <timeout> means the whole operation has failed.
		timeout_interval := time.Duration(60 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), timeout_interval)
		defer cancel()

		progress_chan := make(chan string, 8)
		serialized_state_chan := make(chan []byte)
		error_chan := make(chan error)
		go func(ctx context.Context) {
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)

			// TODO(Ben): Read the 'serialized state' file as well, and return that data back to the Python layer.
			serialized_state_bytes, err := node.ReadDataDirectoryFromHDFS(ctx, progress_chan)
			if err != nil {
				error_chan <- err
				return
			}
			serialized_state_chan <- serialized_state_bytes
		}(ctx)

		tickInterval := time.Duration(10 * time.Second)
		ticker := time.NewTicker(tickInterval)
		noProgress := 0
		done := false
		for !done {
			select {
			case msg := <-progress_chan:
				{
					node.logger.Debug("Made progress.", zap.String("msg", msg))
					noProgress = 0
				}
			case <-ticker.C:
				{
					noProgress += 1
					node.sugaredLogger.Warn("Progress has not been made in roughly %v.", time.Duration(tickInterval*time.Duration(noProgress)))
				}
			case <-ctx.Done():
				{
					err := ctx.Err()
					node.logger.Error("Operation to read data from HDFS timed-out.", zap.Duration("timeout_interval", timeout_interval), zap.Error(err))
					ticker.Stop()
					return nil
				}
			case err := <-error_chan:
				{
					node.logger.Error("Error while reading data directory from HDFS.", zap.Error(err), zap.String("hdfs_data_directory", hdfs_data_directory), zap.String("data_directory", node.data_dir))
					ticker.Stop()
					return nil
				}
			case serialized_state := <-serialized_state_chan:
				{
					node.logger.Info("Successfully read data directory from HDFS to local storage.")
					node.serialized_state_bytes = serialized_state
					done = true
				}
			}
		}

		ticker.Stop()
	} else {
		node.logger.Info("Did not receive a valid HDFS data directory path. Not reading data directory from HDFS.", zap.String("hdfs_data_directory", hdfs_data_directory), zap.String("data_directory", node.data_dir))
	}

	debug.SetPanicOnFault(true)

	fmt.Fprintf(os.Stderr, "Returning LogNode now.\n")

	return node
}

func (node *LogNode) ServeHttpDebug() {
	go func() {
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)

		log.Printf("Serving debug HTTP server on port %d.\n", node.debug_port)

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%d - Hello\n", http.StatusOK)))
		})

		http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%d - Test\n", http.StatusOK)))
		})

		if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", node.debug_port), nil); err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Failed to serve HTTP debug server on port %d because: %v\n", node.debug_port, err)
			log.Fatal("ListenAndServe: ", err)
		}
	}()
}

// Return true if we successfully connected to HDFS.
func (node *LogNode) ConnectedToHDFS() bool {
	return node.hdfsClient != nil
}

func (node *LogNode) NumChanges() int {
	return int(node.num_changes)
}

type startError struct {
	ErrorOccurred bool
	Error         error
}

func (node *LogNode) Start(config *LogNodeConfig) bool {
	node.sugaredLogger.Debugf("LogNode %d is starting with config: %v", node.id, config.String())

	node.config = config
	if !config.Debug {
		node.logger = node.logger.WithOptions(zap.IncreaseLevel(zapcore.InfoLevel))
	}

	startErrorChan := make(chan startError)

	go func() {
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
		node.start(startErrorChan)
	}()

	startError := <-startErrorChan

	if startError.ErrorOccurred {
		node.logger.Error("Failed to start LogNode.", zap.Error(startError.Error))
		return false
	}

	return true
}

func (node *LogNode) StartAndWait(config *LogNodeConfig) {
	node.Start(config)
	node.WaitToClose()
}

// Return the serialized_state_json field.
// This field is populated by ReadDataDirectoryFromHDFS if there is a serialized state file to be read.
// It is only required during migration/error recovery.
func (node *LogNode) GetSerializedState() []byte {
	return node.serialized_state_bytes
}

// Append the difference of the value of specified key to the synchronization queue.
func (node *LogNode) Propose(val Bytes, resolve ResolveCallback, msg string) {
	_, ctx := node.generateProposal(val.Bytes(), ProposalDeadline)
	go func() {
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
		node.propose(ctx, node.sendProposal, resolve, msg)
	}()
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
	debug.SetPanicOnFault(true)
	ctx := node.generateConfChange(&raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(id),
		Context: []byte(addr),
	}, ProposalDeadline)
	go node.propose(ctx, node.manageNode, resolve, "add node")
}

func (node *LogNode) RemoveNode(id int, resolve ResolveCallback) {
	log.Printf("Proposing the removal of node %d\n", id)
	debug.SetPanicOnFault(true)
	ctx := node.generateConfChange(&raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: uint64(id),
	}, ProposalDeadline)
	go node.propose(ctx, node.manageNode, resolve, "remove node")
}

func (node *LogNode) UpdateNode(id int, addr string, resolve ResolveCallback) {
	log.Printf("Proposing an update for node %d to be at address %s\n", id, addr)
	debug.SetPanicOnFault(true)
	ctx := node.generateConfChange(&raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(id),
		Context: []byte(addr),
	}, ProposalDeadline)
	go node.propose(ctx, node.manageNode, resolve, "update node")
}

func (node *LogNode) propose(ctx smrContext, proposer func(smrContext) error, resolve ResolveCallback, msg string) {
	debug.SetPanicOnFault(true)

	if resolve == nil {
		resolve = node.defaultResolveCallback
	}

	node.logger.Info("Proposing to append value", zap.String("key", msg), zap.String("id", ctx.ID()))
	if err := proposer(ctx); err != nil {
		node.logger.Error("Exception while propoising value.", zap.String("key", msg), zap.String("id", ctx.ID()), zap.Error(err))
		resolve(msg, toCError(err))
		return
	}
	node.logger.Info("Proposed value. Waiting for committed or retry.", zap.String("key", msg), zap.String("id", ctx.ID()))
	// Wait for committed or retry
	for !node.waitProposal(ctx) {
		node.logger.Info("Retry proposing to append value", zap.String("key", msg), zap.String("id", ctx.ID()))
		ctx.Reset(ProposalDeadline)
		if err := proposer(ctx); err != nil {
			node.logger.Error("Exception while retrying value proposal.", zap.String("key", msg), zap.String("id", ctx.ID()), zap.Error(err))
			resolve(msg, toCError(err))
			return
		}
	}
	node.logger.Info("Value appended", zap.String("key", msg), zap.String("id", ctx.ID()))
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

// IMPORTANT: This does NOT close the HDFS client.
// This is because, when migrating a raft cluster member, we must first stop the raft
// node before copying the contents of its data directory.
func (node *LogNode) Close() error {
	node.close()

	node.logger.Info("Waiting for Raft node to stop...")

	// Wait for the goroutine to stop.
	lastErr := node.WaitToClose()

	node.logger.Info("Closed LogNode.")

	return lastErr
}

func (node *LogNode) CloseHdfsClient() error {
	if node.hdfsClient != nil {
		err := node.hdfsClient.Close()
		if err != nil {
			node.logger.Error("Error while closing HDFS client.", zap.Error(err))
			return err
		}
	} else {
		node.logger.Warn("HDFS Client is nil. Will skip closing it.")
		return ErrHdfsClientIsNil
	}

	return nil
}

// IMPORTANT: This does NOT close the HDFS client.
// This is because, when migrating a raft cluster member, we must first stop the raft
// node before copying the contents of its data directory.
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

func (node *LogNode) start(startErrorChan chan<- startError) {
	node.sugaredLogger.Debugf("LogNode %d is beginning start procedure now.", node.id)
	debug.SetPanicOnFault(true)

	if node.isSnapEnabled() {
		node.logger.Debug("Snapshots are enabled.")
		if !fileutil.Exist(node.snapdir) {
			if err := os.Mkdir(node.snapdir, 0750); err != nil {
				node.logFatalf("LogNode: cannot create dir for snapshot (%v)", err)
				startErrorChan <- startError{
					ErrorOccurred: true,
					Error:         err,
				}
				return
			}
		}
		node.snapshotter = snap.New(zap.NewExample(), node.snapdir)
	} else {
		node.logger.Warn("Snapshotting is disabled.")
	}

	oldWALExists := false
	node.raftStorage = raft.NewMemoryStorage()
	if node.isWALEnabled() {
		oldWALExists = wal.Exist(node.waldir)
		node.logger.Debug(fmt.Sprintf("WAL is enabled. Old WAL ('%s') available? %v", node.waldir, oldWALExists))
		node.wal = node.replayWAL()
		if node.wal == nil {
			startErrorChan <- startError{
				ErrorOccurred: true,
				Error:         fmt.Errorf("WalReplayError: node.wal is nil after replaying; this should not happen"),
			}
			return
		}
	} else {
		node.logger.Warn("The Raft WAL is disabled.")
	}

	// signal replay has finished
	node.snapshotterReady <- node.snapshotter

	node.logger.Debug("Replaying has completed.")

	rpeers := make([]raft.Peer, 0, len(node.peers))
	for id, peer := range node.peers {
		node.logger.Info(fmt.Sprintf("Adding rpeer %d at addr %s.", id, peer), zap.Int("id", id), zap.String("address", peer))
		rpeers = append(rpeers, raft.Peer{ID: uint64(id)})
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

	if oldWALExists || node.join {
		node.sugaredLogger.Debugf("LogNode %d will be restarting, as the old WAL directory is available (%v), and/or we've been told to join (%v).", node.id, oldWALExists, node.join)
		node.node = raft.RestartNode(c)
	} else {
		node.sugaredLogger.Debugf("LogNode %d will be starting (as if for the first time), as the old WAL directory is not available (%v) and we've not been instructed to join.", node.id, oldWALExists, node.join)
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

	node.logger.Info("Starting Raft HTTP transport now.")
	node.transport.Start()
	for id, peer := range node.peers {
		if id != node.id {
			node.logger.Info(fmt.Sprintf("Adding peer %d at addr %s.", id, peer), zap.Int("id", id), zap.String("address", peer))
			node.transport.AddPeer(types.ID(id), []string{peer})
		}
	}

	go node.serveRaft()
	node.serveChannels(startErrorChan)
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
func (node *LogNode) ReadDataDirectoryFromHDFS(ctx context.Context, progress_chan chan<- string) (serialized_state_bytes []byte, err error) {
	serialized_state_bytes = make([]byte, 0)

	serialized_state_file := filepath.Join(node.data_dir, SerializedStateFile)
	if _, err := node.hdfsClient.Stat(serialized_state_file); err == nil {
		serialized_state_bytes, err = node.hdfsClient.ReadFile(serialized_state_file)
		if err != nil {
			node.logger.Error("Failed to read 'serialized state' from file.", zap.String("path", serialized_state_file), zap.Error(err))
			return serialized_state_bytes, err
		}

		node.logger.Debug("Read serialized state contents from file.", zap.String("path", serialized_state_file))
	} else {
		node.logger.Debug("Did not find a serialized state file. Hopefully you weren't expecting one!")
	}

	node.sugaredLogger.Debugf("Walking the HDFS data directory '%s'", node.hdfs_data_directory)
	walk_err := node.hdfsClient.Walk(node.hdfs_data_directory, func(path string, info fs.FileInfo, err error) error {
		node.sugaredLogger.Debugf("Processing file system object at path \"%s\"", path)
		node.sugaredLogger.Debugf("Base name: \"%s\", Size: %d bytes, Mode: %v, ModTime: %v, IsDir: %v", info.Name(), info.Size(), info.Mode(), info.ModTime(), info.IsDir())

		if info.IsDir() {
			// node.sugaredLogger.Debugf("Found remote directory '%s'", path)
			err := os.MkdirAll(path, os.FileMode(int(0777)))
			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.sugaredLogger.Errorf("Exception encountered while trying to create local directory '%s': %v", path, err)
				return err
			}

			progress_chan <- path
			node.sugaredLogger.Debugf("Successfully created local directory '%s'", path)
		} else {
			// node.sugaredLogger.Debugf("Found remote file '%s'", path)
			err := node.hdfsClient.CopyToLocal(path, path)

			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.sugaredLogger.Errorf("Exception encountered while trying to copy remote-to-local for file '%s': %v", path, err)
				return err
			}

			progress_chan <- path
			node.sugaredLogger.Debugf("Successfully copied remote HDFS file to local file system: '%s'", path)
		}

		return nil
	})

	if walk_err != nil {
		node.sugaredLogger.Errorf("Exception encountered while trying to create HDFS directory '%s'): %v", node.data_dir, walk_err)
		return serialized_state_bytes, walk_err
	}

	return serialized_state_bytes, nil
}

// Write the data directory for this Raft node from local storage to HDFS.
func (node *LogNode) WriteDataDirectoryToHDFS(serialized_state []byte, resolve ResolveCallback) {
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	debug.SetPanicOnFault(true)
	err := node.hdfsClient.MkdirAll(node.data_dir, os.FileMode(int(0777)))
	if err != nil {
		// If we return an error from this function, then WalkDir will stop entirely and return that error.
		node.logger.Error(fmt.Sprintf("Exception encountered while trying to create HDFS directory for node's data directory '%s': %v", node.data_dir, err), zap.String("directory", node.data_dir), zap.Error(err))
		return
	}

	new_serialized_state_file := filepath.Join(node.data_dir, SerializedStateFile)

	var alreadyExists bool = false
	if _, err := node.hdfsClient.Stat(new_serialized_state_file); err == nil {
		// The file already exists. We'll write our state to another file.
		// If that is successful, then we'll delete the old one and replace it with the new one.
		new_serialized_state_file = filepath.Join(node.data_dir, NewSerializedStateFile)
		alreadyExists = true
	}

	writer, err := node.hdfsClient.Create(new_serialized_state_file)
	if err != nil {
		node.logger.Error("Failed to create 'serialized state' file.", zap.String("path", new_serialized_state_file), zap.Error(err))

		// TODO: Handle gracefully, somehow?
		return
	}

	_, err = writer.Write(serialized_state)
	if err != nil {
		node.logger.Error("Error while writing serialized state to file.", zap.String("path", new_serialized_state_file), zap.Error(err))
		writer.Close() // This could also fail.
		return
	}

	st := time.Now()

	for time.Since(st) < (time.Minute * 2) {
		err = writer.Close()

		if err == nil { /* Success */
			break
		}

		// If replication is in progress, then we'll sleep for a few seconds before trying again.
		if hdfs.IsErrReplicating(err) {
			node.logger.Error("Cannot close serialized state file; replication is in progress.", zap.Duration("time-elapsed", time.Since(st)))

			// Sleep for a random interval between 2 and 5 seconds.
			// rand.Intn(4) generates an intenger in the range [0, 4] (i.e., 0, 1, 2, or 3).
			// The minimum is thus 2, and the maximum is 5.
			time.Sleep(time.Second * time.Duration(rand.Intn(4)+2))
			continue
		}

		// Some other error.
		// TODO: Handle this more gracefully?
		node.logger.Error("Failed to close serialized state file.", zap.String("path", new_serialized_state_file), zap.Error(err))
		return
	}

	// If there was already an existing 'serialized state' file in the data directory, then we'll now delete the old one and replace it with the new one.
	if alreadyExists {
		serialized_state_file := filepath.Join(node.data_dir, SerializedStateFile)
		err = node.hdfsClient.Remove(serialized_state_file)

		if err != nil {
			node.logger.Error("Failed to remove existing 'serialized state' file.", zap.String("path", new_serialized_state_file), zap.Error(err))
			// Don't return here. Try the rename operation, in case the file was already deleted for some reason.
		}

		err = node.hdfsClient.Rename(new_serialized_state_file /* serialized_state_new.json */, serialized_state_file /* serialized_state.json */)
		if err != nil {
			node.logger.Error("Failed to rename new 'serialized state' file.", zap.String("old_path", new_serialized_state_file), zap.String("new_path", serialized_state_file))
		}
	}

	node.logger.Debug("Successfully wrote 'serialized state' to file.", zap.String("path", filepath.Join(node.data_dir, SerializedStateFile)))

	// Walk through the entire etcd-raft data directory, copying each file one-at-a-time to HDFS.
	walkdir_err := filepath.WalkDir(node.data_dir, func(path string, d os.DirEntry, err_arg error) error {
		// Note: the first entry found is the base directory passed to filepath.WalkDir (node.data_dir in this case).
		if d.IsDir() {
			node.logger.Info(fmt.Sprintf("Found local directory '%s'", path), zap.String("directory", path))
			err := node.hdfsClient.MkdirAll(path, os.FileMode(int(0777)))
			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				node.logger.Error(fmt.Sprintf("Exception encountered while trying to create HDFS directory '%s': %v", path, err), zap.String("directory", path), zap.Error(err))
				return err
			}

			node.logger.Info(fmt.Sprintf("Successfully created remote (HDFS) directory: '%s'", path), zap.String("directory", path))
		} else {
			node.logger.Info(fmt.Sprintf("Found local file '%s'", path), zap.String("file", path))

			// If the file already exists...
			if _, err := node.hdfsClient.Stat(path); err == nil {
				// ... then we need to remove it and re-write it.
				// TODO (Ben): Can we optimize this so that we only need to add the new data?
				err = node.hdfsClient.Remove(path)
				if err != nil {
					node.logger.Error("Failed to remove existing file during re-write process.", zap.String("path", path), zap.Error(err))
					return err
				}
			}

			var num_tries int = 1
			for {
				err := node.hdfsClient.CopyToRemote(path, path)

				// Based on the documentation within the HDFS package, we can just ignore an ErrReplicating.
				// Specifically, if the datanodes have acknowledged all writes but not yet to the namenode,
				// then this can return ErrReplicating (wrapped in an os.PathError). This indicates
				// that all data has been written, but the lease is still open for the file.
				// It is safe in this case to either ignore the error or perform.
				if err != nil {
					// If it is a replication error, then we'll simply retry with exponential backoff until we close without an error.
					// This is what the Java HDFS client does.
					if hdfs.IsErrReplicating(err) {
						node.sugaredLogger.Warnf("Could not close file \"%s\" on attempt #%d; data is still being replicated. Will retry.", num_tries, path)
						time.Sleep(time.Second * 2 * time.Duration(num_tries))
						num_tries += 1
						continue
					} else {
						// If we return an error from this function, then WalkDir will stop entirely and return that error.
						node.sugaredLogger.Errorf("Exception encountered while trying to copy local-to-remote for file '%s': %v", path, err)
						return err
					}
				}

				break
			}

			node.logger.Info(fmt.Sprintf("Successfully copied local file to HDFS: '%s'", path), zap.String("file", path))
		}
		return nil
	})

	if walkdir_err != nil {
		resolve(fmt.Sprintf("Exception encountered while trying to create HDFS directory '%s'): %v", node.data_dir, walkdir_err), toCError(walkdir_err))
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

		node.logger.Info("Loaded snapshot from WAL directory.", zap.String("waldir", node.waldir), zap.Int("snapshot-size-bytes", snapshot.Size()))
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
		node.logger.Info(fmt.Sprintf("WAL directory \"%s\" does not already exist. Creating it now.", node.waldir), zap.String("directory", node.waldir))
		if err := os.Mkdir(node.waldir, 0750); err != nil {
			node.logFatalf("LogNode: cannot create dir for wal (%v)", err)
			return nil
		}

		w, err := wal.Create(zap.NewExample(), node.waldir, nil)
		if err != nil {
			node.logFatalf("LogNode: create WAL error (%v)", err)
			return nil
		}
		w.Close()
		node.logger.Info(fmt.Sprintf("Successfully created WAL direcotry: \"%s\"", node.waldir), zap.String("uri", node.waldir))
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
	node.logger.Debug("Replaying WAL now.")
	snapshot := node.loadSnapshot()
	// Restore kernel from snapshot
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		if node.config.onRestore == nil {
			node.logFatalf("LogNode %d: no RestoreCallback configured on start.", node.id)
			return nil
		}
		if err := fromCError(node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshot.Data)}, len(snapshot.Data))); err != nil {
			node.logFatalf("LogNode %d: failed to restore states (%v)", node.id, err)
			return nil
		}
	}

	w := node.openWAL(snapshot)
	if w == nil {
		node.sugaredLogger.Warnf("Failed to open WAL snapshot.", zap.String("waldir", node.waldir))
		return w
	}

	node.logger.Debug("Successfully opened WAL via snapshot")

	// Recover the in-memory storage from persistent snapshot, state and entries.
	_, st, ents, err := w.ReadAll()
	if err != nil {
		node.logFatalf("LogNode %d: failed to read WAL (%v)", node.id, err)
		return nil
	}
	if snapshot != nil {
		node.logger.Debug("Applying Snapshot to RaftStorage now.")
		err = node.raftStorage.ApplySnapshot(*snapshot)
		if err != nil {
			node.logFatalf("LogNode %d: error encountered while applying WAL snapshot to Raft storage: %v", node.id, err)
		}
	}
	node.logger.Debug("Recovered Raft HardState from WAL snapshot.", zap.Uint64("Term", st.Term), zap.Uint64("Vote", st.Vote), zap.Uint64("Commit", st.Commit))
	node.sugaredLogger.Debugf("Recovered %d entry/entries from WAL snapshot.", len(ents))
	err = node.raftStorage.SetHardState(st)
	if err != nil {
		node.logFatalf("LogNode %d: error encountered while setting Raft HardState to the HardState recovered from WAL snapshot: %v", node.id, err)
	}

	// Append to storage so raft starts at the right place in log
	err = node.raftStorage.Append(ents)
	if err != nil {
		node.logFatalf("LogNode %d: error encountered while appending entries recovered from WAL snapshot to Raft storage: %v", node.id, err)
	}

	node.logger.Info("Finished replaying WAL.")

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
	node.logger.Warn("Stopping raft node now.")
	node.node.Stop()
	close(node.errorC)
	node.errorC = nil
}

func (node *LogNode) stopHTTP() {
	node.logger.Warn("Stopping HTTP server now.")
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

func (node *LogNode) serveChannels(startErrorChan chan<- startError) {
	node.logger.Info(fmt.Sprintf("LogNode %d is serving channels.", node.id))

	snap, err := node.raftStorage.Snapshot()
	if err != nil {
		startErrorChan <- startError{
			ErrorOccurred: true,
			Error:         err,
		}
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
					node.logger.Info("Clearing local proposal channel.")
					proposeC = nil // Clear local channel.
				} else {
					// blocks until accepted by raft state machine
					node.logger.Info(fmt.Sprintf("LogNode %d: proposing something.", node.id))
					node.node.Propose(ctx, ctx.Proposal)
					node.logger.Info(fmt.Sprintf("LogNode %d: finished proposing something.", node.id))
				}

			case cc, ok := <-confChangeC:
				if !ok || cc == nil {
					node.logger.Info("Clearing local confChange channel.")
					confChangeC = nil // Clear local channel.
				} else {
					confChangeCount++
					cc.ConfChange.ID = confChangeCount
					node.logger.Info(fmt.Sprintf("LogNode %d: proposing configuration change: %s", node.id, cc.ConfChange.String()), zap.String("conf-change", cc.ConfChange.String()))
					node.node.ProposeConfChange(context.TODO(), *cc.ConfChange)
					node.logger.Info(fmt.Sprintf("LogNode %d: finished proposing configuration change.", node.id))
				}
			}
		}

		node.logger.Info("Client closed channel(s). Shutting down Raft (if it isn't already shutdown).")
		// client closed channel; shutdown raft if not already
		close(node.stopc)
	}()
	node.started = true

	// apply commits to state machine
	go func() {
		for {
			select {
			case commit := <-node.commitC:
				// node.logger.Info(fmt.Sprintf("LogNode %d: Applying commit with %d data item(s) to local state machine.", node.id, len(commit.data)))
				for _, d := range commit.data {
					realData, ctx := node.doneProposal(d)
					id := ""
					if ctx != nil {
						id = ctx.ID()
					}
					// node.sugaredLogger.Infof("LogNode %d: Applying data item %d/%d to local state machine.", node.id, idx+1, len(commit.data))
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

	// No error occurred!
	startErrorChan <- startError{
		ErrorOccurred: false,
		Error:         nil,
	}

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			node.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-node.node.Ready():
			// if len(rd.CommittedEntries) > 0 || rd.HardState.Commit > 0 {
			// 	node.logger.Info("ready", zap.Int("num-committed-entries", len(rd.CommittedEntries)), zap.Uint64("hardstate.commit", rd.HardState.Commit))
			// }
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
			node.logger.Warn("Writing error", zap.Error(err))

			// Write errors and close.
			node.writeError(err)
			node.close()

		case <-node.stopc:
			node.sugaredLogger.Warnf("LogNode %d is stopping now.", node.id)

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

	node.sugaredLogger.Infof("LogNode %d is beginning to serve Raft.", node.id)

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
	node.sugaredLogger.Warnf(format, args...)
}

func (node *LogNode) logFatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
	node.sugaredLogger.Errorf(format, args...)
	node.writeError(fmt.Errorf(format, args...))
	node.close()
}

func (node *LogNode) panic(err error) {
	log.Println(err)
	node.logger.Error("Fatal error occurred.", zap.Error(err))
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
