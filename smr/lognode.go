// Package smr
//
// # Copyright 2015 The etcd Authors
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
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/scusemua/distributed-notebook/smr/storage"

	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
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

	_ "github.com/ianlancetaylor/cgosymbolizer"
)

const (
	SerializedStateDirectory       string = "serialized_raft_log_states"
	SerializedStateBaseFileName    string = "serialized_state.json"
	SerializedStateFileExtension   string = ".json"
	NewSerializedStateBaseFileName string = "serialized_state_new"
	DoneString                     string = "DONE"
	VersionText                    string = "1.0.1"

	hdfsRemoteStorage  string = "hdfs"
	redisRemoteStorage string = "redis"
	localStorage       string = "local"
)

var (
	ProposalDeadline          = 1 * time.Minute
	ErrClosed                 = errors.New("node closed")
	ErrEOF                    = io.EOF.Error() // For python module to check if io.EOF is returned
	ErrRemoteStorageClientNil = errors.New("remote remote_storage client is nil; cannot close it")
	sig                       = make(chan os.Signal, 1)
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

func finalize() {
	if err := recover(); err != nil {
		fmt.Printf("Panic/Error: %v\n", err)
		fmt.Printf("Stacktrace:\n")
		debug.PrintStack()

		time.Sleep(time.Second * 30)

		panic(err)
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

// WithRestoreCallback
//
// Note: the restore callback should not be run on a Python IO loop.
// LogNode::Start is called (from Python code --> Go code) on an asyncio IO loop.
// While going from Python --> Go releases the GIL, the IO loop will still essentially be blocked.
// So, while we can call back into Python code from Go (from the LogNode::Start method),
// we must do so "directly", and not by scheduling something to run on an IO loop.
// (If we schedule something to run on the IO loop, then it will not be executed until we return from LogNode::Start).
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

// LogNode is a SyncLog backed by raft
type LogNode struct {
	proposeC    chan *proposalContext   // proposed serialized messages
	confChangeC chan *confChangeContext // proposed cluster config changes
	commitC     chan *commit
	errorC      chan error // errors from raft session

	deploymentMode string

	storageProvider storage.Provider

	remoteStorageReadTime time.Duration // remoteStorageReadTime is the amount of time spent reading data from remote remote_storage.

	id    int            // Client ID for raft session
	peers map[int]string // Raft peer URLs. For now, just used during start. ID of Nth peer is N+1. Each address should be prefixed by "http://"
	join  bool           // Node is joining an existing cluster

	waldir                          string // Path to WAL directory
	snapdir                         string // Path to snapshot directory
	dataDir                         string // The raft data directory. Note: the base path of store_path is the persistent ID.
	shouldLoadDataFromRemoteStorage bool

	confState        raftpb.ConfState
	snapshotIndex    uint64
	appliedIndex     uint64
	numChanges       uint64
	deniedChanges    uint64
	proposalPadding  int
	proposalRegistry hashmap.BaseHashMap[string, smrContext]

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      LogSnapshotter
	snapshotterReady chan LogSnapshotter // signals when snapshotter is ready

	snapCount       uint64
	transport       *rafthttp.Transport
	started         bool
	stopChannel     chan struct{} // signals proposal channel closed
	httpStopChannel chan struct{} // signals http server to shut down
	httpDoneChannel chan struct{} // signals http server shutdown complete

	// Bridges
	config *LogNodeConfig

	// This field will be populated by ReadDataDirectoryFromRemoteStorage method
	// if there is a serialized state file to be read.
	serializedStateBytes []byte

	logger        *zap.Logger
	sugaredLogger *zap.SugaredLogger

	httpDebugPort int // Http debug server

	atom zap.AtomicLevel
}

var defaultSnapshotCount uint64 = 10000

func PrintTestMessage() {
	fmt.Printf("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec auctor quam vel sapien porta, rutrum facilisis ex scelerisque. Vestibulum bibendum luctus ullamcorper. Nunc mattis magna ut sapien ornare posuere. Mauris lorem massa, molestie sodales consequat a, pellentesque a urna. Maecenas consequat nibh vel dolor ultricies, vitae malesuada mauris sollicitudin. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed placerat tellus et enim mattis volutpat. Mauris rhoncus mollis justo vel feugiat. Integer finibus aliquet erat ac porta.\n")
	fmt.Printf("Proin cursus id nibh a semper. Donec eget augue aliquam, efficitur nisl vitae, auctor diam. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed tempus dui vel eros efficitur scelerisque. Pellentesque scelerisque leo nibh, et congue justo viverra ut. Suspendisse id dui vitae lacus tincidunt congue. Nam tempus est elementum consectetur tincidunt.\n")
	fmt.Printf("Ut sit amet justo et risus porta aliquet. Donec id quam ligula. Etiam in purus maximus, aliquet leo sit amet, blandit lacus. Vivamus in euismod ligula. Phasellus pellentesque dapibus faucibus. Curabitur vel tellus a lorem convallis iaculis. Sed molestie gravida felis eu ultrices. Suspendisse consequat sed turpis ac aliquet.\n")
	fmt.Printf("Maecenas facilisis nulla sit amet volutpat luctus. Aenean maximus a diam aliquet bibendum. Nunc ut tellus vel felis congue luctus ut sit amet erat. Cras scelerisque, felis in posuere mollis, purus nunc ultrices odio, sit amet euismod sem lorem vel massa. Sed turpis neque, mattis sit amet scelerisque eu, bibendum hendrerit magna. Phasellus efficitur lacinia euismod. Proin faucibus dignissim elementum. Interdum et malesuada fames ac ante ipsum primis in faucibus. Phasellus dolor eros, finibus sit amet nisl pellentesque, sollicitudin fermentum nisl. Pellentesque sollicitudin leo velit, et tempus tellus tempor quis. Mauris ut diam ut orci imperdiet faucibus.\n")
	fmt.Printf("Aliquam accumsan ut tortor id cursus. Donec tincidunt ullamcorper ligula sed finibus. Maecenas ac turpis a dui placerat eleifend. Aenean suscipit ut turpis sit amet feugiat. Maecenas porta commodo sapien non tempus. Curabitur bibendum fermentum libero vel dapibus. Maecenas vitae tellus in massa aliquet lacinia. Fusce dictum mi tortor, sit amet vestibulum lectus suscipit suscipit. Pellentesque metus nisi, sodales quis semper eu, iaculis at velit.\n")
}

func CreateBytes(len byte) []byte {
	res := make([]byte, len)
	for i := (byte)(0); i < len; i++ {
		res[i] = i
	}
	return res
}

// NewLogNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries.
//
// To shut down, close proposeC and read errorC.
//
// The store_path is used as the actual data directory.
func NewLogNode(storePath string, id int, remoteStorageHostname string, remoteStorage string, shouldLoadDataFromRemoteStorage bool,
	peerAddresses []string, peerIDs []int, join bool, httpDebugPort int, deploymentMode string) *LogNode {

	defer finalize()
	_, _ = fmt.Fprintf(os.Stderr, "Creating a new LogNode [version %v].\n", VersionText)

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGSEGV)

	if len(peerAddresses) != len(peerIDs) {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Received unequal number of peer addresses (%d) and peer node IDs (%d). They must be equal.\n", len(peerAddresses), len(peerIDs))
		return nil
	}

	_, _ = fmt.Fprintf(os.Stderr, "Checking validity of remote remote_storage hostname.\n")

	remoteStorage = strings.TrimSpace(remoteStorage)
	remoteStorage = strings.ToLower(remoteStorage)

	if len(remoteStorageHostname) == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Cannot connect to remote remote_storage %s; no hostname received.", remoteStorage)
		return nil
	}

	_, _ = fmt.Fprintf(os.Stderr, "Creating LogNode struct now.\n")

	node := &LogNode{
		proposeC:                        make(chan *proposalContext),
		confChangeC:                     make(chan *confChangeContext),
		commitC:                         make(chan *commit),
		errorC:                          make(chan error, 1),
		id:                              id,
		join:                            join,
		snapCount:                       defaultSnapshotCount,
		stopChannel:                     make(chan struct{}),
		httpStopChannel:                 make(chan struct{}),
		httpDoneChannel:                 make(chan struct{}),
		numChanges:                      0,
		deniedChanges:                   0,
		proposalRegistry:                hashmap.NewConcurrentMap[smrContext](32),
		snapshotterReady:                make(chan LogSnapshotter, 1),
		dataDir:                         storePath, // The base path of store_path is the persistent ID.
		shouldLoadDataFromRemoteStorage: shouldLoadDataFromRemoteStorage,
		httpDebugPort:                   httpDebugPort,
		deploymentMode:                  deploymentMode,
		atom:                            zap.NewAtomicLevelAt(zap.DebugLevel),
	}

	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), os.Stdout, node.atom)
	node.logger = zap.New(core, zap.Development())
	node.sugaredLogger = node.logger.Sugar()

	if node.sugaredLogger == nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to create sugared version of Zap development logger.")
		return nil
	}

	node.ServeHttpDebug()

	if storePath != "" {
		node.waldir = path.Join(storePath, fmt.Sprintf("dnlog-%d", id))
		node.snapdir = path.Join(storePath, fmt.Sprintf("dnlog-%d-snap", id))

		node.sugaredLogger.Infof("LogNode %d WAL directory: '%s'", id, node.waldir)
		node.sugaredLogger.Infof("LogNode %d Snapshot directory: '%s'", id, node.snapdir)

		fmt.Printf("LogNode %d WAL directory: '%s'", id, node.waldir)
		fmt.Printf("LogNode %d Snapshot directory: '%s'\n", id, node.snapdir)
	}

	testId, _ := node.patchPropose(nil)
	node.proposalPadding = len(testId)

	node.peers = make(map[int]string, len(peerAddresses))
	for i := 0; i < len(peerAddresses); i++ {
		peerAddr := peerAddresses[i]
		peerId := peerIDs[i]
		node.logger.Info("Discovered peer.", zap.String("peer_address", peerAddr), zap.Int("peer_id", peerId))
		fmt.Printf("Discovered peer %d: %s.\n", peerId, peerAddr)
		node.peers[peerId] = peerAddr
	}

	node.logger.Info("Creating remote remote_storage provider now.",
		zap.String("hostname", remoteStorageHostname),
		zap.String("deployment_mode", deploymentMode),
		zap.String("remote_storage", remoteStorage))

	if remoteStorage == hdfsRemoteStorage {
		node.storageProvider = storage.NewHdfsProvider(remoteStorageHostname, deploymentMode, node.id, &node.atom)
	} else if remoteStorage == redisRemoteStorage {
		node.storageProvider = storage.NewRedisProvider(remoteStorageHostname, deploymentMode, node.id, &node.atom)
	} else if remoteStorage == localStorage {
		node.storageProvider = storage.NewLocalProvider(deploymentMode, node.id, &node.atom)
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Invalid remote remote_storage specified: \"%s\". Must be \"hdfs\" or \"redis\".",
			remoteStorage)
		return nil
	}

	if node.storageProvider == nil {
		node.logger.Error("Failed to create remote remote_storage provider.",
			zap.String("remote_storage", remoteStorage),
			zap.String("hostname", remoteStorageHostname))
		return nil
	}

	err := node.storageProvider.Connect()
	if err != nil {
		node.logger.Error("Failed to connect to remote remote_storage.",
			zap.String("hostname", remoteStorageHostname),
			zap.String("deployment_mode", deploymentMode),
			zap.String("remote_storage", remoteStorage),
			zap.Error(err))
	}

	// TODO(Ben): Read the data directory from remote remote_storage.
	if shouldLoadDataFromRemoteStorage {
		node.logger.Info(fmt.Sprintf("Reading data directory from remote remote_storage now: %s.", node.dataDir))

		// TODO: Make configurable, or make this interval longer to support larger recoveries.
		// Alternatively, have the Goroutine that's reading the data from remote remote_storage periodically indicate that it is still alive/making progress,
		// and as long as that is happening, we continue waiting, with a timeout such that no progress after <timeout> means the whole operation has failed.
		timeoutInterval := 60 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeoutInterval)
		defer cancel()

		st := time.Now()
		progressChan := make(chan string, 8)
		errorChan := make(chan error)
		go func() {
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGSEGV)
			defer finalize()

			// TODO(Ben): Read the 'serialized state' file as well, and return that data back to the Python layer.
			serializedStateBytes, err := node.readDataDirectoryFromRemoteStorage(progressChan)
			if err != nil {
				errorChan <- err
				return
			}
			node.serializedStateBytes = serializedStateBytes
			progressChan <- DoneString
		}()

		tickInterval := 10 * time.Second
		ticker := time.NewTicker(tickInterval)
		noProgress := 0
		done := false
		for !done {
			select {
			case msg := <-progressChan:
				{
					if msg == DoneString { // If we received the special 'DONE' message, then we're done reading the entire data directory.
						node.remoteStorageReadTime = time.Since(st)
						node.logger.Info("Successfully read entire data directory from remote remote_storage to local remote_storage and received serialized state from other goroutine.", zap.Duration("time_elapsed", node.remoteStorageReadTime))
						done = true
					} else /* The message we received will be the path of whatever file or directory was copied from remote remote_storage to our local file system */ {
						node.logger.Info("Made progress.", zap.String("msg", msg))
						noProgress = 0
					}
				}
			case <-ticker.C:
				{
					noProgress += 1
					node.sugaredLogger.Warn("Progress has not been made in roughly %v.", tickInterval*time.Duration(noProgress))
					continue
				}
			case <-ctx.Done():
				{
					err := ctx.Err()
					node.logger.Error("Operation to read data from remote remote_storage timed-out.",
						zap.Duration("timeout_interval", timeoutInterval), zap.Error(err))
					ticker.Stop()
					return nil
				}
			case err := <-errorChan:
				{
					node.logger.Error("Error while reading data directory from remote remote_storage.",
						zap.Error(err), zap.String("data_dir", node.dataDir), zap.String("waldir", node.waldir), zap.String("data_directory", node.dataDir))
					ticker.Stop()
					return nil
				}
			}
		}

		ticker.Stop()
	} else {
		node.logger.Info("We've not been instructed to retrieve any data directory from remote remote_storage.",
			zap.String("data_directory", node.dataDir), zap.String("waldir", node.waldir))
	}

	debug.SetPanicOnFault(true)

	_, _ = fmt.Fprintf(os.Stderr, "Returning LogNode now.\n")

	return node
}

func (node *LogNode) ServeHttpDebug() {
	if node.httpDebugPort < 0 {
		node.logger.Warn("Debug port is negative. HTTP debug server is disabled.")
		return
	}

	go func() {
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGSEGV)

		log.Printf("Serving debug HTTP server on port %d.\n", node.httpDebugPort)

		if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", node.httpDebugPort), nil); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to serve HTTP debug server on port %d because: %v\n",
				node.httpDebugPort, err)

			log.Fatal("ListenAndServe: ", err)
		}
	}()
}

// RemoteStorageReadLatencyMilliseconds returns the latency of the remote remote_storage read operation(s) performed by the LogNode.
// If the LogNode did not read data from remote remote_storage, then -1 is returned.
func (node *LogNode) RemoteStorageReadLatencyMilliseconds() int {
	if !node.shouldLoadDataFromRemoteStorage {
		return -1
	}

	return int(node.remoteStorageReadTime.Milliseconds())
}

// ConnectedToRemoteStorage returns true if we successfully connected to remote remote_storage.
func (node *LogNode) ConnectedToRemoteStorage() bool {
	return node.storageProvider.ConnectionStatus() == storage.Connected
}

func (node *LogNode) NumChanges() int {
	return int(node.numChanges)
}

type startError struct {
	ErrorOccurred bool
	Error         error
}

func (node *LogNode) Start(config *LogNodeConfig) bool {
	node.sugaredLogger.Infof("LogNode %d is starting with config: %v", node.id, config.String())

	node.config = config
	if !config.Debug {
		node.logger = node.logger.WithOptions(zap.IncreaseLevel(zapcore.InfoLevel))
	}

	startErrorChan := make(chan startError)

	go func() {
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGSEGV)
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
	_ = node.WaitToClose()
}

// GetSerializedState returns the serialized_state_json field.
// This field is populated by ReadDataDirectoryFromRemoteStorage if there is a serialized state file to be read.
// It is only required during migration/error recovery.
func (node *LogNode) GetSerializedState() []byte {
	node.sugaredLogger.Infof("Returning serialized state of size/length %d.", len(node.serializedStateBytes))
	return node.serializedStateBytes
}

// Propose appends the difference of the value of specified key to the synchronization queue.
func (node *LogNode) Propose(val Bytes, resolve ResolveCallback, msg string) {
	_, ctx := node.generateProposal(val.Bytes(), ProposalDeadline)
	go func() {
		// signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
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
	defer finalize()

	if resolve == nil {
		// node.logger.Info("No Python callback provided. Using default resolve callback.")
		resolve = node.defaultResolveCallback
	}
	// else {
	// node.sugaredLogger.Infof("Provided Python resolve callback. Message: %s", msg)
	// }

	// node.logger.Info("Proposing to append value", zap.String("key", msg), zap.String("id", ctx.ID()))
	if err := proposer(ctx); err != nil {
		node.logger.Error("Exception while proposing value.", zap.String("key", msg), zap.String("id", ctx.ID()), zap.Error(err))
		resolve(msg, toCError(err))
		return
	}
	// node.logger.Info("Proposed value. Waiting for committed or retry.", zap.String("key", msg), zap.String("id", ctx.ID()))
	// Wait for committed or retry
	for !node.waitProposal(ctx) {
		// node.logger.Info("Retry proposing to append value", zap.String("key", msg), zap.String("id", ctx.ID()))
		ctx.Reset(ProposalDeadline)
		if err := proposer(ctx); err != nil {
			node.logger.Error("Exception while retrying value proposal.", zap.String("key", msg), zap.String("id", ctx.ID()), zap.Error(err))
			resolve(msg, toCError(err))
			return
		}
	}
	node.logger.Info("Value appended", zap.String("key", msg), zap.String("id", ctx.ID()))
	if resolve != nil {
		node.logger.Info("Calling `resolve` callback.", zap.Any("resolve-callback", resolve), zap.String("msg", msg))
		resolve(msg, toCError(nil))
	} else {
		node.logger.Info("There is no `resolve` callback to invoke.", zap.String("msg", msg))
	}
}

func (node *LogNode) manageNode(ctx smrContext) error {
	select {
	case node.confChangeC <- ctx.(*confChangeContext):
		return nil
	case <-node.stopChannel:
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

// Close closes the LogNode.
//
// NOTE: Close does NOT close the remote remote_storage client.
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

func (node *LogNode) CloseRemoteStorageClient() error {
	if node.storageProvider != nil {
		err := node.storageProvider.Close()
		if err != nil {
			node.logger.Error("Error while closing remote remote_storage client.", zap.Error(err))
			return err
		}
	} else {
		node.logger.Warn("Remote remote_storage client is nil. Will skip closing it.")
		return ErrRemoteStorageClientNil
	}

	return nil
}

// IMPORTANT: This does NOT close the remote remote_storage client.
// This is because, when migrating a raft cluster member, we must first stop the raft
// node before copying the contents of its data directory.
func (node *LogNode) close() {
	node.logger.Info("Closing nodes...")
	// Clear node channels
	proposeC, confChangeC := node.proposeC, node.confChangeC
	node.proposeC, node.confChangeC = nil, nil

	if !node.started {
		close(node.stopChannel)
	} else {
		// Signal the routine that depends on input channels to stop.
		// Will trigger the close(stopChannel).
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
	defer finalize()

	node.sugaredLogger.Infof("LogNode %d is beginning start procedure now.", node.id)
	debug.SetPanicOnFault(true)

	if node.isSnapEnabled() {
		node.logger.Info("Snapshots are enabled.")
		if !fileutil.Exist(node.snapdir) {
			if err := os.Mkdir(node.snapdir, 0750); err != nil {
				node.logFatalf("LogNode: cannot create directory \"%s\" for snapshot because: %v", node.snapdir, err)
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
		node.logger.Info(fmt.Sprintf("WAL is enabled. Old WAL ('%s') available? %v", node.waldir, oldWALExists))
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

	node.logger.Info("Replaying has completed.")

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
		node.sugaredLogger.Infof("LogNode %d will be restarting, as the old WAL directory is available (%v), and/or we've been told to join (%v).", node.id, oldWALExists, node.join)
		node.node = raft.RestartNode(c)
	} else {
		node.sugaredLogger.Infof("LogNode %d will be starting (as if for the first time), as the old WAL directory is not available (%v) and we've not been instructed to join (%v).", node.id, oldWALExists, node.join)
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
	err := node.transport.Start()
	if err != nil {
		panic(err)
	}
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

func (node *LogNode) readDataDirectoryFromRemoteStorage(progressChan chan<- string) ([]byte, error) {
	return node.storageProvider.ReadDataDirectory(progressChan, node.dataDir, node.waldir, node.snapdir)
}

// WriteDataDirectoryToRemoteStorage writes the data directory for this Raft node from local remote_storage to remote remote_storage.
func (node *LogNode) WriteDataDirectoryToRemoteStorage(serializedState []byte, resolve ResolveCallback) {
	go func() {
		err := node.storageProvider.WriteDataDirectory(serializedState, node.dataDir, node.waldir, node.snapdir)

		if err != nil {
			node.logger.Error("Error while writing data directory to remote remote_storage.", zap.Error(err))

			if resolve != nil {
				resolve(node.waldir, toCError(err))
			}

			return
		}

		resolve(node.waldir, toCError(nil))
	}()
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (node *LogNode) publishEntries(entries []raftpb.Entry) (<-chan struct{}, bool) {
	if len(entries) == 0 {
		return nil, true
	}

	data := make([][]byte, 0, len(entries))
	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				// ignore empty messages
				break
			}
			data = append(data, entries[i].Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			_ = cc.Unmarshal(entries[i].Data)

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
		case <-node.stopChannel:
			return nil, false
		}

		// after commit, update num_changes
		node.numChanges += uint64(len(data))
	}

	// after commit, update appliedIndex
	node.appliedIndex = entries[len(entries)-1].Index

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
		if err != nil && !errors.Is(err, snap.ErrNoSnapshot) {
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
		_ = w.Close()
		node.logger.Info(fmt.Sprintf("Successfully created WAL direcotry: \"%s\"", node.waldir), zap.String("uri", node.waldir))
	}

	walSnapshot := walpb.Snapshot{}
	if snapshot != nil {
		walSnapshot.Index, walSnapshot.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(zap.NewExample(), node.waldir, walSnapshot)
	if err != nil {
		node.logFatalf("LogNode: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (node *LogNode) replayWAL() *wal.WAL {
	node.logger.Info("Replaying WAL now.")
	snapshot := node.loadSnapshot()
	// Restore kernel from snapshot
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		node.logger.Info("Will attempt to restore kernel from snapshot.")
		if node.config.onRestore == nil {
			node.logFatalf("LogNode %d: no RestoreCallback configured on start.", node.id)
			return nil
		}
		node.sugaredLogger.Infof("Passing snapshot.Data (of length %d) to onRestore callback now.", len(snapshot.Data))
		if err := fromCError(node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshot.Data)}, len(snapshot.Data))); err != nil {
			node.logFatalf("LogNode %d: failed to restore states (%v)", node.id, err)
			return nil
		}
		node.logger.Info("Successfully restored data from Raft snapshot.", zap.Int("snapshot-size-bytes", len(snapshot.Data)))
	}

	w := node.openWAL(snapshot)
	if w == nil {
		node.sugaredLogger.Warn("Failed to open WAL snapshot.", zap.String("waldir", node.waldir))
		return w
	}

	node.logger.Info("Successfully opened WAL via snapshot")

	// Recover the in-memory remote_storage from persistent snapshot, state and entries.
	_, st, ents, err := w.ReadAll()
	if err != nil {
		node.logFatalf("LogNode %d: failed to read WAL (%v)", node.id, err)
		return nil
	}
	if snapshot != nil {
		node.logger.Info("Applying Snapshot to RaftStorage now.")
		err = node.raftStorage.ApplySnapshot(*snapshot)
		if err != nil {
			node.logFatalf("LogNode %d: error encountered while applying WAL snapshot to Raft remote_storage: %v", node.id, err)
		}
	}
	node.sugaredLogger.Infof("Recovered Raft HardState from WAL snapshot. Term: %d. Vote: %d. Commit: %v.", st.Term, st.Vote, st.Commit)
	node.sugaredLogger.Infof("Recovered %d entry/entries from WAL snapshot.", len(ents))
	err = node.raftStorage.SetHardState(st)
	if err != nil {
		node.logFatalf("LogNode %d: error encountered while setting Raft HardState to the HardState recovered from WAL snapshot: %v", node.id, err)
	}

	// Append to remote_storage so raft starts at the right place in log
	err = node.raftStorage.Append(ents)
	if err != nil {
		node.logFatalf("LogNode %d: error encountered while appending entries recovered from WAL snapshot to Raft remote_storage: %v", node.id, err)
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
	close(node.httpStopChannel)
	<-node.httpDoneChannel

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
	node.sugaredLogger.Infof("Calling onRestore callback with snapshotToSave.Data of length %d", len(snapshotToSave.Data))
	node.config.onRestore(&readerWrapper{reader: bytes.NewBuffer(snapshotToSave.Data)}, len(snapshotToSave.Data))

	node.confState = snapshotToSave.Metadata.ConfState
	node.snapshotIndex = snapshotToSave.Metadata.Index
	node.appliedIndex = snapshotToSave.Metadata.Index
	node.numChanges = 0
}

var snapshotCatchUpEntriesN uint64 = 1000

func (node *LogNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if !node.isSnapEnabled() ||
		node.config.getSnapshot == nil || // No snapshotCallback
		(node.config.shouldSnapshot == nil && node.appliedIndex-node.snapshotIndex <= node.snapCount) { // No shouldSnapshot and failed default test.
		return
	} else if node.config.shouldSnapshot != nil && (node.numChanges == node.deniedChanges || !node.config.shouldSnapshot(node)) { // Failed shouldSnapshot test
		// Update denied_changes to avoid triggering snapshot too frequently.
		node.deniedChanges = node.numChanges
		return
	}

	node.logger.Info("should snapshot passed")

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-node.stopChannel:
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
		defer finalize()
		node.logger.Info("Beginning snapshot now.", zap.Uint64("applied index", node.appliedIndex), zap.Uint64("last index", node.snapshotIndex))
		if err := fromCError(node.config.getSnapshot(&writerWrapper{writer: writer})); err != nil {
			node.logger.Error("Failed to write snapshot data from Python.", zap.Error(err))
			_ = writer.CloseWithError(err)
		}
	}()

	node.logger.Info("start snapshot", zap.Uint64("applied index", node.appliedIndex), zap.Uint64("last index", node.snapshotIndex))
	data, err := io.ReadAll(reader)
	if err != nil {
		node.logger.Error("Failed to read snapshot data from Python.", zap.Error(err))
		node.panic(err)
		return
	}
	snapshot, err := node.raftStorage.CreateSnapshot(node.appliedIndex, &node.confState, data)
	if err != nil {
		node.logger.Error("Failed to create snapshot.", zap.Uint64("applied index", node.appliedIndex), zap.Error(err))
		node.panic(err)
		return
	}
	if err := node.saveSnap(snapshot); err != nil {
		node.logger.Error("Failed to save snapshot.", zap.Uint64("applied index", node.appliedIndex), zap.Error(err))
		node.panic(err)
		return
	}

	compactIndex := node.appliedIndex
	if node.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = node.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := node.raftStorage.Compact(compactIndex); err != nil {
		node.logger.Error("Failed to compact raft remote_storage.", zap.Uint64("compactIndex", compactIndex), zap.Error(err))
		node.panic(err)
		return
	}

	node.logger.Info("compacted log", zap.Uint64("index", compactIndex))
	node.snapshotIndex = node.appliedIndex
	node.numChanges = 0
}

func (node *LogNode) serveChannels(startErrorChan chan<- startError) {
	node.logger.Info(fmt.Sprintf("LogNode %d is serving channels.", node.id))

	snapshot, err := node.raftStorage.Snapshot()
	if err != nil {
		startErrorChan <- startError{
			ErrorOccurred: true,
			Error:         err,
		}
		node.panic(err)
		return
	}
	node.confState = snapshot.Metadata.ConfState
	node.snapshotIndex = snapshot.Metadata.Index
	node.appliedIndex = snapshot.Metadata.Index

	if node.wal != nil {
		defer node.wal.Close()
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		defer finalize()
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
					err := node.node.Propose(ctx, ctx.Proposal)
					if err != nil {
						node.logger.Error("Failed to propose value.", zap.Error(err))
					}
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
					err := node.node.ProposeConfChange(context.TODO(), *cc.ConfChange)
					if err != nil {
						node.logger.Error("Failed to propose configuration change.", zap.Error(err))
					}
					node.logger.Info(fmt.Sprintf("LogNode %d: finished proposing configuration change.", node.id))
				}
			}
		}

		node.logger.Info("Client closed channel(s). Shutting down Raft (if it isn't already shutdown).")
		// client closed channel; shutdown raft if not already
		close(node.stopChannel)
	}()
	node.started = true

	// apply commits to state machine
	go func() {
		defer finalize()
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
			case <-node.stopChannel:
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
			// if len(rd.CommittedEntries) > 0 || rd.HardState.Commit > 0 || len(rd.Entries) > 0 {
			// node.logger.Info("ready", zap.Int("num-committed-entries", len(rd.CommittedEntries)), zap.Uint64("hardstate.commit", rd.HardState.Commit))
			// node.sugaredLogger.Infof("Storing Raft entries to WAL. NumCommittedEntries: %d. NumEntries: %d. NumMessages: %d. HardState: [Term: %d, Vote: %d, Commit: %d].", len(rd.CommittedEntries), len(rd.Entries), len(rd.Messages), rd.HardState.Term, rd.HardState.Vote, rd.HardState.Commit)
			// }

			if node.wal != nil {
				err = node.wal.Save(rd.HardState, rd.Entries)
				if err != nil {
					node.logger.Error("Failed to save WAL.", zap.Error(err))
				}
			}
			if node.snapshotter != nil && !raft.IsEmptySnap(rd.Snapshot) {
				err = node.saveSnap(rd.Snapshot)
				if err != nil {
					node.logger.Error("Failed to save snapshot.", zap.Error(err))
				}

				err = node.raftStorage.ApplySnapshot(rd.Snapshot)
				if err != nil {
					node.logger.Error("Failed to apply snapshot.", zap.Error(err))
				}

				node.publishSnapshot(rd.Snapshot)
			}

			err = node.raftStorage.Append(rd.Entries)
			if err != nil {
				node.logger.Error("Failed to append entries to Raft remote_storage.", zap.Error(err))
			}

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

		case <-node.stopChannel:
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
	defer finalize()
	defer close(node.httpDoneChannel)

	peerUrl, err := url.Parse(node.peers[node.id])
	if err != nil {
		node.logFatalf("LogNode: Failed parsing URL (%v)", err)
		return
	}

	ln, err := newStoppableListener(peerUrl.Host, node.httpStopChannel)
	if err != nil {
		node.logFatalf("LogNode: Failed to listen rafthttp (%v)", err)
		return
	}

	node.sugaredLogger.Infof("LogNode %d is beginning to serve Raft.", node.id)

	err = (&http.Server{Handler: node.transport.Handler()}).Serve(ln)
	select {
	case <-node.httpStopChannel:
	default:
		node.logFatalf("LogNode: Failed to serve rafthttp (%v)", err)
	}
}

func (node *LogNode) patchPropose(val []byte) (ret []byte, id string) {
	proposalUuid := uuid.New()
	binary, _ := proposalUuid.MarshalBinary()
	id = proposalUuid.String()
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
	case <-node.stopChannel:
		return ErrClosed
	}
}

func (node *LogNode) doneProposal(val []byte) ([]byte, smrContext) {
	if len(val) < node.proposalPadding {
		return val, nil
	}

	binary := val[len(val)-node.proposalPadding:]
	var id uuid.UUID
	err := id.UnmarshalBinary(binary)
	if err != nil {
		node.logger.Error("Failed to unmarshal binary proposal data.", zap.Error(err))
	}

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
	return errors.Is(ctx.Err(), context.Canceled)
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
