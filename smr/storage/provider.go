package storage

import (
	"fmt"
	"go.uber.org/zap"
	"os"
)

const (
	Connected    ConnectionStatus = "CONNECTED"
	Connecting   ConnectionStatus = "CONNECTING"
	Disconnected ConnectionStatus = "DISCONNECTED"

	SerializedStateDirectory       string = "serialized_raft_log_states"
	SerializedStateBaseFileName    string = "serialized_state.json"
	SerializedStateFileExtension   string = ".json"
	NewSerializedStateBaseFileName string = "serialized_state_new"
)

// ConnectionStatus indicates the status of the connection with the remote storage.
type ConnectionStatus string

// Provider is a generic API for reading and writing to an arbitrary intermediate storage medium, such
// as Redis, AWS S3, or HDFS.
type Provider interface {
	Connect() error

	Close() error

	// ConnectionStatus returns the current ConnectionStatus of the Provider.
	ConnectionStatus() ConnectionStatus

	// WriteDataDirectory writes the data directory for this Raft node from local storage to remote storage.
	WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error

	ReadDataDirectory(progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error)
}

type baseProvider struct {
	logger        *zap.Logger
	sugaredLogger *zap.SugaredLogger

	status ConnectionStatus

	hostname       string
	deploymentMode string
	nodeId         int

	instance Provider
}

func newBaseProvider(hostname string, deploymentMode string, nodeId int, atom *zap.AtomicLevel) *baseProvider {
	provider := &baseProvider{
		hostname:       hostname,
		deploymentMode: deploymentMode,
		status:         Disconnected,
		nodeId:         nodeId,
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to create Zap Development logger because: %v\n", err)
		return nil
	}
	provider.logger = logger
	provider.sugaredLogger = logger.Sugar()

	if provider.sugaredLogger == nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to create sugared version of Zap development logger.")
		return nil
	}

	return provider
}

// ConnectionStatus returns the current ConnectionStatus of the Provider.
func (p *baseProvider) ConnectionStatus() ConnectionStatus {
	return p.status
}

func (p *baseProvider) Connect() error {
	return p.instance.Connect()
}
