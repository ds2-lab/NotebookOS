package storage

import (
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type LocalProvider struct {
	*baseProvider
}

func NewLocalProvider(deploymentMode string, nodeId int, atom *zap.AtomicLevel) *LocalProvider {
	baseProvider := newBaseProvider("", deploymentMode, nodeId, atom)

	provider := &LocalProvider{
		baseProvider: baseProvider,
	}

	return provider
}

func (l *LocalProvider) Close() error {
	return nil
}

// WriteDataDirectory writes the data directory for this Raft node from local remote_storage to remote remote_storage.
func (l *LocalProvider) WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error {
	panic("Not implemented")
}

func (l *LocalProvider) ReadDataDirectory(ctx context.Context, progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error) {
	panic("Not implemented")
}
