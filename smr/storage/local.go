package storage

import "go.uber.org/zap"

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

// WriteDataDirectory writes the data directory for this Raft node from local storage to remote storage.
func (l *LocalProvider) WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error {
	panic("Not implemented")
}

func (l *LocalProvider) ReadDataDirectory(progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error) {
	panic("Not implemented")
}
