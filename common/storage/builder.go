package storage

import "github.com/scusemua/distributed-notebook/common/proto"

// RemoteStorageBuilder is a builder for constructing a RemoteStorage object.
type RemoteStorageBuilder struct {
	name                         string
	downloadRate                 int64
	uploadRate                   int64
	downloadVariancePercent      float32
	uploadVariancePercent        float32
	readFailureChancePercentage  float32
	writeFailureChancePercentage float32
}

// NewRemoteStorageBuilder creates and returns a new instance of RemoteStorageBuilder.
func NewRemoteStorageBuilder() *RemoteStorageBuilder {
	return &RemoteStorageBuilder{}
}

// WithName sets the name of the RemoteStorage.
func (b *RemoteStorageBuilder) WithName(name string) *RemoteStorageBuilder {
	b.name = name
	return b
}

// WithDownloadRate sets the download rate of the RemoteStorage.
func (b *RemoteStorageBuilder) WithDownloadRate(rate int64) *RemoteStorageBuilder {
	b.downloadRate = rate
	return b
}

// WithUploadRate sets the upload rate of the RemoteStorage.
func (b *RemoteStorageBuilder) WithUploadRate(rate int64) *RemoteStorageBuilder {
	b.uploadRate = rate
	return b
}

// WithDownloadVariancePercent sets the download variance percentage of the RemoteStorage.
func (b *RemoteStorageBuilder) WithDownloadVariancePercent(percent float32) *RemoteStorageBuilder {
	b.downloadVariancePercent = percent
	return b
}

// WithUploadVariancePercent sets the upload variance percentage of the RemoteStorage.
func (b *RemoteStorageBuilder) WithUploadVariancePercent(percent float32) *RemoteStorageBuilder {
	b.uploadVariancePercent = percent
	return b
}

// WithReadFailureChancePercentage sets the read failure chance percentage of the RemoteStorage.
func (b *RemoteStorageBuilder) WithReadFailureChancePercentage(percent float32) *RemoteStorageBuilder {
	b.readFailureChancePercentage = percent
	return b
}

// WithWriteFailureChancePercentage sets the write failure chance percentage of the RemoteStorage.
func (b *RemoteStorageBuilder) WithWriteFailureChancePercentage(percent float32) *RemoteStorageBuilder {
	b.writeFailureChancePercentage = percent
	return b
}

// Build constructs and returns a RemoteStorage object.
func (b *RemoteStorageBuilder) Build() *proto.RemoteStorageDefinition {
	return &proto.RemoteStorageDefinition{
		Name:                         b.name,
		DownloadRate:                 b.downloadRate,
		UploadRate:                   b.uploadRate,
		DownloadVariancePercent:      b.downloadVariancePercent,
		UploadVariancePercent:        b.uploadVariancePercent,
		ReadFailureChancePercentage:  b.readFailureChancePercentage,
		WriteFailureChancePercentage: b.writeFailureChancePercentage,
	}
}
