package storage

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"
)

// S3Provider implements the StorageProvider API for AWS S3.
type S3Provider struct {
	*baseProvider

	s3Client *s3.Client
	s3Bucket string
}

func NewS3Provider(hostname string, deploymentMode string, nodeId int, atom *zap.AtomicLevel) *S3Provider {
	return &S3Provider{
		baseProvider: newBaseProvider(hostname, deploymentMode, nodeId, atom),
	}
}

func (p *S3Provider) Close() error {
	return nil
}

func (p *S3Provider) Connect() error {
	p.logger.Debug("Connecting to remote remote_storage",
		zap.String("remote_storage", "AWS S3"),
		zap.String("hostname", p.hostname))

	p.status = Connecting

	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		p.logger.Error("Failed to load AWS SDK config", zap.Error(err))
		return err
	}

	p.s3Client = s3.NewFromConfig(sdkConfig)

	p.status = Connected

	p.logger.Debug("Successfully connected to remote remote_storage",
		zap.String("remote_storage", "AWS S3"),
		zap.String("hostname", p.hostname))

	return nil
}

// WriteDataDirectory writes the data directory for this Raft node from local remote_storage to AWS S3.
func (p *S3Provider) WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error {
	p.logger.Debug("Writing data directory to AWS S3.",
		zap.String("WAL directory", waldir),
		zap.String("snapshot directory", snapdir),
		zap.String("data directory", datadir),
		zap.String("bucket", p.s3Bucket),
		zap.Int("num_bytes", len(serializedState)))

	debug.SetPanicOnFault(true)

	ctx := context.Background()

	err := p.writeRaftLogSerializedStateToS3(ctx, serializedState, datadir)
	if err != nil {
		p.logger.Error("Failed to write the RaftLog's serialized state to AWS S3.",
			zap.String("data_directory", datadir), zap.String("bucket", p.s3Bucket), zap.Error(err))
		return err
	}

	// Write the WAL directory to AWS S3.
	walDirErr := p.writeLocalDirectoryToS3(ctx, waldir)
	if walDirErr != nil {
		p.logger.Error("Failed to write the RaftLog's WAL directory to AWS S3.",
			zap.String("wal_directory", waldir), zap.String("bucket", p.s3Bucket), zap.Error(err))
		return walDirErr
	}

	// Write the snapshot directory to AWS S3.
	snapDirErr := p.writeLocalDirectoryToS3(ctx, snapdir)
	if snapDirErr != nil {
		p.logger.Error("Failed to write the RaftLog's snapshot directory to AWS S3.",
			zap.String("snap_directory", snapdir), zap.String("bucket", p.s3Bucket), zap.Error(err))
		return snapDirErr
	}

	return nil
}

// writeRaftLogSerializedStateToS3 writes the RaftLog's serialized state to AWS S3.
func (p *S3Provider) writeRaftLogSerializedStateToS3(ctx context.Context, serializedState []byte, dataDirectory string) error {
	serializedStateFileDir := filepath.Join(dataDirectory, SerializedStateDirectory)

	serializedStateFilename := SerializedStateBaseFileName + fmt.Sprintf("-node%d", p.nodeId) + SerializedStateFileExtension
	serializedStateFilepath := filepath.Join(serializedStateFileDir, serializedStateFilename)

	_, err := p.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(p.s3Bucket),
		Key:    aws.String(serializedStateFilepath),
		Body:   bytes.NewReader(serializedState),
	})
	if err != nil {
		p.logger.Error("Error while writing serialized state to S3 file.",
			zap.String("path", serializedStateFilepath), zap.String("bucket", p.s3Bucket), zap.Error(err))
		return err
	}

	p.logger.Debug("Successfully wrote 'serialized state' to file.",
		zap.String("path", serializedStateFilepath), zap.String("bucket", p.s3Bucket))

	return nil
}

// Write the specified local directory to the same path within AWS S3.
func (p *S3Provider) writeLocalDirectoryToS3(ctx context.Context, dir string) error {
	// Walk through the entire etcd-raft data directory, copying each file one-at-a-time to AWS S3.
	walkdirErr := filepath.WalkDir(dir, func(path string, d os.DirEntry, errArg error) error {
		// Note: the first entry found is the base directory passed to filepath.WalkDir (p.data_dir in this case).
		if d.IsDir() {
			return nil
		}

		p.logger.Info(fmt.Sprintf("Found local file '%s'", path), zap.String("file", path))

		// The p.s3Client has a CopyLocalToRemote function which does exactly what the code below does.
		// The only difference is that we retry remote.Close() in a loop until it stops returning ErrReplicating and succeeds.
		// Because s3Client.CopyLocalToRemote doesn't do this, we don't call that function and instead inline that function's logic below.

		// Open the local file that we'll be copying.
		local, err := os.Open(path)
		if err != nil {
			return err
		}

		_, err = p.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(p.s3Bucket),
			Key:    aws.String(path),
			Body:   local,
		})
		if err != nil {
			p.logger.Error("Error while writing local file to S3.",
				zap.String("path", path), zap.String("bucket", p.s3Bucket), zap.Error(err))
			return err
		}

		p.logger.Info(fmt.Sprintf("Successfully copied local file to AWS S3: '%s'", path),
			zap.String("file", path), zap.String("bucket", p.s3Bucket))

		return nil
	})

	return walkdirErr
}

func (p *S3Provider) ReadDataDirectory(ctx context.Context, progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error) {
	serializedStateBytes, err := p.readRaftLogSerializedStateFromS3(ctx, datadir)
	if err != nil {
		return nil, err
	}

	// Read the WAL dir (write-ahead log).
	err = p.readDirectoryFromS3(ctx, waldir, progressChannel)
	if err != nil {
		return serializedStateBytes, err
	}

	// Read the snapshot directory.
	err = p.readDirectoryFromS3(ctx, snapdir, progressChannel)

	return serializedStateBytes, err
}

func (p *S3Provider) readRaftLogSerializedStateFromS3(ctx context.Context, dataDirectory string) ([]byte, error) {
	serializedStateFileDir := filepath.Join(dataDirectory, SerializedStateDirectory)
	serializedStateFilename := SerializedStateBaseFileName + fmt.Sprintf("-node%d", p.nodeId) + SerializedStateFileExtension
	serializedStateFilepath := filepath.Join(serializedStateFileDir, serializedStateFilename)

	readStart := time.Now()
	result, err := p.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.s3Bucket),
		Key:    aws.String(serializedStateFilepath),
	})

	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			p.logger.Debug("Could not retrieve serialized state from AWS S3 as specified key does not exist.",
				zap.String("key", serializedStateFilepath), zap.String("bucket", p.s3Bucket))

			return nil, nil
		} else {
			p.logger.Error("Could not retrieve serialized state from AWS S3.",
				zap.String("key", serializedStateFilepath),
				zap.String("bucket", p.s3Bucket),
				zap.Error(err))
		}

		return nil, err
	}

	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		p.logger.Error("Could not read serialized state from S3 response body.",
			zap.String("key", serializedStateFilepath),
			zap.String("bucket", p.s3Bucket),
			zap.Error(err))

		return nil, err
	}

	p.logger.Debug("Successfully read RaftLog's serialized state from AWS S3.",
		zap.String("key", serializedStateFilepath),
		zap.String("bucket", p.s3Bucket),
		zap.Duration("duration", time.Since(readStart)))

	return data, nil
}

func (p *S3Provider) readDirectoryFromS3(ctx context.Context, dir string, progressChannel chan<- string) error {
	p.logger.Debug("Walking directory (in AWS S3): copying files from S3 to local storage.",
		zap.String("directory", dir), zap.String("bucket", p.s3Bucket))

	s3Keys := make([]string, 0, 16)

	startTime := time.Now()

	// Pagination loop to retrieve all objects.
	var continuationToken *string
	for {
		output, err := p.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(p.s3Bucket),
			Prefix:            aws.String(dir),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			p.logger.Error("Failed to list AWS S3 objects while reading directory from AWS S3.",
				zap.String("directory", dir), zap.String("bucket", p.s3Bucket), zap.Error(err))

			return fmt.Errorf("failed to list AWS S3 objects: %w", err)
		}

		// Print object keys (file paths)
		for _, obj := range output.Contents {
			key := *obj.Key
			s3Keys = append(s3Keys, key)
			p.logger.Debug("Found S3 key in target directory.",
				zap.String("directory", dir), zap.String("key", key), zap.String("bucket", p.s3Bucket))
		}

		// If more objects exist, continue listing.
		if *output.IsTruncated {
			continuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	p.logger.Debug("Retrieving objects from AWS S3.",
		zap.String("directory", dir),
		zap.Int("num_keys", len(s3Keys)),
		zap.String("bucket", p.s3Bucket),
		zap.Duration("time_elapsed", time.Since(startTime)))

	// Iterate over all the keys.
	for idx, key := range s3Keys {
		p.logger.Debug("Retrieving object from AWS S3.",
			zap.String("directory", dir),
			zap.String("key", key),
			zap.Int("index", idx),
			zap.Int("total_num_keys", len(s3Keys)),
			zap.String("bucket", p.s3Bucket),
			zap.Duration("time_elapsed", time.Since(startTime)))

		// Extract the directory part of the file path.
		dir := filepath.Dir(key)
		readStart := time.Now()

		data, err := p.readObjectFromS3(ctx, key)
		if err != nil {
			p.logger.Error("Could not retrieve object from AWS S3.",
				zap.String("directory", dir),
				zap.String("key", key),
				zap.String("bucket", p.s3Bucket),
				zap.Error(err))
			return err
		}

		// Create all parent directories (locally) if they do not already exist.
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			p.logger.Debug("Failed to create parent dirs for local file while reading dir from AWS S3.",
				zap.String("directory", dir),
				zap.String("key", key),
				zap.String("bucket", p.s3Bucket),
				zap.Error(err))
			return fmt.Errorf("failed to create parent directories: %w", err)
		}

		// Create the local file.
		localFile, err := os.Create(key)
		if err != nil {
			p.logger.Debug("Failed to create local file while reading dir from AWS S3.",
				zap.String("directory", dir),
				zap.String("key", key),
				zap.String("bucket", p.s3Bucket),
				zap.Error(err))
			return fmt.Errorf("failed to create parent directories: %w", err)
		}

		defer localFile.Close()

		readDuration := time.Since(readStart)
		writeStart := time.Now()

		// Write the data to the local file.
		_, err = localFile.Write(data)
		if err != nil {
			p.logger.Error("Could not write object from S3 to local file.",
				zap.String("directory", dir),
				zap.String("key", key),
				zap.String("bucket", p.s3Bucket),
				zap.Error(err))
			return err
		}

		p.logger.Debug("Successfully read object from AWS S3.",
			zap.String("directory", dir),
			zap.String("key", key),
			zap.String("bucket", p.s3Bucket),
			zap.Duration("read_duration", readDuration),
			zap.Duration("write_duration", time.Since(writeStart)),
			zap.Duration("total_time_elapsed", time.Since(startTime)))

		return nil
	}

	p.logger.Debug("Successfully read all data from AWS S3 directory.",
		zap.String("directory", dir),
		zap.String("bucket", p.s3Bucket),
		zap.Duration("time_elapsed", time.Since(startTime)))

	return nil
}

// readObjectFromS3 reads a single object from AWS S3.
func (p *S3Provider) readObjectFromS3(ctx context.Context, key string) ([]byte, error) {
	// Read the data from AWS S3.
	result, err := p.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.s3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		p.logger.Error("Could not retrieve object from AWS S3.",
			zap.String("key", key),
			zap.String("bucket", p.s3Bucket),
			zap.Error(err))
		return nil, err
	}

	defer result.Body.Close()

	// Read the data from the S3 response body.
	data, err := io.ReadAll(result.Body)
	if err != nil {
		p.logger.Error("Could not read object from S3 response body.",
			zap.String("key", key),
			zap.String("bucket", p.s3Bucket),
			zap.Error(err))
		return nil, err
	}

	return data, nil
}
