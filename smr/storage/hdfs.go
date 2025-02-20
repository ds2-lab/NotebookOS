package storage

import (
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"io"
	"io/fs"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
)

const (
	defaultHdfsUsername string = "jovyan"
)

// HdfsProvider implements the Provider API for redis.
type HdfsProvider struct {
	*baseProvider

	hdfsUsername string
	hdfsClient   *hdfs.Client
}

func NewHdfsProvider(hostname string, deploymentMode string, nodeId int, atom *zap.AtomicLevel) *HdfsProvider {
	baseProvider := newBaseProvider(hostname, deploymentMode, nodeId, atom)

	provider := &HdfsProvider{
		baseProvider: baseProvider,
		hdfsUsername: defaultHdfsUsername,
	}

	return provider
}

// SetHdfsUsername sets the username to use when connecting to HDFS.
//
// If the HdfsProvider is already connected to HDFS, then changing the username will not have an effect
// unless the HdfsProvider reconnects to HDFS.
func (p *HdfsProvider) SetHdfsUsername(user string) {
	p.hdfsUsername = user
}

func (p *HdfsProvider) Close() error {
	if p.hdfsClient == nil {
		return nil
	}

	return p.hdfsClient.Close()
}

func (p *HdfsProvider) Connect() error {
	p.logger.Debug("Connecting to remote storage",
		zap.String("remote_storage", "hdfs"),
		zap.String("hostname", p.hostname))

	p.status = Connecting

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{p.hostname},
		User:      p.hdfsUsername,
		NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext(ctx, network, address)
			if err != nil {
				p.sugaredLogger.Errorf("Failed to dial HDFS DataNode at address '%s' with network '%s' because: %v", address, network, err)
				return nil, err
			}
			return conn, nil
		},
		// Temporary work-around to deal with Kubernetes networking issues with HDFS.
		// The HDFS NameNode returns the IP for the client to use to connect to the DataNode for reading/writing file blocks.
		// At least for development/testing, I am using a local Kubernetes cluster and a local HDFS deployment.
		// So, the HDFS NameNode returns the local IP address. But since Kubernetes Pods have their own local host, they cannot use this to connect to the HDFS Datap.
		DatanodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			if p.deploymentMode == "LOCAL" || p.deploymentMode == "DOCKER_COMPOSE" {
				// If it is local, then we just use the loopback address (or whatever) to the host,
				// which is where the data node is running.
				originalAddress := address
				dataNodeAddress := strings.Split(p.hostname, ":")[0]
				dataNodePort := strings.Split(address, ":")[1]                // Get the port that the DataNode is using. Discard the IP address.
				address = fmt.Sprintf("%s:%s", dataNodeAddress, dataNodePort) // returns the IP address that will enable the local k8s Pods to find the local Datap.
				p.logger.Debug("Modified HDFS DataNode address.", zap.String("original_address", originalAddress), zap.String("updated_address", address))
			}

			p.logger.Info("Dialing HDFS Datap.", zap.String("datanode_address", address))

			childCtx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext(childCtx, network, address)
			if err != nil {
				p.sugaredLogger.Errorf("Failed to dial HDFS DataNode at address '%s' because: %v", address, err)
				return nil, err
			}

			return conn, nil
		},
	})

	if err != nil {
		p.logger.Error("Failed to create HDFS client.", zap.String("remote_storage_hostname", p.hostname), zap.Error(err))
		return err
	} else {
		p.sugaredLogger.Infof("Successfully connected to HDFS at '%s'", p.hostname)
		fmt.Printf("Successfully connected to HDFS at '%s'\n", p.hostname)
		p.hdfsClient = hdfsClient
	}

	p.status = Connected

	return nil
}

// WriteDataDirectory writes the data directory for this Raft node from local storage to HDFS.
func (p *HdfsProvider) WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error {
	p.logger.Debug("Writing data directory to HDFS.",
		zap.String("WAL directory", waldir),
		zap.String("snapshot directory", snapdir),
		zap.String("data directory", datadir),
		zap.Int("num_bytes", len(serializedState)))

	debug.SetPanicOnFault(true)
	err := p.hdfsClient.MkdirAll(waldir, os.FileMode(0777))
	if err != nil {
		// If we return an error from this function, then WalkDir will stop entirely and return that error.
		p.logger.Error("Exception encountered while trying to create HDFS directory for node's WAL directory.",
			zap.String("wal-directory", waldir), zap.Error(err))
		return err
	}

	err = p.hdfsClient.MkdirAll(snapdir, os.FileMode(0777))
	if err != nil {
		// If we return an error from this function, then WalkDir will stop entirely and return that error.
		p.logger.Error("Exception encountered while trying to create HDFS directory for node's snapshot directory.",
			zap.String("snapdir-directory", snapdir), zap.Error(err))
		return err
	}

	err = p.writeRaftLogSerializedStateToHdfs(serializedState, datadir)
	if err != nil {
		p.logger.Error("Failed to write the RaftLog's serialized state to HDFS.", zap.Error(err))
		return err
	}

	// Write the WAL directory to HDFS.
	walDirErr := p.writeLocalDirectoryToHdfs(waldir)
	if walDirErr != nil {
		return walDirErr
	}

	// Write the snapshot directory to HDFS.
	snapDirErr := p.writeLocalDirectoryToHdfs(snapdir)
	if snapDirErr != nil {
		return snapDirErr
	}

	return nil
}

// writeRaftLogSerializedStateToHdfs writes the RaftLog's serialized state to HDFS.
func (p *HdfsProvider) writeRaftLogSerializedStateToHdfs(serializedState []byte, dataDirectory string) error {
	serializedStateFileDir := filepath.Join(dataDirectory, SerializedStateDirectory)
	err := p.hdfsClient.MkdirAll(serializedStateFileDir, os.FileMode(0777))
	if err != nil {
		// If we return an error from this function, then WalkDir will stop entirely and return that error.
		p.logger.Error("Exception encountered while trying to create HDFS directory for serialized RaftLog states.",
			zap.String("directory", serializedStateFileDir), zap.Error(err))
		return err
	}

	serializedStateFilename := SerializedStateBaseFileName + fmt.Sprintf("-node%d", p.nodeId) + SerializedStateFileExtension
	serializedStateFilepath := filepath.Join(serializedStateFileDir, serializedStateFilename)
	// new_serialized_state_filepath := filepath.Join(serialized_state_file_dir, SerializedStateFile)

	var alreadyExists = false
	if _, err := p.hdfsClient.Stat(serializedStateFilepath); err == nil {
		// The file already exists. We'll write our state to another file.
		// If that is successful, then we'll delete the old one and replace it with the new one.
		newSerializedStateFilename := NewSerializedStateBaseFileName + fmt.Sprintf("-node%d", p.nodeId) + SerializedStateFileExtension
		serializedStateFilepath = filepath.Join(serializedStateFileDir, newSerializedStateFilename)
		alreadyExists = true
	}

	writer, err := p.hdfsClient.Create(serializedStateFilepath)
	if err != nil {
		p.logger.Error("Failed to create 'serialized state' file.", zap.String("path", serializedStateFilepath), zap.Error(err))

		// TODO: Handle gracefully, somehow?
		return err
	}

	_, err = writer.Write(serializedState)
	if err != nil {
		p.logger.Error("Error while writing serialized state to file.",
			zap.String("path", serializedStateFilepath), zap.Error(err))
		_ = writer.Close() // This could also fail.
		return err
	}

	st := time.Now()

	for time.Since(st) < (time.Minute * 2) {
		err = writer.Close()

		if err == nil { /* Success */
			break
		}

		// If replication is in progress, then we'll sleep for a few seconds before trying again.
		if hdfs.IsErrReplicating(err) {
			p.logger.Error("Cannot close serialized state file; replication is in progress.",
				zap.Duration("time-elapsed", time.Since(st)))

			// Sleep for a random interval between 2 and 5 seconds.
			// rand.Intn(4) generates an intenger in the range [0, 4] (i.e., 0, 1, 2, or 3).
			// The minimum is thus 2, and the maximum is 5.
			time.Sleep(time.Second * time.Duration(rand.Intn(4)+2))
			continue
		}

		// Some other error.
		// TODO: Handle this more gracefully?
		p.logger.Error("Failed to close serialized state file.",
			zap.String("path", serializedStateFilepath), zap.Error(err))
		return err
	}

	// If there was already an existing 'serialized state' file in the data directory, then we'll now delete the old one and replace it with the new one.
	if alreadyExists {
		destFilename := SerializedStateBaseFileName + fmt.Sprintf("-node%d", p.nodeId) + SerializedStateFileExtension
		destFilepath := filepath.Join(serializedStateFileDir, destFilename)

		// RemoveHost the existing file. We'll copy the new one in its place.
		err = p.hdfsClient.Remove(destFilepath)

		if err != nil {
			p.logger.Error("Failed to remove existing 'serialized state' file.",
				zap.String("path", destFilepath), zap.Error(err))
			// Don't return here. Try the rename operation, in case the file was already deleted for some reason.
		}

		// Rename the new one, which has the same name with a "_new" suffix.
		err = p.hdfsClient.Rename(serializedStateFilepath /* serialized_state_new.json */, destFilepath /* serialized_state.json */)
		if err != nil {
			p.logger.Error("Failed to rename new 'serialized state' file.",
				zap.String("old_path", serializedStateFilepath), zap.String("new_path", destFilepath))
		}
	}

	p.logger.Debug("Successfully wrote 'serialized state' to file.", zap.String("path", serializedStateFilepath))

	return nil
}

// Write the specified local directory to the same path within HDFS.
func (p *HdfsProvider) writeLocalDirectoryToHdfs(dir string) error {
	// Walk through the entire etcd-raft data directory, copying each file one-at-a-time to HDFS.
	walkdirErr := filepath.WalkDir(dir, func(path string, d os.DirEntry, errArg error) error {
		// Note: the first entry found is the base directory passed to filepath.WalkDir (p.data_dir in this case).
		if d.IsDir() {
			p.logger.Info(fmt.Sprintf("Found local directory '%s'", path), zap.String("directory", path))
			err := p.hdfsClient.MkdirAll(path, os.FileMode(0777))
			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				p.logger.Error(fmt.Sprintf("Exception encountered while trying to create HDFS directory '%s': %v", path, err), zap.String("directory", path), zap.Error(err))
				return err
			}

			p.logger.Info(fmt.Sprintf("Successfully created remote (HDFS) directory: '%s'", path), zap.String("directory", path))
		} else {
			p.logger.Info(fmt.Sprintf("Found local file '%s'", path), zap.String("file", path))

			// If the file already exists...
			if _, err := p.hdfsClient.Stat(path); err == nil {
				// ... then we need to remove it and re-write it.
				// TODO (Ben): Can we optimize this so that we only need to add the new data?
				err = p.hdfsClient.Remove(path)
				if err != nil {
					p.logger.Error("Failed to remove existing file during re-write process.", zap.String("path", path), zap.Error(err))
					return err
				}
			}

			var numTries = 1

			// The p.hdfsClient has a CopyLocalToRemote function which does exactly what the code below does.
			// The only difference is that we retry remote.Close() in a loop until it stops returning ErrReplicating and succeeds.
			// Because hdfsClient.CopyLocalToRemote doesn't do this, we don't call that function and instead inline that function's logic below.

			// Open the local file that we'll be copying.
			local, err := os.Open(path)
			if err != nil {
				return err
			}

			// Create the remote file that we'll be copying to.
			// TODO: Switch to using Append to only write the new data.
			// TODO: Persist data back to intermediate remote_storage in the background.
			remote, err := p.hdfsClient.Create(path)
			if err != nil {
				return err
			}

			// Copy the local file to the remote HDFS file.
			// TODO: Switch to using Append to only write the new data.
			_, err = io.Copy(remote, local)
			if err != nil {
				p.logger.Error("Failed to copy local file to HDFS.", zap.String("path", path), zap.Error(err))
				_ = remote.Close()
				return err
			}

			// Close the local file.
			_ = local.Close()

			// Close the remote file.
			// Like the HDFS Java client, we'll keep retrying the Close operation if we receive an ErrReplicating.
			for {
				// Try to close the remote (HDFS) file.
				err := remote.Close()

				// If we received an error, then we'll handle it in one of two ways, depending on what the error is...
				if err != nil {
					// If it is a replication error, then we'll simply retry with exponential backoff until we close without an error.
					// This is what the Java HDFS client does.
					if hdfs.IsErrReplicating(err) {
						p.sugaredLogger.Warnf("Could not close file \"%s\" on attempt #%d; data is still being replicated. Will retry.", path, numTries)
						time.Sleep(time.Second * 2 * time.Duration(numTries))
						numTries += 1
						continue
					} else {
						// It's not a ErrReplication error, so something else went wrong...
						// If we return an error from this function, then WalkDir will stop entirely and return that error.
						p.sugaredLogger.Errorf("Exception encountered while trying to copy local-to-remote for file '%s': %v", path, err)
						return err
					}
				}

				// We successfully closed the remote file, so let's break out of the for-loop.
				break
			}

			p.logger.Info(fmt.Sprintf("Successfully copied local file to HDFS: '%s'", path), zap.String("file", path))
		}
		return nil
	})

	return walkdirErr
}

func (p *HdfsProvider) ReadDataDirectory(_ context.Context, progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error) {
	serializedStateBytes, err := p.readRaftLogSerializedStateFromHdfs(datadir)
	if err != nil {
		return nil, err
	}

	// Read the WAL dir (write-ahead log).
	err = p.readDirectoryFromHdfs(waldir, progressChannel)
	if err != nil {
		return serializedStateBytes, err
	}

	// Read the snapshot directory.
	err = p.readDirectoryFromHdfs(snapdir, progressChannel)

	return serializedStateBytes, err
}

func (p *HdfsProvider) readRaftLogSerializedStateFromHdfs(dataDirectory string) (serializedStateBytes []byte, err error) {
	serializedStateBytes = make([]byte, 0)

	serializedStateFileDir := filepath.Join(dataDirectory, SerializedStateDirectory)
	serializedStateFilename := SerializedStateBaseFileName + fmt.Sprintf("-node%d", p.nodeId) + SerializedStateFileExtension
	serializedStateFilepath := filepath.Join(serializedStateFileDir, serializedStateFilename)
	if _, err = p.hdfsClient.Stat(serializedStateFilepath); err == nil {
		serializedStateBytes, err = p.hdfsClient.ReadFile(serializedStateFilepath)
		if err != nil {
			p.logger.Error("Failed to read 'serialized state' from file.", zap.String("path", serializedStateFilepath), zap.Error(err))
			return
		}

		p.logger.Debug("Read serialized state contents from file.", zap.String("path", serializedStateFilepath))
	} else {
		p.logger.Debug("Did not find a serialized state file. Hopefully you weren't expecting one!")
	}

	return
}

func (p *HdfsProvider) readDirectoryFromHdfs(dir string, progressChannel chan<- string) error {
	p.sugaredLogger.Debugf("Walking directory (in HDFS), copying remote files and directories to local storage: '%s'", dir)
	walkErr := p.hdfsClient.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		p.sugaredLogger.Debugf("Processing file system object at path \"%s\"", path)
		p.sugaredLogger.Debugf("Base name: \"%s\", Len: %d bytes, Mode: %v, ModTime: %v, IsDir: %v", info.Name(), info.Size(), info.Mode(), info.ModTime(), info.IsDir())

		if info.IsDir() {
			// p.sugaredLogger.Debugf("Found remote directory '%s'", path)
			err := os.MkdirAll(path, os.FileMode(0777))
			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				p.sugaredLogger.Errorf("Exception encountered while trying to create local directory '%s': %v", path, err)
				return err
			}

			progressChannel <- path
			p.sugaredLogger.Debugf("Successfully created local directory '%s'", path)
		} else {
			// p.sugaredLogger.Debugf("Found remote file '%s'", path)
			err := p.hdfsClient.CopyToLocal(path, path)

			if err != nil {
				// If we return an error from this function, then WalkDir will stop entirely and return that error.
				p.sugaredLogger.Errorf("Exception encountered while trying to copy remote-to-local for file '%s': %v", path, err)
				return err
			}

			progressChannel <- path
			p.sugaredLogger.Debugf("Successfully copied remote HDFS file to local file system: '%s'", path)
		}

		return nil
	})

	if walkErr != nil {
		p.sugaredLogger.Errorf("Exception encountered while trying to copy HDFS file or directory '%s' to local storage: %v", dir, walkErr)
		return walkErr
	}

	p.sugaredLogger.Debugf("Successfully read all data from HDFS directory: \"%s\".", dir)

	return nil
}
