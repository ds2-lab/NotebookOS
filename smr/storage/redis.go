package storage

import (
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
)

const (
	directoryPrefix string = "__dir__"
	filePrefix      string = "__file__"
)

var (
	ErrNoPathsFound = errors.New("no paths found in redis")
)

// RedisProvider implements the StorageProvider API for redis.
type RedisProvider struct {
	*baseProvider

	databaseIndex int
	password      string

	redisClient *redis.Client
}

func NewRedisProvider(hostname string, deploymentMode string, nodeId int, atom *zap.AtomicLevel) *RedisProvider {
	baseProvider := newBaseProvider(hostname, deploymentMode, nodeId, atom)

	provider := &RedisProvider{
		baseProvider:  baseProvider,
		databaseIndex: 0,
		password:      "",
	}

	return provider
}

func (p *RedisProvider) Close() error {
	if p.redisClient == nil {
		return nil
	}

	return p.redisClient.Close()
}

// SetDatabase sets the database number to use when connecting to Redis.
//
// If the RedisProvider is already connected to Redis, then changing the database number will not have an effect
// unless the RedisProvider reconnects to Redis.
func (p *RedisProvider) SetDatabase(db int) {
	p.databaseIndex = db
}

// SetRedisPassword sets the password to use when connecting to Redis.
//
// If the RedisProvider is already connected to Redis, then changing the password will not have an effect
// unless the RedisProvider attempts to reconnect to Redis.
func (p *RedisProvider) SetRedisPassword(password string) {
	p.password = password
}

func (p *RedisProvider) Connect() error {
	p.logger.Debug("Connecting to remote storage",
		zap.String("remote_storage", "redis"),
		zap.String("hostname", p.hostname))

	p.status = Connecting

	p.redisClient = redis.NewClient(&redis.Options{
		Addr:                  p.hostname,
		Password:              p.password,      // no password set
		DB:                    p.databaseIndex, // use default DB
		ContextTimeoutEnabled: true,
		DialTimeout:           time.Second * 30,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	res, err := p.redisClient.Ping(ctx).Result()
	if err != nil {
		p.logger.Error("Failed to connect to remote storage.",
			zap.String("remote_storage", "redis"),
			zap.String("hostname", p.hostname),
			zap.Error(err))

		return err
	}

	p.status = Connected

	p.logger.Debug("Successfully connected to remote storage",
		zap.String("remote_storage", "redis"),
		zap.String("hostname", p.hostname),
		zap.String("res", res))

	return nil
}

func (p *RedisProvider) WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error {
	st := time.Now()

	p.logger.Debug("Writing data directory to Redis.",
		zap.String("WAL directory", waldir),
		zap.String("snapshot directory", snapdir),
		zap.String("data directory", datadir),
		zap.Int("num_bytes", len(serializedState)))

	debug.SetPanicOnFault(true)

	err := p.writeSerializedState(datadir, serializedState)
	if err != nil {
		return err
	}

	// Write the WAL directory to Redis.
	walDirectoryPaths, walDirErr := p.writeLocalDirectoryToRedis(waldir)
	if walDirErr != nil {
		return walDirErr
	}

	// Write the snapshot directory to Redis.
	snapDirectoryPaths, snapDirErr := p.writeLocalDirectoryToRedis(snapdir)
	if snapDirErr != nil {
		return snapDirErr
	}

	// Write wal directory paths.
	err = p.writePathsToRedis(waldir, walDirectoryPaths)
	if err != nil {
		return err
	}

	// Write snapshot directory paths.
	err = p.writePathsToRedis(snapdir, snapDirectoryPaths)
	if err != nil {
		return err
	}

	p.logger.Debug("Successfully wrote data directory and serialized state to Redis.",
		zap.String("WAL directory", waldir),
		zap.String("snapshot directory", snapdir),
		zap.String("data directory", datadir),
		zap.Int("serialized_state_num_bytes", len(serializedState)),
		zap.Duration("time_elapsed", time.Since(st)))

	return nil
}

func (p *RedisProvider) writeLocalDirectoryToRedis(targetDirectory string) ([]interface{}, error) {
	paths := make([]interface{}, 0)

	// Walk through the entire etcd-raft data directory, copying each file one-at-a-time to Redis.
	walkdirErr := filepath.WalkDir(targetDirectory, func(path string, d os.DirEntry, errArg error) error {
		// Note: the first entry found is the base directory passed to filepath.WalkDir (p.data_dir in this case).
		if d.IsDir() {
			p.logger.Info("Found local directory.", zap.String("directory", path))
			prefixedPath := fmt.Sprintf("%s%s", directoryPrefix, path)
			paths = append(paths, prefixedPath)
		} else {
			p.logger.Info("Found local file.", zap.String("file", path))

			// The p.hdfsClient has a CopyLocalToRemote function which does exactly what the code below does.
			// The only difference is that we retry remote.Close() in a loop until it stops returning ErrReplicating and succeeds.
			// Because hdfsClient.CopyLocalToRemote doesn't do this, we don't call that function and instead inline that function's logic below.

			// Open the local file that we'll be copying.
			fileContents, err := os.ReadFile(path)
			if err != nil {
				p.logger.Error("Failed to read contents of local file.",
					zap.String("path", path),
					zap.String("directory", targetDirectory),
					zap.Error(err))
				return err
			}

			err = p.redisClient.Set(context.Background(), path, fileContents, 0).Err()
			if err != nil {
				p.logger.Error("Failed to write contents of local file to Redis.",
					zap.String("path", path),
					zap.String("directory", targetDirectory),
					zap.Error(err))
				return err
			}

			prefixedPath := fmt.Sprintf("%s%s", filePrefix, path)
			paths = append(paths, prefixedPath)

			p.logger.Info(fmt.Sprintf("Successfully copied local file to Redis: '%s'", path), zap.String("file", path))
		}
		return nil
	})

	return paths, walkdirErr
}

func (p *RedisProvider) writeSerializedState(datadir string, serializedState []byte) error {
	redisKey := fmt.Sprintf("%s-%d", datadir, p.nodeId)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()

	err := p.redisClient.Set(ctx, redisKey, serializedState, 0).Err()
	if err != nil {
		p.logger.Error("Failed to write serialized state to Redis.",
			zap.String("redis_key", redisKey),
			zap.Error(err))
	}

	p.logger.Debug("Successfully wrote serialized state to Redis.",
		zap.String("redis_key", redisKey))

	return err
}

func (p *RedisProvider) writePathsToRedis(key string, paths []interface{}) error {
	err := p.redisClient.Del(context.Background(), key).Err()
	if err != nil {
		p.logger.Error("Failed to delete directory path list in Redis",
			zap.String("redis_key", key),
			zap.Error(err))
		return err
	}

	err = p.redisClient.RPush(context.Background(), key, paths...).Err()
	if err != nil {
		p.logger.Error("Failed to write directory path list to Redis",
			zap.String("redis_key", key),
			zap.Error(err))
		return err
	}

	return nil
}

func (p *RedisProvider) ReadDataDirectory(ctx context.Context, progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error) {
	st := time.Now()

	serializedStateBytes, err := p.readSerializedStateFromRedis(ctx, datadir)
	if err != nil {
		return nil, err
	}

	// Read the WAL dir (write-ahead log).
	err = p.readDirectoryFromRedis(waldir, progressChannel)
	if err != nil {
		return serializedStateBytes, err
	}

	// Read the snapshot directory.
	err = p.readDirectoryFromRedis(snapdir, progressChannel)

	p.logger.Debug("Successfully read data directory and serialized state from Redis.",
		zap.String("WAL directory", waldir),
		zap.String("snapshot directory", snapdir),
		zap.String("data directory", datadir),
		zap.Int("serialized_state_num_bytes", len(serializedStateBytes)),
		zap.Duration("time_elapsed", time.Since(st)))

	return serializedStateBytes, err
}

// readSerializedStateFromRedis reads the serialized state of the RaftLog from Redis and returns it.
func (p *RedisProvider) readSerializedStateFromRedis(ctx context.Context, dataDirectory string) ([]byte, error) {
	redisKey := fmt.Sprintf("%s-%d", dataDirectory, p.nodeId)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()

	serializedState, err := p.redisClient.Get(ctx, redisKey).Bytes()
	if err != nil {
		p.logger.Error("Failed to read serialized state from Redis.",
			zap.String("redis_key", redisKey),
			zap.Error(err))
	}

	return serializedState, err
}

func (p *RedisProvider) readDirectoryFromRedis(dir string, progressChannel chan<- string) error {
	p.logger.Debug("Reading directory from Redis.", zap.String("directory", dir))

	redisKey := dir
	paths, err := p.redisClient.LRange(context.Background(), redisKey, 0, -1).Result()
	if err != nil {
		p.logger.Error("Failed to read paths for directory from Redis.",
			zap.String("redis_key", redisKey),
			zap.String("directory", dir),
			zap.Error(err))

		return err
	}

	if len(paths) == 0 {
		p.logger.Error("Retrieved 0 paths from Redis for directory.",
			zap.String("redis_key", redisKey),
			zap.String("directory", dir))

		return fmt.Errorf("%w for directory \"%s\"", ErrNoPathsFound, dir)
	}

	p.logger.Debug("Retrieved paths from Redis for directory.",
		zap.String("redis_key", redisKey),
		zap.String("directory", dir),
		zap.Int("num_paths", len(paths)),
		zap.Strings("paths", paths))

	for pathIndex, path := range paths {
		isDirectory := strings.HasPrefix(path, directoryPrefix)

		var trimmedPath string
		if isDirectory {
			p.logger.Debug("Processing directory.",
				zap.String("redis_key", redisKey),
				zap.String("directory", dir),
				zap.Int("num_paths", len(paths)),
				zap.String("path", path),
				zap.String("trimmed_path", trimmedPath))

			trimmedPath = strings.TrimPrefix(path, directoryPrefix)

			err = os.MkdirAll(trimmedPath, os.FileMode(0777))
			if err != nil {
				p.logger.Error("Failed to create local directory.",
					zap.String("redis_key", redisKey),
					zap.String("directory", dir),
					zap.Int("num_paths", len(paths)),
					zap.String("path", path),
					zap.String("trimmed_path", trimmedPath),
					zap.Error(err))
				return err
			}

			progressChannel <- trimmedPath
			p.logger.Debug("Successfully created local directory.",
				zap.String("redis_key", redisKey),
				zap.String("directory", dir),
				zap.Int("num_paths", len(paths)),
				zap.String("path", path),
				zap.String("trimmed_path", trimmedPath))

			continue
		}

		trimmedPath = strings.TrimPrefix(path, filePrefix)

		p.logger.Debug("Retrieving data for path from Redis.",
			zap.String("file_or_directory", dir),
			zap.Bool("is_directory", isDirectory),
			zap.Int("path_index", pathIndex),
			zap.Int("num_paths", len(paths)),
			zap.String("path", path),
			zap.String("trimmed_path", trimmedPath))

		fileContents, err := p.redisClient.Get(context.Background(), trimmedPath).Bytes()
		if err != nil {
			p.logger.Error("Exception encountered while trying to read file from Redis.",
				zap.String("file_or_directory", dir),
				zap.Bool("is_directory", isDirectory),
				zap.Int("path_index", pathIndex),
				zap.Int("num_paths", len(paths)),
				zap.String("path", path),
				zap.String("trimmed_path", trimmedPath),
				zap.Error(err))
			return err
		}

		file, fileCreationError := os.Create(trimmedPath)
		if fileCreationError != nil {
			p.logger.Error("Failed to create local file.",
				zap.String("redis_key", redisKey),
				zap.String("directory", dir),
				zap.Int("num_paths", len(paths)),
				zap.String("path", path),
				zap.String("trimmed_path", trimmedPath),
				zap.Error(fileCreationError))
			return fileCreationError
		}

		_, err = file.Write(fileContents)
		if err != nil {
			p.logger.Error("Failed to write contents from Redis to local file.",
				zap.String("redis_key", redisKey),
				zap.String("directory", dir),
				zap.Int("num_paths", len(paths)),
				zap.String("path", path),
				zap.String("trimmed_path", trimmedPath),
				zap.Error(fileCreationError))
			return err
		}

		p.logger.Debug("Successfully wrote contents from Redis to local file.",
			zap.String("redis_key", redisKey),
			zap.String("directory", dir),
			zap.Int("num_paths", len(paths)),
			zap.String("path", path),
			zap.String("trimmed_path", trimmedPath),
			zap.Int("num_bytes", len(fileContents)))

		progressChannel <- trimmedPath
	}

	return nil
}
