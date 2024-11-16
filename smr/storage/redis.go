package storage

import (
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
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

func NewRedisProvider(hostname string, deploymentMode string, nodeId int) *RedisProvider {
	baseProvider := newBaseProvider(hostname, deploymentMode, nodeId)

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
	p.status = Connecting

	p.redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: p.password,      // no password set
		DB:       p.databaseIndex, // use default DB
	})

	p.status = Connected

	return nil
}

func (p *RedisProvider) WriteDataDirectory(serializedState []byte, datadir string, waldir string, snapdir string) error {
	panic("Not implemented")
}

func (p *RedisProvider) ReadDataDirectory(progressChannel chan<- string, datadir string, waldir string, snapdir string) ([]byte, error) {
	serializedStateBytes, err := p.readSerializedStateFromRedis(datadir)
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

	return serializedStateBytes, err
}

// readSerializedStateFromRedis reads the serialized state of the RaftLog from Redis and returns it.
func (p *RedisProvider) readSerializedStateFromRedis(dataDirectory string) ([]byte, error) {
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

		fileContents, err := p.redisClient.Get(context.Background(), path).Bytes()
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
