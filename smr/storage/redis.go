package storage

import "github.com/redis/go-redis/v9"

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
	panic("Not implemented")
}
