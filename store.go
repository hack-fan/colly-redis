package colly_redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisClient is because go-redis has many kind of clients.
type RedisClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	RPop(ctx context.Context, key string) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
}

// Storage implements the redis storage backend for Colly
type Storage struct {
	// Client any kind of [go-redis](https://github.com/go-redis/redis) client
	Client RedisClient

	// Prefix is an optional string in the keys. It can be used
	// to use one redis database for independent scraping tasks.
	Prefix string

	// Expiration time for Visited keys. After expiration pages
	// are to be visited again.
	Expires time.Duration

	// Context can be used for canceling all redis request, if you supply your own.
	Context context.Context

	mu sync.RWMutex // Only used for cookie methods.
}

// Init initializes the redis storage
func (s *Storage) Init() error {
	if s.Prefix == "" {
		s.Prefix = "colly"
	}
	if s.Context == nil {
		s.Context = context.Background()
	}
	if s.Client == nil {
		return errors.New("redis client not found")
	}
	err := s.Client.Ping(s.Context).Err()
	if err != nil {
		return fmt.Errorf("redis connection error: %w", err)
	}
	return err
}

// Clear removes all entries from the storage
func (s *Storage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, err := s.Client.Keys(s.Context, s.getCookieID("*")).Result()
	if err != nil {
		return err
	}
	keys2, err := s.Client.Keys(s.Context, s.Prefix+":request:*").Result()
	if err != nil {
		return err
	}
	keys = append(keys, keys2...)
	keys = append(keys, s.getQueueID())
	return s.Client.Del(s.Context, keys...).Err()
}

// Visited implements colly/storage.Visited()
func (s *Storage) Visited(requestID uint64) error {
	return s.Client.Set(s.Context, s.getIDStr(requestID), "1", s.Expires).Err()
}

// IsVisited implements colly/storage.IsVisited()
func (s *Storage) IsVisited(requestID uint64) (bool, error) {
	err := s.Client.Get(s.Context, s.getIDStr(requestID)).Err()
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// SetCookies implements colly/storage..SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	// TODO(js) Cookie methods currently have no way to return an error.

	// We need to use a write lock to prevent a race in the db:
	// if two callers set cookies in a very small window of time,
	// it is possible to drop the new cookies from one caller
	// ('last update wins' == best avoided).
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.Client.Set(s.Context, s.getCookieID(u.Host), cookies, 0).Err()
	if err != nil {
		// return nil
		log.Printf("SetCookies() .Set error %s", err)
		return
	}
}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	// TODO(js) Cookie methods currently have no way to return an error.

	s.mu.RLock()
	cookiesStr, err := s.Client.Get(s.Context, s.getCookieID(u.Host)).Result()
	s.mu.RUnlock()
	if err == redis.Nil {
		cookiesStr = ""
	} else if err != nil {
		// return nil, err
		log.Printf("Cookies() .Get error %s", err)
		return ""
	}
	return cookiesStr
}

// AddRequest implements queue.Storage.AddRequest() function
func (s *Storage) AddRequest(r []byte) error {
	return s.Client.LPush(s.Context, s.getQueueID(), r).Err()
}

// GetRequest implements queue.Storage.GetRequest() function
func (s *Storage) GetRequest() ([]byte, error) {
	r, err := s.Client.RPop(s.Context, s.getQueueID()).Bytes()
	if err != nil {
		return nil, err
	}
	return r, err
}

// QueueSize implements queue.Storage.QueueSize() function
func (s *Storage) QueueSize() (int, error) {
	i, err := s.Client.LLen(s.Context, s.getQueueID()).Result()
	return int(i), err
}

func (s *Storage) getIDStr(ID uint64) string {
	return fmt.Sprintf("%s:request:%d", s.Prefix, ID)
}

func (s *Storage) getCookieID(c string) string {
	return fmt.Sprintf("%s:cookie:%s", s.Prefix, c)
}

func (s *Storage) getQueueID() string {
	return fmt.Sprintf("%s:queue", s.Prefix)
}
