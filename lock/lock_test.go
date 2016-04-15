package lock

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var keyPrefix = "tq:l:"

func getClient() *redis.Pool {
	client := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			// Construct connection
			conn, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			if _, err := conn.Do("SELECT", "7"); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
	}
	return client
}

func TestGetLock(t *testing.T) {
	assert := assert.New(t)
	client := getClient()
	defer client.Close()
	key := keyPrefix + "getlock"
	lock := NewLockOnKey(client, key, false)
	assert.NotEmpty(lock)
	defer lock.Release()
	lock.Duration = 3 * time.Second
	result, err := lock.Get()
	assert.Empty(err)
	assert.True(result)
	conn := client.Get()
	value, err := redis.String(conn.Do("GET", key))
	assert.Empty(err)
	assert.Equal(value, lock.value)
	time.Sleep(lock.Duration)
	value, err = redis.String(conn.Do("GET", key))
	assert.Equal(err, redis.ErrNil)
	assert.Empty(value)
	conn.Close()
}

func TestAutoRenew(t *testing.T) {
	assert := assert.New(t)
	client := getClient()
	defer client.Close()
	// Create lock
	key := keyPrefix + "autorenew"
	lock := NewLockOnKey(client, key, true)
	assert.NotEmpty(lock)
	defer lock.Release()
	lock.Duration = 2 * time.Second
	// Get lock
	result, err := lock.Get()
	assert.Empty(err)
	assert.True(result)
	conn := client.Get()
	value, err := redis.String(conn.Do("GET", key))
	assert.Empty(err)
	assert.Equal(value, lock.value)
	// Wait after the normal duration
	time.Sleep(2 * lock.Duration)
	// Lock should still be valid
	value, err = redis.String(conn.Do("GET", key))
	assert.Empty(err)
	assert.Equal(value, lock.value)
	conn.Close()
}

func TestMutualExclusion(t *testing.T) {
	assert := assert.New(t)
	client := getClient()
	defer client.Close()
	// Create first lock
	key := keyPrefix + "mutex"
	lock1 := NewLockOnKey(client, key, true)
	assert.NotEmpty(lock1)
	defer lock1.Release()
	// Get first lock
	result, err := lock1.Get()
	assert.Empty(err)
	assert.True(result)
	conn := client.Get()
	value, err := redis.String(conn.Do("GET", key))
	assert.Empty(err)
	assert.Equal(value, lock1.value)
	conn.Close()
	// Create second lock
	key = keyPrefix + "mutex"
	lock2 := NewLockOnKey(client, key, true)
	assert.NotEmpty(lock2)
	defer lock2.Release()
	// Should not be able to acquire second lock
	result, err = lock2.Get()
	assert.False(result)
	assert.Equal(err, ErrLockFailedAfterMaxAttempts)
}

func TestRelease(t *testing.T) {

}
