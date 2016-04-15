package lock

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	// DefaultDuration is the default duration of the lock
	DefaultDuration = 10 * time.Second
	// DefaultAttempts is the default attempts for acquiring lock
	DefaultAttempts = 7
	// DefaultDelay is the default delay between attempts
	DefaultDelay = 512 * time.Millisecond
	// DefaultFactor is the default drift factor
	DefaultFactor = 0.01
)

var (
	// ErrLockFailedAfterMaxAttempts is the error for failing to acquire the lock after maximum attempts
	ErrLockFailedAfterMaxAttempts = errors.New("Lock Error: fail to acquire lock after maximum attempts!")
	// ErrLockExtensionFailed is the error for failing to extend the lock
	ErrLockExtensionFailed = errors.New("Lock Error: lock extension failed!")
	// ErrLockLost is the error for lock lost during auto renewal
	ErrLockLost = errors.New("Lock Error: lock is lost during auto renewal!")
)

var (
	// LockARCommandStop is the command for stopping the auto renew timer
	LockARCommandStop = "STOP"
	// LockARSignalStopSuccess is the signal for a successful stop
	LockARSignalStopSuccess = "STOPSuccess"
)

// Lock is a lock on a key
type Lock struct {
	Key         string        // redis key
	Duration    time.Duration // duration of the lock
	Factor      float64       // drif factor
	MaxAttempts int           // maxmium attempts to acquire the lock before failure
	Delay       time.Duration // delay between attempts
	AutoRenew   bool          // whether to auto renew the lock

	Client *redis.Pool // the redis client

	value string     // random string used as the value of the lock
	until time.Time  // timestamp at which the lock expires
	mutex sync.Mutex // internal mutex for updates

	ARControl chan string // auto renew control channel
	ARResult  chan string // auto renew result channel
}

// NewLockOnKey creates a lock struct (unacquired) on a key
func NewLockOnKey(client *redis.Pool, key string, ar bool) *Lock {
	lock := &Lock{
		Key:         key,
		Duration:    DefaultDuration,
		Factor:      DefaultFactor,
		MaxAttempts: DefaultAttempts,
		Delay:       DefaultDelay,
		AutoRenew:   ar,
		Client:      client,
		ARControl:   make(chan string, 1),
		ARResult:    make(chan string, 1),
	}
	return lock
}

// Get attempts to acquire lock on the key
func (l *Lock) Get() (bool, error) {
	// Pick up the internal mutext
	l.mutex.Lock()
	// Generate random value for the lock
	raw := make([]byte, 32)
	_, err := rand.Read(raw)
	if err != nil {
		return false, err
	}
	value := base64.StdEncoding.EncodeToString(raw)
	// Get a redis connection from the pool
	conn := l.Client.Get()
	defer conn.Close()
	// Start the process
	for i := 0; i < l.MaxAttempts; i++ {
		// Wait between attempts
		if i != 0 {
			time.Sleep(l.Delay)
		}
		// Start a timer to adjust for lost time during acquisition
		start := time.Now()
		duration := int(l.Duration / time.Millisecond)
		reply, err := redis.String(conn.Do("SET", l.Key, value, "NX", "PX", duration))
		// If anything fails, try again
		if err != nil {
			continue
		}
		if reply != "OK" {
			continue
		}
		// Calculate real duration for lock
		until := time.Now().Add(l.Duration - time.Now().Sub(start) - time.Duration(int64(float64(l.Duration)*l.Factor)) + 2*time.Millisecond)
		// Update the lock internal values
		l.value = value
		l.until = until
		l.mutex.Unlock()
		// Start auto renewal if specified
		if l.AutoRenew {
			go l.autoRenew()
		}
		// Lock is now acquired
		return true, nil
	}
	// Fail to acquire after max attempts
	l.mutex.Unlock()
	return false, ErrLockFailedAfterMaxAttempts
}

// Release revokes the lock on the key
func (l *Lock) Release() {
	// If lock is not acquired or released, do nothing
	if l.value == "" {
		return
	}
	// Signal auto renew to stop
	if l.AutoRenew {
		l.ARControl <- LockARCommandStop
		_ = <-l.ARResult
	}
	// Pick up the internal mutext
	l.mutex.Lock()
	defer l.mutex.Unlock()
	// Check lock status again
	if l.value == "" {
		return
	}
	// Clear the lock
	conn := l.Client.Get()
	defer conn.Close()
	_, _ = extendLock.Do(conn, l.Key, l.value)
	// Clear internal
	l.value = ""
	return
}

// Private functions

// AutoRenew
func (l *Lock) autoRenew() {
	// Run forever
	for {
		select {
		// Check commands
		case command := <-l.ARControl:
			if command == LockARCommandStop {
				l.ARResult <- LockARSignalStopSuccess
				return
			}
		// By default, sleep until renewal time
		default:
			// Sleep till next renewal time
			sleepDuration := time.Duration(int64(float64(l.Duration) * 0.5))
			time.Sleep(sleepDuration)
			// Extend lock
			result, err := l.extend(l.Duration)
			// If failed, panic so that the upstream function knows to stop
			if err != nil {
				fmt.Println(err)
				panic(ErrLockLost)
			}
			if !result {
				panic(ErrLockLost)
			}
		}
	}
}

// Extend the lock
func (l *Lock) extend(duration time.Duration) (result bool, err error) {
	// Pick up the internal mutext
	l.mutex.Lock()
	defer l.mutex.Unlock()
	// If lock is not acquired or released, do nothing
	result = false
	if l.value == "" {
		return
	}
	// Get a redis connection from the pool
	conn := l.Client.Get()
	defer conn.Close()
	// Extend the lock on the key
	start := time.Now()
	extension := int(duration / time.Millisecond)
	reply, err := extendLock.Do(conn, l.Key, l.value, extension)
	if err != nil {
		return
	}
	if reply != "OK" {
		return
	}
	// Update the lock
	until := time.Now().Add(l.Duration - time.Now().Sub(start) - time.Duration(int64(float64(l.Duration)*l.Factor)) + 2*time.Millisecond)
	l.until = until
	result = true
	return
}

// Redis script for releasing lock
var releaseLockScript = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  else
    return 0
  end
`
var releaseLock = redis.NewScript(1, releaseLockScript)

// Redis script for extending lock
var extendLockScript = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("SET", KEYS[1], ARGV[1], "XX", "PX", ARGV[2])
  else
    return "ERR"
  end
`
var extendLock = redis.NewScript(1, extendLockScript)
