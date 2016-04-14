package catapult

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zencoder/disque-go/disque"

	"catapult/queue"
)

// Catapult is the main catapult program
type Catapult struct {
	dClient *disque.DisquePool
	rClient *redis.Pool
}

// DisqueConnectOptions is the parameters for connecting to disque
type DisqueConnectOptions struct {
	Address string
}

// RedisConnectOptions is the paramters for connecting to redis
type RedisConnectOptions struct {
	Address string
	Auth    string
	DB      string
}

// Connect creates a catapult instance
func Connect(dOptions *DisqueConnectOptions, rOptions *RedisConnectOptions) (catapult *Catapult) {
	// Connect to disque
	dClient := disque.NewDisquePool(
		[]string{
			dOptions.Address,
		},
		1000,           // cycle
		5,              // initial capacity
		20,             // max capacity
		15*time.Minute, // idle timeout
	)
	// Connect to redis
	rClient := &redis.Pool{
		MaxIdle:     3,                 // max idle connections
		IdleTimeout: 240 * time.Second, // idle timeout
		Dial: func() (redis.Conn, error) {
			// Construct connection
			conn, err := redis.Dial("tcp", rOptions.Address)
			if err != nil {
				return nil, err
			}
			// Authenticate if necessary
			if rOptions.Auth != "" {
				if _, err := conn.Do("AUTH", rOptions.Auth); err != nil {
					conn.Close()
					return nil, err
				}
			}
			// Select db if necessary
			if rOptions.DB != "" {
				if _, err := conn.Do("SELECT", rOptions.DB); err != nil {
					conn.Close()
					return nil, err
				}
			}
			return conn, nil
		},
	}
	// Construct catapult
	catapult = &Catapult{
		dClient: dClient,
		rClient: rClient,
	}
	return
}

// Add is a public interface for queue.AddJob
func (c *Catapult) Add(queueName string, body string, ETA time.Time, options *map[string]string) (job *queue.Job, err error) {
	job, err = queue.AddJob(c.dClient, queueName, body, ETA, options)
	return
}

// Get is a public interface for queue.GetJob
func (c *Catapult) Get(id string) (job *queue.Job, err error) {
	job, err = queue.GetJob(c.dClient, id)
	return
}

// Remove is the public interface for queue.RemoveJob
func (c *Catapult) Remove(id string) (err error) {
	err = queue.RemoveJob(c.dClient, id)
	return
}

// Close shuts down the catapult
func (c *Catapult) Close() {
	c.dClient.Close()
	c.rClient.Close()
}
