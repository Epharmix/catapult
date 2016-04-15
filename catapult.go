package catapult

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zencoder/disque-go/disque"

	"catapult/lock"
	"catapult/queue"
)

var (
	// CatapultCMDStopProcessing is the command for stopping the processing routine
	CatapultCMDStopProcessing = "STOPProc"
	// CatapultSignalStopProcSuccess signals the success of the stop command
	CatapultSignalStopProcSuccess = "STOPProcSuccess"
)

// DelegateFunction defines the signature of a delegate function
type DelegateFunction func(*queue.Job, string, *Catapult) interface{}

// Catapult is the main catapult program
type Catapult struct {
	Delegates map[string]DelegateFunction
	Control   chan string
	Result    chan string

	dClient *disque.DisquePool
	rClient *redis.Pool
	prefix  string

	processing bool
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
		Delegates:  make(map[string]DelegateFunction),
		Control:    make(chan string, 1),
		Result:     make(chan string, 1),
		dClient:    dClient,
		rClient:    rClient,
		prefix:     "ctpq:",
		processing: false,
	}
	return
}

// Delegate tasks from a specific queue to a function
func (c *Catapult) Delegate(queueName string, fn DelegateFunction) {
	c.Delegates[queueName] = fn
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

// Process kicks off the processing of jobs
func (c *Catapult) Process(queueName string, concurrency int) {
	// Check if there is a delegate for this queue
	if _, exists := c.Delegates[queueName]; !exists {
		// If not, do thing
		return
	}
	delegate := c.Delegates[queueName]
	// Mark as processing
	c.processing = true
	var job *queue.Job
	for {
		select {
		case command := <-c.Control:
			// Listen for controls
			if command == CatapultCMDStopProcessing {
				c.Result <- CatapultSignalStopProcSuccess
				c.processing = false
				return
			}
		default:
			// Fetch jobs from the queue
			jobs, err := queue.FetchJobs(c.dClient, queueName, concurrency)
			if err != nil {
				continue
			}
			// Process jobs
			for _, job = range jobs {
				c.process(job, queueName, delegate)
				fmt.Println("Done processing")
			}
		}
	}
}

// Close shuts down the catapult
func (c *Catapult) Close() {
	if c.processing {
		c.Control <- CatapultCMDStopProcessing
		_ = <-c.Result
	}
	c.dClient.Close()
	c.rClient.Close()
}

// Private functions

func (c *Catapult) getKeyForJob(job *queue.Job) string {
	return c.prefix + job.ID
}

func (c *Catapult) process(job *queue.Job, queueName string, fn DelegateFunction) {
	fmt.Println("Start processing: ", job.ID)
	// Catch any panics
	defer func() {
		if r := recover(); r != nil {
			// Log out the error
			fmt.Println(r)
			// Nack the job
			_ = queue.NackJob(c.dClient, job.ID)
		}
	}()
	// Acquire a lock on the job
	key := c.getKeyForJob(job)
	l := lock.NewLockOnKey(c.rClient, key, true)
	result, err := l.Get()
	// If lock cannot be acquired, return
	if err != nil {
		return
	}
	if !result {
		return
	}
	// Make sure to release the lock
	defer l.Release()
	// Start processing
	fn(job, queueName, c)
	// If success, ack the job
	err = queue.AckJob(c.dClient, job.ID)
	if err != nil {
		fmt.Println(err)
	}
	return
}
