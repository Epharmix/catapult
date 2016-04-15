package catapult

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"catapult/queue"
)

var testQueue = "tq"

func getInstance() *Catapult {
	dOptions := &DisqueConnectOptions{
		Address: "127.0.0.1:7711",
	}
	rOptions := &RedisConnectOptions{
		Address: "127.0.0.1:6379",
		DB:      "7",
	}
	catapult := Connect(dOptions, rOptions)
	return catapult
}

func TestConnect(t *testing.T) {
	assert := assert.New(t)
	catapult := getInstance()
	defer catapult.Close()
	assert.NotEmpty(catapult)
}

func TestAddJob(t *testing.T) {
	assert := assert.New(t)
	catapult := getInstance()
	defer catapult.Close()
	delay, _ := time.ParseDuration("10s")
	eta := time.Now().Add(delay)
	job, err := catapult.Add(testQueue, "add job", eta, nil)
	assert.Empty(err)
	assert.NotEmpty(job)
	assert.NotEmpty(job.ID)
	_job, err := catapult.Get(job.ID)
	assert.Empty(err)
	assert.NotEmpty(_job)
	assert.Equal(job.ID, _job.ID)
}

func TestRemoveJob(t *testing.T) {
	assert := assert.New(t)
	catapult := getInstance()
	defer catapult.Close()
	delay, _ := time.ParseDuration("10s")
	eta := time.Now().Add(delay)
	job, err := catapult.Add(testQueue, "remove job", eta, nil)
	assert.Empty(err)
	assert.NotEmpty(job)
	assert.NotEmpty(job.ID)
	err = catapult.Remove(job.ID)
	assert.Empty(err)
	_job, err := catapult.Get(job.ID)
	assert.Empty(err)
	assert.Empty(_job)
}

func TestProcessJobs(t *testing.T) {
	assert := assert.New(t)
	catapult := getInstance()
	defer catapult.Close()
	qName := "tqproc"
	// Set up delegate
	delegate := func(job *queue.Job, qName string, c *Catapult) interface{} {
		a := 1
		b := 2
		y := a + b
		return y
	}
	catapult.Delegate(qName, delegate)
	// Kick off the processing
	go catapult.Process(qName, 1)
	// Add some jobs
	jobs := make([]*queue.Job, 3)
	delay, _ := time.ParseDuration("1s")
	eta := time.Now().Add(delay)
	for i := 0; i < 3; i++ {
		job, err := catapult.Add(qName, "process job", eta, nil)
		assert.Empty(err)
		assert.NotEmpty(job)
		jobs[i] = job
	}
	time.Sleep(3 * time.Second)
	// Check if the jobs are processed
	for _, job := range jobs {
		_job, err := catapult.Get(job.ID)
		assert.Empty(err)
		assert.Empty(_job)
	}
}

func TestProcessThroughPut(t *testing.T) {
	assert := assert.New(t)
	catapult := getInstance()
	defer catapult.Close()
	qName := "tqprocmass"
	// Set up delegate
	delegate := func(job *queue.Job, qName string, c *Catapult) interface{} {
		a := 1
		b := 2
		y := a + b
		return y
	}
	catapult.Delegate(qName, delegate)
	// Kick off the processing
	go catapult.Process(qName, 10)
	// Add some jobs
	jobs := make([]*queue.Job, 200)
	eta := time.Now().Add(1 * time.Second)
	for i := 0; i < 200; i++ {
		job, err := catapult.Add(qName, "process job", eta, nil)
		assert.Empty(err)
		assert.NotEmpty(job)
		jobs[i] = job
	}
	time.Sleep(5 * time.Second)
	// Check if the jobs are processed
	for _, job := range jobs {
		_job, err := catapult.Get(job.ID)
		assert.Empty(err)
		assert.Empty(_job)
	}
}
