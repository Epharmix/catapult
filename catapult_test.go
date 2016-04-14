package catapult

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
