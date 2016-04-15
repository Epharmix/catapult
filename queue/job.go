package queue

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zencoder/disque-go/disque"
)

// JobTimeout is the default job timeout
var JobTimeout = "5s"

// FetchTimeout is the default fetch timeout
var FetchTimeout = "10s"

// Job is the job struct
type Job struct {
	ID        string
	QueueName string
	Body      string
	ETA       time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
	Raw       *disque.JobDetails
}

// Data is a wrapper struct for the job's data
type Data struct {
	Body      string
	ETA       time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// AddJob adds a job to the queue
func AddJob(client *disque.DisquePool, queueName string, body string, ETA time.Time, options *map[string]string) (job *Job, err error) {
	// Construct the job
	job = &Job{
		QueueName: queueName,
		ETA:       ETA,
	}
	timeout, _ := time.ParseDuration(JobTimeout)
	// Calculate the delay
	now := time.Now()
	job.CreatedAt = now
	job.UpdatedAt = now
	delay := ETA.Sub(now).Seconds()
	_options := make(map[string]string)
	if delay > 0 {
		_options["DELAY"] = strconv.Itoa(int(delay))
	}
	data, _ := json.Marshal(
		&Data{
			Body:      body,
			ETA:       ETA,
			CreatedAt: now,
			UpdatedAt: now,
		},
	)
	id, err := addJob(client, nil, queueName, string(data), timeout, &_options)
	if err != nil {
		return
	}
	job.ID = id
	job.Body = body
	return
}

// GetJob gets a job from the queue using the id
func GetJob(client *disque.DisquePool, id string) (job *Job, err error) {
	// Get the job details
	details, err := getJob(client, nil, id)
	if err != nil {
		if err == redis.ErrNil {
			err = nil
		}
		return
	}
	// Construct job from details
	job, err = fromDetails(details)
	return
}

// RemoveJob removes a job from the queue using the id
func RemoveJob(client *disque.DisquePool, id string) (err error) {
	err = removeJob(client, nil, id)
	return
}

// FetchJobs gets jobs from the queue that are due for processing
func FetchJobs(client *disque.DisquePool, queueName string, n int) (jobs []*Job, err error) {
	jobs = make([]*Job, 0)
	// Fetch jobs from queue
	timeout, _ := time.ParseDuration(FetchTimeout)
	details, err := fetchJobs(client, nil, queueName, n, timeout)
	if err != nil {
		return
	}
	// Construct jobs from details
	var job *Job
	for _, segment := range details {
		job, err = fromDetails(segment)
		if err != nil {
			// Nack faulty jobs, then skip
			_ = nackJob(client, nil, segment.JobId)
			continue
		}
		jobs = append(jobs, job)
	}
	return
}

// NackJob sends an NACK about a job to the queue
func NackJob(client *disque.DisquePool, id string) (err error) {
	err = nackJob(client, nil, id)
	return
}

// AckJob sends an ACK about a job to the queue
func AckJob(client *disque.DisquePool, id string) (err error) {
	err = ackJob(client, nil, id)
	return
}

// Private functions

func fromDetails(details *disque.JobDetails) (job *Job, err error) {
	var data Data
	err = json.Unmarshal([]byte(details.Message), &data)
	if err != nil {
		return
	}
	job = &Job{
		ID:        details.JobId,
		QueueName: details.QueueName,
		Body:      data.Body,
		ETA:       data.ETA,
		CreatedAt: data.CreatedAt,
		UpdatedAt: data.UpdatedAt,
		Raw:       details,
	}
	return
}
