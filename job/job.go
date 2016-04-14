package job

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/zencoder/disque-go/disque"
)

// JobTimeout is the default job timeout
var JobTimeout = "2s"

// Job represents the job struct
type Job struct {
	ID        string
	QueueName string
	Body      string
	ETA       time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
	Raw       *disque.JobDetails
}

// AddJob is the function to add a job to the queue
func AddJob(client *disque.DisquePool, queueName string, body string, ETA time.time, options *map[string]string) (job *Job, err error) {
	// Construct the job
	job := &Job{
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
	id, err := cluster.Add(queueName, string(data), timeout, &_options)
	if err != nil {
		return
	}
	job.ID = id
	job.Body = body
	return
}

// FromDetails constructs a job instance using the job details from disque
func FromDetails(details *disque.JobDetails) (*Job, error) {
	var data Data
	err := json.Unmarshal([]byte(details.Message), &data)
	if err != nil {
		return nil, err
	}
	job := &Job{
		ID:        details.JobId,
		QueueName: details.QueueName,
		Body:      data.Body,
		ETA:       data.ETA,
		CreatedAt: data.CreatedAt,
		UpdatedAt: data.UpdatedAt,
		Raw:       details,
	}
	return job, nil
}
