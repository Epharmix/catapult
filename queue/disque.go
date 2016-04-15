package queue

import (
	"errors"
	"golang.org/x/net/context"
	"time"

	"github.com/zencoder/disque-go/disque"
)

// ErrNoConnection is the error thrown when no connection options are provided
var ErrNoConnection = errors.New("Disque Error: no client nor connection is provided!")

// Producer functions

func addJob(client *disque.DisquePool, conn *disque.Disque, queueName string, data string, timeout time.Duration, options *map[string]string) (id string, err error) {
	if client != nil {
		conn, err = client.Get(context.Background())
		if err != nil {
			return
		}
		defer client.Put(conn)
	} else if conn == nil {
		panic(ErrNoConnection)
	}
	if _, val := (*options)["RETRY"]; val {
		(*options)["RETRY"] = "5"
	}
	id, err = conn.PushWithOptions(queueName, data, timeout, *options)
	return
}

func getJob(client *disque.DisquePool, conn *disque.Disque, id string) (details *disque.JobDetails, err error) {
	if client != nil {
		conn, err = client.Get(context.Background())
		if err != nil {
			return
		}
		defer client.Put(conn)
	} else if conn == nil {
		panic(ErrNoConnection)
	}
	details, err = conn.GetJobDetails(id)
	return
}

func removeJob(client *disque.DisquePool, conn *disque.Disque, id string) (err error) {
	if client != nil {
		conn, err = client.Get(context.Background())
		if err != nil {
			return
		}
		defer client.Put(conn)
	} else if conn == nil {
		panic(ErrNoConnection)
	}
	err = conn.Delete(id)
	return
}

// Consumer functions

func fetchJobs(client *disque.DisquePool, conn *disque.Disque, queueName string, n int, timeout time.Duration) (details []*disque.JobDetails, err error) {
	details = make([]*disque.JobDetails, 0)
	if client != nil {
		conn, err = client.Get(context.Background())
		if err != nil {
			return
		}
		defer client.Put(conn)
	} else if conn == nil {
		panic(ErrNoConnection)
	}
	jobs, err := conn.FetchMultiple(queueName, n, timeout)
	if err != nil {
		return
	}
	var segment *disque.JobDetails
	for _, job := range jobs {
		segment, err = getJob(nil, conn, job.JobId)
		if err != nil {
			// Nack faulty jobs, then skip
			_ = nackJob(nil, conn, job.JobId)
			continue
		}
		details = append(details, segment)
	}
	return
}

func nackJob(client *disque.DisquePool, conn *disque.Disque, id string) (err error) {
	if client != nil {
		conn, err = client.Get(context.Background())
		if err != nil {
			return
		}
		defer client.Put(conn)
	} else if conn == nil {
		panic(ErrNoConnection)
	}
	err = conn.Nack(id)
	return
}

func ackJob(client *disque.DisquePool, conn *disque.Disque, id string) (err error) {
	if client != nil {
		conn, err = client.Get(context.Background())
		if err != nil {
			return
		}
		defer client.Put(conn)
	} else if conn == nil {
		panic(ErrNoConnection)
	}
	err = conn.Ack(id)
	return
}
