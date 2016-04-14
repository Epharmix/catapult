package queue

import (
	"golang.org/x/net/context"
	"time"

	"github.com/zencoder/disque-go/disque"
)

// Private functions

func addJob(client *disque.DisquePool, queueName string, data string, timeout time.Duration, options *map[string]string) (id string, err error) {
	conn, err := client.Get(context.Background())
	if err != nil {
		return
	}
	defer client.Put(conn)
	if _, val := (*options)["RETRY"]; val {
		(*options)["RETRY"] = "5"
	}
	id, err = conn.PushWithOptions(queueName, data, timeout, *options)
	return
}

func getJob(client *disque.DisquePool, id string) (details *disque.JobDetails, err error) {
	conn, err := client.Get(context.Background())
	if err != nil {
		return
	}
	defer client.Put(conn)
	details, err = conn.GetJobDetails(id)
	return
}

func removeJob(client *disque.DisquePool, id string) (err error) {
	conn, err := client.Get(context.Background())
	if err != nil {
		return
	}
	defer client.Put(conn)
	err = conn.Delete(id)
	return
}
