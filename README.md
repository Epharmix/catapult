# Catapult

Catapult is a [disque](https://github.com/antirez/disque) based distributed task worker. Compared to other distributed work queues, it's built specifically for running jobs that are scheduled for execution at some timestamp in the future.

**Warning**: as of time of writing, `disque` is still in the 1.0-rc1 state, so until `disque` hits official 1.0 using this library in production is not recommended.

This project's designs and code are heavily influenced by these libraries: [bull](https://github.com/OptimalBits/bull), [redsync](https://github.com/hjr265/redsync.go).

### Overview

Under the hood, catapult is a wrapper around the **disque** library from [antirez](https://github.com/antirez), the author of **redis**. Disque is an "in-memory, distributed job queue", promising at-least-once message delivery. Utilizing the power of disque, catapult offers some high level producer-consumer constructs as well as a redis based locking mechanism to ensure jobs are only processed by a single worker at the same time.

### Prerequisites

Catapult requires both disque and redis to work.

`disque`: at least `1.0-rc1`

`redis`: at least `2.6.12`


### Installation

```
go get github.com/Epharmix/catapult
```

### Documentation

[View Documentation](https://godoc.org/github.com/Epharmix/catapult)

### Usage

First of, you will want to instantiate a catapult instance:

```go
import (
  "github.com/Epharmix/catapult"
)

dOptions := &DisqueConnectOptions{
  Address: "127.0.0.1:7711",
}
rOptions := &RedisConnectOptions{
  Address: "127.0.0.1:6379",
  DB:      "7",
}
c := catapult.Connect(dOptions, rOptions)
```

#### Producer

To push a job to the queue, use `catapult.AddJob`:

```go
queue = "myjobqueue"
delay, _ := time.ParseDuration("10s")
eta := time.Now().Add(delay)
job, err := c.Add(queue, "job1", eta, nil)
```

The code above schedules a job (`job1`) to be run 10 seconds from now. `job1` is the body the job, since job is essentially a piece of message in the `disque` context. Generally you can turn your custom parameters for the job into a JSON string and then set it as the body of the job; when you are processing the job, simply marshal the body back to original format.

Once a job is added, `disque` will handle the scheduling and promoting: the job will be put on the queue for consumption no earlier than the designated timestamp (the `eta`).

#### Consumer

To setup workers to consume the jobs, you will need to provide catapult a function that fits the signature of `DelegateFunction`:

```go
func(*queue.Job, string, *Catapult) interface{}
```

Each delegate function is linked to a specific queue name, and will only accept jobs from that queue.

```go
delegate := func(job *queue.Job, qName string, c *Catapult) interface{} {
  // Do something per the job...
  return
}
c.Delegate("math", delegate) // delegate all jobs from `math` to this delegate
```

Next you will want to tell catapult to start processing the jobs from that queue:

```go
c.Process("math", 10)
```

The second argument designates the concurrency of the job processing - in this case, catapult will try to grab 10 jobs from the queue and process them at the same time. You can play around with the number and see which value works best for you.

### License

The MIT License (MIT)

Copyright (c) 2016 Evan Huang <evan@epharmix.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
