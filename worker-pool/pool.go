package pool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Job struct {
	Id      string
	Payload any
	Task    func(ctx context.Context, payload any) (any, error)
	Timeout time.Duration
}

type Result struct {
	JobId   string
	Err     error
	Output  any
	Latency time.Duration
}

type Config struct {
	Workers    int
	QueueSize  int
	JobTimeout time.Duration
}

type Pool struct {
	cfg     Config
	jobs    chan Job
	results chan Result

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPool(cfg Config) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &Pool{
		cfg:     cfg,
		jobs:    make(chan Job, cfg.QueueSize),
		results: make(chan Result, cfg.QueueSize),
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := 0; i < cfg.Workers; i++ {
		pool.wg.Add(1)
		go pool.Worker()
	}

	return pool
}

func (p *Pool) Worker() {
	defer p.wg.Done()

	for job := range p.jobs {
		p.RunJob(job)
	}
}

func (p *Pool) RunJob(job Job) {
	start := time.Now()
	timeout := p.cfg.JobTimeout
	if job.Timeout > 0 {
		timeout = job.Timeout
	}

	var err error
	var output any

	if timeout > 0 {
		ctx, cancel := context.WithTimeout(p.ctx, timeout)
		defer cancel()

		done := make(chan struct{})
		go func() {
			output, err = job.Task(ctx, job.Payload)
			close(done)
		}()
		select {
		case <-done:
			fmt.Println("task completed within time")
		case <-ctx.Done():
			err = fmt.Errorf("job: %s timout after %v", job.Id, timeout)
		}
	}

	select {
	case p.results <- Result{
		JobId:   job.Id,
		Err:     err,
		Output:  output,
		Latency: time.Since(start),
	}:
	default:
		fmt.Println("Result channel is full")
	}
}
