package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	stopped atomic.Bool
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

	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	done := make(chan struct{})
	go func() { //go routine is used here because we have timeout ans select waits on done channel for which we need go routine
		output, err = job.Task(ctx, job.Payload)
		close(done)
	}()
	select {
	case <-done:
		fmt.Println("task completed within time")
	case <-ctx.Done():
		err = fmt.Errorf("job: %s timout after %v", job.Id, timeout)
	}

	select { //non-blocking select as default will be executed instantly if result channel is full
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

var (
	ErrPoolStopped = errors.New("pool is stopped")
	ErrPoolFull    = errors.New("pool is full")
)

func (p *Pool) Submit(job Job) (err error) {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	defer func() {
		if r := recover(); r != nil {
			err = ErrPoolStopped
		}
	}()

	select {
	case p.jobs <- job:
		return nil
	case <-p.ctx.Done():
		return ErrPoolStopped
	}
}

func (p *Pool) TrySubmit(job Job) (err error) { //for fast operations, using default in select
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	defer func() {
		if r := recover(); r != nil {
			err = ErrPoolStopped
		}
	}()
	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrPoolFull
	}
}

func (p *Pool) Results() <-chan Result {
	return p.results
}

func (p *Pool) Shutdown() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	close(p.jobs)    // close jobs channel so no more writes
	p.wg.Wait()      // wait for all jobs to finish
	p.cancel()       //cancel context
	close(p.results) //close results channel
}
