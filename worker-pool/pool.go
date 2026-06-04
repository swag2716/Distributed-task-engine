package pool

import (
	"context"
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

	for i := 0; i < cfg.workers; i++ {

	}

	return pool
}
