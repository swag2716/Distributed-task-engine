package main

import (
	"context"
	"fmt"
	"time"

	pool "github.com/swag2716/distributed-task-engine/worker-pool"
)

func main() {
	fmt.Println("starting pool engine")
	p := pool.NewPool(
		pool.Config{
			MinWorkers:    3,
			MaxWorkers:    5,
			Workers:       3,
			QueueSize:     20,
			JobTimeout:    5 * time.Second,
			ScaleInterval: 5 * time.Second,
		},
	)

	go func() {
		for res := range p.Results() {
			if res.Err != nil {
				fmt.Printf("[FAIL] job=%-12s latency=%v err=%v\n", res.JobId, res.Latency, res.Err)
			} else {
				fmt.Printf("[PASS] job=%-12s latency=%v output=%v\n", res.JobId, res.Latency, res.Output)
			}
		}
		fmt.Println("result reader done")
	}()

	p.Submit(pool.Job{
		Id:      "math-01",
		Payload: 10,
		Task: func(_ context.Context, payload any) (any, error) {
			n := payload.(int)
			return n * n, nil
		},
	})

	p.Submit(pool.Job{
		Id:      "io-fast",
		Payload: "user",
		Task: func(_ context.Context, payload any) (any, error) {
			time.Sleep(50 * time.Millisecond)
			return fmt.Sprintf("fetched %s", payload), nil
		},
	})
	p.Shutdown()
}
