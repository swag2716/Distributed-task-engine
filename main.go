package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	localserver "github.com/swag2716/distributed-task-engine/localServer"
	"github.com/swag2716/distributed-task-engine/retry"
	pool "github.com/swag2716/distributed-task-engine/worker-pool"
)

func main() {
	fmt.Println("starting pool engine")
	server := localserver.SpinLocalServer()
	defer server.Close()
	p := pool.NewPool(
		pool.Config{
			MinWorkers:    3,
			MaxWorkers:    5,
			Workers:       3,
			QueueSize:     20,
			JobTimeout:    20 * time.Second,
			ScaleInterval: 5 * time.Second,
		},
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for res := range p.Results() {
			if res.Err != nil {
				fmt.Printf("[FAIL] job=%-12s latency=%v err=%v\n", res.JobId, res.Latency, res.Err)
			} else {
				fmt.Printf("[PASS] job=%-12s latency=%v output=%v\n", res.JobId, res.Latency, res.Output)
			}
		}
		fmt.Println("result reader done")
	}()

	policy := retry.Policy{
		BaseDelay:   100 * time.Millisecond,
		MaxAttempts: 3,
		MaxDelay:    2 * time.Second,
		Multiplier:  2.0,
		Jitter:      0.2,
	}

	task := func(ctx context.Context, payload any) (any, error) {
		u := payload.(string)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return nil, fmt.Errorf("server error: %d", resp.StatusCode)
		}
		return resp.StatusCode, nil
	}

	jobs := []struct {
		id  string
		url string
	}{
		{"success", server.URL + "/success"}, // succeeds immediately
		{"fail", server.URL + "/fail"},       // fails all 3 retries
		{"flaky", server.URL + "/flaky"},     // fails twice, succeeds third
	}

	for _, j := range jobs {
		p.Submit(pool.Job{
			Id:      j.id,
			Payload: j.url,
			Task:    retry.Wrap(task, policy),
		})
	}

	p.Submit(pool.Job{
		Id:      "io-fast",
		Payload: "user",
		Task: func(_ context.Context, payload any) (any, error) {
			time.Sleep(50 * time.Millisecond)
			return fmt.Sprintf("fetched %s", payload), nil
		},
	})
	p.Shutdown()
	<-done
}
