package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
	Jitter      float64
}

func Wrap(task func(ctx context.Context, payload any) (any, error), policy Policy) func(ctx context.Context, payload any) (any, error) {
	return func(ctx context.Context, payload any) (any, error) {
		delay := policy.BaseDelay
		var err error
		for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
			result, err := task(ctx, payload)
			if err == nil {
				return result, nil
			}

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}

			if attempt == policy.MaxAttempts {
				fmt.Println(payload, "task cant be completed")
				break
			}
			noise := float64(delay) * float64(policy.Jitter) * (rand.Float64()*2 - 1)
			wait := delay + time.Duration(noise)

			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			delay = time.Duration(float64(delay) * policy.Multiplier)
			if delay > policy.MaxDelay {
				delay = policy.MaxDelay
			}
		}
		return nil, err
	}
}
