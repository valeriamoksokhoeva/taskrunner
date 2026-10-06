package domain

import (
	"context"
	custom_errors "task_runner/internal/errors"
	"time"
)

// NewTokenBucket returns a bucket that starts full, so the initial burst is
// available right away, and is topped up one token per refill period.
func NewTokenBucket(ctx context.Context, capacity int, refill time.Duration) *TokenBucket {
	tb := &TokenBucket{
		tokens: make(chan struct{}, capacity),
		ticker: time.NewTicker(refill),
	}

	for range capacity {
		tb.tokens <- struct{}{}
	}

	go func() {
		defer tb.ticker.Stop()
		for {
			select {
			case <-tb.ticker.C:
				select {
				case tb.tokens <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return tb
}

// Allow takes a token. It returns ErrRateLimited on timeout and
// ErrCtxCancelled on cancellation; callers treat those differently.
func (tb *TokenBucket) Allow(ctx context.Context, timeout time.Duration) error {
	select {
	case <-tb.tokens:
		return nil
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-tb.tokens:
		return nil
	case <-timer.C:
		return custom_errors.ErrRateLimited
	case <-ctx.Done():
		return custom_errors.ErrCtxCancelled
	}
}
