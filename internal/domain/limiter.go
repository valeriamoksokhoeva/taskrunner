package domain

import (
	"context"
	custom_errors "task_runner/internal/errors"
	"time"
)



func NewTokenBucket(ctx context.Context, capacity int,  tikerTime time.Duration) *TokenBucket {

	tb := &TokenBucket{
		tokens: make(chan struct{}, capacity), 
		ticker: time.NewTicker(tikerTime),
	}
	
	for range capacity{
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

func (s *TokenBucket) Allow(ctx context.Context,timeout time.Duration) error {
	select {
	case <-s.tokens:
		return nil
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-s.tokens:
		return nil
	case <-timer.C:
		return custom_errors.ErrRateLimited
	case <-ctx.Done():
		return custom_errors.ErrCtxCancelled
	}
}