package domain

import (
	"context"
	"errors"
	custom_errors "task_runner/internal/errors"
	"testing"
	"time"
)

func TestTokenBucketRate(t *testing.T) {
	const (
		burst  = 5
		refill = 50 * time.Millisecond
		window = 1 * time.Second
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tb := NewTokenBucket(ctx, burst, refill)

	allowed := 0
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if err := tb.Allow(ctx, time.Millisecond); err == nil {
			allowed++
		}
	}

	rate := int(window / refill)
	maxExpected := burst + rate + 2
	t.Logf("%v window: %d allowed, burst=%d rate=%d/s, ceiling %d", window, allowed, burst, rate, maxExpected)

	if allowed > maxExpected {
		t.Fatalf("allowed %d, want at most %d", allowed, maxExpected)
	}
	if allowed < burst {
		t.Fatalf("initial burst unavailable: only %d of %d", allowed, burst)
	}
}

func TestTokenBucketStartsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const burst = 4
	tb := NewTokenBucket(ctx, burst, time.Hour)

	for i := 0; i < burst; i++ {
		if err := tb.Allow(ctx, time.Millisecond); err != nil {
			t.Fatalf("token %d should be available immediately: %v", i, err)
		}
	}
	if err := tb.Allow(ctx, 10*time.Millisecond); !errors.Is(err, custom_errors.ErrRateLimited) {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
}

func TestTokenBucketDistinguishesCancelFromLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tb := NewTokenBucket(ctx, 1, time.Hour)
	if err := tb.Allow(ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if err := tb.Allow(ctx, 10*time.Millisecond); !errors.Is(err, custom_errors.ErrRateLimited) {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}

	cancelled, cc := context.WithCancel(context.Background())
	cc()
	if err := tb.Allow(cancelled, time.Second); !errors.Is(err, custom_errors.ErrCtxCancelled) {
		t.Fatalf("want ErrCtxCancelled, got %v", err)
	}
}
