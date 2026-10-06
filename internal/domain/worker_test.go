package domain

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testSample(s ServiceSample) ServiceSample {
	if s.Capacity == 0 {
		s.Capacity = 100
	}
	if s.TickerTime == 0 {
		s.TickerTime = time.Millisecond
	}
	if s.ChanCap == 0 {
		s.ChanCap = 64
	}
	if s.SemCap == 0 {
		s.SemCap = 4
	}
	if s.NWorkers == 0 {
		s.NWorkers = 4
	}
	if s.TokenWait == 0 {
		s.TokenWait = 200 * time.Millisecond
	}
	if s.TaskTimeout == 0 {
		s.TaskTimeout = time.Second
	}
	if s.SimulateFor == 0 {
		s.SimulateFor = 50 * time.Millisecond
	}
	if s.Overflow == "" {
		s.Overflow = "reject"
	}
	if s.Mode == "" {
		s.Mode = "sleep"
	}
	return s
}

func (s *Service) submit(t *testing.T, ctx context.Context, p Priority, id string) {
	t.Helper()
	s.SetToCache(id, TaskResult{Status: StatusPending})
	if err := s.PutInQueue(ctx, p, Task{ID: id, Payload: "http://example.invalid"}); err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
}

func (s *Service) waitStatus(id, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r, ok := s.GetFromCache(id); ok && r.Status == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func TestPriorityOrderAndNoStarvation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, testSample(ServiceSample{
		NWorkers: 1, SemCap: 1, SimulateFor: 30 * time.Millisecond,
	}))

	// Low priority is submitted first: plain FIFO would run it first.
	s.submit(t, ctx, PriorityLow, "low-1")
	s.submit(t, ctx, PriorityLow, "low-2")
	s.submit(t, ctx, PriorityHigh, "high-1")
	s.submit(t, ctx, PriorityHigh, "high-2")

	s.Run(ctx)

	ids := []string{"low-1", "low-2", "high-1", "high-2"}
	seen := map[string]bool{}
	var order []string

	deadline := time.Now().Add(10 * time.Second)
	for len(order) < len(ids) && time.Now().Before(deadline) {
		for _, id := range ids {
			if seen[id] {
				continue
			}
			if r, ok := s.GetFromCache(id); ok && r.Status == StatusDone {
				seen[id] = true
				order = append(order, id)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	if len(order) != len(ids) {
		t.Fatalf("only %d of %d finished, order %v: low priority starved", len(order), len(ids), order)
	}
	if order[0][:4] != "high" || order[1][:4] != "high" {
		t.Fatalf("priority not honoured, order: %v", order)
	}
	t.Logf("order: %v", order)
}

func TestSemaphoreCapsConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const workers, semCap = 12, 3
	s := NewService(ctx, testSample(ServiceSample{
		NWorkers: workers, SemCap: semCap, SimulateFor: 80 * time.Millisecond,
	}))

	for i := 0; i < 24; i++ {
		s.submit(t, ctx, PriorityHigh, fmt.Sprintf("t-%d", i))
	}
	s.Run(ctx)

	var mu sync.Mutex
	peak := 0
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			mu.Lock()
			if n := len(s.sem); n > peak {
				peak = n
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(900 * time.Millisecond)
	close(done)

	mu.Lock()
	got := peak
	mu.Unlock()

	if got > semCap {
		t.Fatalf("semaphore leaked: peak %d > SemCap %d", got, semCap)
	}
	if got < 2 {
		t.Fatalf("almost no parallelism: peak %d", got)
	}
	t.Logf("workers=%d SemCap=%d peak=%d", workers, semCap, got)
}

func TestShutdownLosesNoTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := NewService(ctx, testSample(ServiceSample{
		NWorkers: 8, SemCap: 3, ChanCap: 64, SimulateFor: 20 * time.Millisecond,
	}))
	s.Run(ctx)

	const n = 40
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t-%d", i)
		ids = append(ids, id)
		s.submit(t, ctx, PriorityHigh, id)
	}

	cancel()
	s.CloseChannels()
	if err := s.WaitTimeout(15 * time.Second); err != nil {
		t.Fatalf("workers did not finish: %v", err)
	}

	counts := map[string]int{}
	for _, id := range ids {
		r, ok := s.GetFromCache(id)
		if !ok {
			t.Fatalf("task %s vanished from the store", id)
		}
		counts[r.Status]++
	}
	t.Logf("final statuses: %v", counts)

	if counts[StatusPending] != 0 {
		t.Fatalf("%d tasks left pending", counts[StatusPending])
	}
	if counts[StatusDone]+counts[StatusCancelled]+counts[StatusFailed] != n {
		t.Fatalf("statuses do not add up: %v", counts)
	}
}

func TestCancellationInterruptsRunningTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, testSample(ServiceSample{
		NWorkers: 1, SemCap: 1, SimulateFor: 5 * time.Second,
	}))
	s.Run(ctx)
	s.submit(t, ctx, PriorityHigh, "long")

	deadline := time.Now().Add(2 * time.Second)
	for len(s.sem) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(s.sem) == 0 {
		t.Fatal("worker never started")
	}

	start := time.Now()
	cancel()

	if !s.waitStatus("long", StatusCancelled, 2*time.Second) {
		r, _ := s.GetFromCache("long")
		t.Fatalf("task not cancelled, status=%q", r.Status)
	}
	t.Logf("interrupted %v after cancel, work was scheduled for 5s",
		time.Since(start).Round(time.Millisecond))
}

func TestOverflowReject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, testSample(ServiceSample{
		NWorkers: 0, ChanCap: 2, Overflow: "reject",
	}))

	for i := 0; i < 2; i++ {
		if err := s.PutInQueue(ctx, PriorityHigh, Task{ID: fmt.Sprint(i)}); err != nil {
			t.Fatalf("task %d should fit: %v", i, err)
		}
	}

	start := time.Now()
	err := s.PutInQueue(ctx, PriorityHigh, Task{ID: "overflow"})
	took := time.Since(start)

	if err == nil {
		t.Fatal("want an error on a full queue, got nil")
	}
	if took > 50*time.Millisecond {
		t.Fatalf("reject should not wait, took %v", took)
	}
	t.Logf("refused in %v: %v", took.Round(time.Microsecond), err)
}

func TestOverflowBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, testSample(ServiceSample{
		NWorkers: 0, ChanCap: 1, Overflow: "block",
	}))

	if err := s.PutInQueue(ctx, PriorityHigh, Task{ID: "first"}); err != nil {
		t.Fatal(err)
	}

	putCtx, putCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer putCancel()

	start := time.Now()
	err := s.PutInQueue(putCtx, PriorityHigh, Task{ID: "second"})
	took := time.Since(start)

	if err == nil {
		t.Fatal("want an error once the context expires")
	}
	if took < 150*time.Millisecond {
		t.Fatalf("returned after %v, does not look like it waited", took)
	}
	t.Logf("waited %v, then %v", took.Round(time.Millisecond), err)
}

func TestPutInQueueExpiredContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, testSample(ServiceSample{NWorkers: 0, Overflow: "block"}))

	expired, expCancel := context.WithTimeout(context.Background(), -1)
	defer expCancel()

	if err := s.PutInQueue(expired, PriorityHigh, Task{ID: "x"}); err == nil {
		t.Fatal("want an error with an expired context")
	}
	if len(s.highCh) != 0 {
		t.Fatalf("task was enqueued anyway: len=%d", len(s.highCh))
	}
}

func TestUnknownPriorityRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, testSample(ServiceSample{NWorkers: 0}))
	if err := s.PutInQueue(ctx, Priority("medium"), Task{ID: "x"}); err == nil {
		t.Fatal("unknown priority should be rejected")
	}
}

func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	s := NewService(ctx, testSample(ServiceSample{NWorkers: 8, SemCap: 2}))
	s.Run(ctx)
	for i := 0; i < 10; i++ {
		s.submit(t, ctx, PriorityLow, fmt.Sprintf("g-%d", i))
	}
	time.Sleep(100 * time.Millisecond)

	cancel()
	s.CloseChannels()
	if err := s.WaitTimeout(10 * time.Second); err != nil {
		t.Fatalf("workers: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak: before %d, after %d", before, after)
	}
	t.Logf("goroutines before=%d after=%d", before, after)
}
