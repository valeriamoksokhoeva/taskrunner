package domain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	custom_errors "task_runner/internal/errors"
	"time"
)

func (s *Service) Run(ctx context.Context) {
	for i := range s.numWorkers {
		s.wg.Add(1)
		go s.Worker(ctx, i)
	}
	slog.Info("worker pool started", "workers", s.numWorkers, "concurrency", cap(s.sem),
		"mode", s.mode, "overflow", s.overflow)
}

func (s *Service) chooseChannel(p Priority) (chan Task, error) {
	switch p {
	case PriorityHigh:
		return s.highCh, nil
	case PriorityLow:
		return s.lowCh, nil
	default:
		return nil, fmt.Errorf("%w: %q", custom_errors.ErrUnknownPriority, p)
	}
}

// PutInQueue enqueues a task. In reject mode a full queue fails immediately;
// in block mode it waits until ctx expires.
func (s *Service) PutInQueue(ctx context.Context, p Priority, t Task) error {
	ch, err := s.chooseChannel(p)
	if err != nil {
		return err
	}

	// Checked before the select so an expired context cannot lose the race
	// against free buffer space.
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.overflow == "reject" {
		select {
		case ch <- t:
			return nil
		default:
			return custom_errors.ErrFullQueue
		}
	}

	select {
	case ch <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Worker prefers highCh over lowCh and returns only once both queues are
// closed and drained. Cancellation is handled in processTask.
func (s *Service) Worker(ctx context.Context, idWorker int) {
	defer s.wg.Done()
	s.m.IncWorkers()
	defer s.m.DecWorkers()

	high, low := s.highCh, s.lowCh

	for high != nil || low != nil {
		select {
		case t, ok := <-high:
			if !ok {
				high = nil
				continue
			}
			s.processTask(ctx, PriorityHigh, idWorker, t)
			continue
		default:
		}

		select {
		case t, ok := <-high:
			if !ok {
				high = nil
				continue
			}
			s.processTask(ctx, PriorityHigh, idWorker, t)
		case t, ok := <-low:
			if !ok {
				low = nil
				continue
			}
			s.processTask(ctx, PriorityLow, idWorker, t)
		}
	}
	slog.Info("worker finished", "worker", idWorker)
}

func (s *Service) processTask(ctx context.Context, p Priority, idWorker int, t Task) error {
	log := slog.With("task_id", t.ID, "worker", idWorker, "priority", string(p))

	if err := ctx.Err(); err != nil {
		s.markCancelled(t, "shutdown before start")
		log.Warn("task cancelled before start")
		return err
	}

	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	if err := s.tb.Allow(ctx, s.tokenWait); err != nil {
		if errors.Is(err, custom_errors.ErrCtxCancelled) {
			s.markCancelled(t, "cancelled while waiting for a token")
			log.Warn("task cancelled waiting for rate limiter")
		} else {
			s.store.Set(t.ID, TaskResult{Status: StatusFailed, Result: err.Error()})
			s.m.IncFailed()
			log.Warn("task rejected by rate limiter")
		}
		return err
	}

	start := time.Now()
	res, err := s.runWork(ctx, t)
	took := time.Since(start)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.markCancelled(t, "cancelled during execution")
			log.Warn("task cancelled during execution", "err", err)
			return err
		}
		s.store.Set(t.ID, TaskResult{Status: StatusFailed, Result: err.Error()})
		s.m.IncFailed()
		log.Error("task failed", "err", err, "took_ms", took.Milliseconds())
		return err
	}

	s.store.Set(t.ID, TaskResult{Status: StatusDone, Result: res})
	s.m.IncProcessed()
	s.m.AddDuration(took)
	log.Info("task done", "took_ms", took.Milliseconds())
	return nil
}

func (s *Service) markCancelled(t Task, reason string) {
	s.store.Set(t.ID, TaskResult{Status: StatusCancelled, Result: reason})
	s.m.IncCancelled()
}

func (s *Service) runWork(ctx context.Context, t Task) (string, error) {
	if s.mode == "http" {
		return s.checkURL(ctx, t)
	}
	return s.simulate(ctx, t)
}

func (s *Service) checkURL(ctx context.Context, t Task) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, s.taskTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, t.Payload, nil)
	if err != nil {
		return "", fmt.Errorf("%w: %v", custom_errors.ErrBadURL, err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		if ctxErr := reqCtx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return "", ctxErr
		}
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Drain a bounded amount so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	return fmt.Sprintf("GET %s -> %d %s", t.Payload, resp.StatusCode, http.StatusText(resp.StatusCode)), nil
}

func (s *Service) simulate(ctx context.Context, t Task) (string, error) {
	timer := time.NewTimer(s.simulateFor)
	defer timer.Stop()

	select {
	case <-timer.C:
		return fmt.Sprintf("simulated check of %s: ok", t.Payload), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// CloseChannels must be called only after http.Server.Shutdown has returned,
// otherwise a handler still blocked on a send will panic.
func (s *Service) CloseChannels() {
	s.closeOnce.Do(func() {
		close(s.highCh)
		close(s.lowCh)
	})
}

func (s *Service) WaitTimeout(d time.Duration) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		return custom_errors.ErrWorkersLate
	}
}

func (s *Service) Metrics() MetricsRes { return s.m.Snapshot() }

func (s *Service) GetFromCache(key string) (TaskResult, bool) { return s.store.Get(key) }
func (s *Service) SetToCache(key string, v TaskResult)        { s.store.Set(key, v) }
func (s *Service) DeleteFromCache(key string)                 { s.store.Delete(key) }
