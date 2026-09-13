package domain

import (
	"context"
	"errors"
	"fmt"
	"log"
	custom_errors "task_runner/internal/errors"
	"time"
)

func (s *Service) Run(ctx context.Context) {
	for i := range s.n_workers {
		s.wg.Add(1)
		go s.Worker(ctx, i)
	}
}

func (s *Service) ChooseChannel(req Priority, t Task) (chan Task, error) {
	switch req {
	case PriorityHigh:
		return s.highCh, nil
	case PriorityLow:
		return s.lowCh, nil
	default:
		return nil , custom_errors.ErrUnknownPriority
	}
}

func (s *Service) PutInQueue(ctx context.Context, req Priority, t Task) error {
	ch, err := s.ChooseChannel(req, t)
	if err != nil {
		return err
	}

	select {
	case ch <- t:
		return nil
	case <-ctx.Done():
		s.store.Delete(t.ID)
		return custom_errors.ErrFullQueue
}
}

func (s *Service) Worker(ctx context.Context, idWorker int) {
	defer s.m.DecWorkers()
	defer s.wg.Done()
	s.m.IncWorkers()
	high, low := s.highCh, s.lowCh
	for high != nil || low != nil{
		select {
		case t, ok := <-high:
			if !ok {
				high = nil
				continue
			}
			err := s.processTask(ctx, "high", idWorker, t)
			if err != nil {
				log.Printf("error: %v", err)
			}
			continue
		default:
		}

		select {
		case t, ok := <-high:
			if !ok {
				high = nil 
				continue
			}
			err := s.processTask(ctx, "high", idWorker, t)
			if err != nil {
				log.Printf("error: %v", err)
			}
		case t, ok := <-low:
			if !ok {
				low = nil
				continue
			}
			err := s.processTask(ctx, "low", idWorker, t)
			if err != nil {
				log.Printf("error: %v", err)
			}
		}
	}
}

func processURL(ctx context.Context, t Task) string {
	time.Sleep(2 * time.Second)
	return fmt.Sprintf("проверено: %s, статус: ok", t.ID)
}

func (s *Service) processTask(ctx context.Context, typeTask string, idWorker int, t Task) error {
	
	if err := ctx.Err(); err != nil {
		s.store.Set(t.ID, TaskResult{Status: "cancelled", Result: "cancelled by shutdown"})
		s.m.IncFailed()
		log.Printf("задача отменена при shutdown, task_id: %s, worker: %d", t.ID, idWorker)
		return err
	}
	s.sem <- struct{}{}
	defer func ()  {
		<-s.sem
	}()
	if s.tb.Allow(ctx, s.timeout) != nil {
		s.store.Set(t.ID, TaskResult{Status: "failed", Result: "failed"})
		s.m.IncFailed()
		return errors.New("перегружено")
	}
	res := processURL(ctx, t)
	s.store.Set(t.ID, TaskResult{Status: "done", Result: res})
	s.m.IncProcessed()
	log.Printf("Worker %d got task with %s priority, ID task: %s", idWorker, typeTask, t.ID)
	
	return nil
}

func (s *Service) CloseChannels() {
	s.closeOnce.Do(func() {
		close(s.highCh)
		close(s.lowCh)
	})
	
}

func (s *Service) GetMetrics() (int64, int64, int64) {
	return s.m.Snapshot()
}

func (s *Service) GetFromCache(key string) (TaskResult, bool) {
	
	return s.store.Get(key)
} 

func (s *Service) SetToCache(key string, value TaskResult) {
	s.store.Set(key, value)
}

func (s *Service) WaitTimeout(t time.Duration) error {
	done := make(chan struct{})
	go func(){
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(t):
		return custom_errors.ErrWorkersLate
	}

}